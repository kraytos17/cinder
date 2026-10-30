#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fuzzer/FuzzedDataProvider.h>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "cinder/cluster/clock.hpp"
#include "cinder/cluster/transport.hpp"
#include "cinder/common/status.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/node/anti_entropy.hpp"
#include "cinder/store/cache_store.hpp"
#include "cinder/store/lru_store.hpp"

namespace {

// Deterministic clock so TTL expiries are reproducible (no RealClock).
// nowSystem is fixed in lockstep (like SimClock) so the sys<->steady
// conversion in anti-entropy blob parse round-trips exactly — a real wall
// clock here would let identical re-applies observe different expiries
// across microseconds.
class FuzzClock final : public cinder::Clock {
public:

    auto now() const -> std::chrono::steady_clock::time_point override {
        return std::chrono::steady_clock::time_point(std::chrono::milliseconds(1'000'000));
    }

    auto nowSystem() const -> std::chrono::system_clock::time_point override {
        return std::chrono::system_clock::time_point(std::chrono::milliseconds(1'700'000'000'000LL));
    }
};

class FuzzTransport final : public cinder::Transport {
public:

    void sendAsync(
        const cinder::NodeId& /*to*/,
        const cinder::net::Request& /*req*/,
        SendCallback on_done
    ) override {
        on_done(cinder::ok());
    }

    void sendRequestAsync(
        const cinder::NodeId& /*to*/,
        const cinder::net::Request& /*req*/,
        RequestCallback on_done
    ) override {
        on_done(
            cinder::err<cinder::net::Response>(cinder::Error(cinder::Errc::NotReady, "fuzz stub"))
        );
    }

    void onMessage(MessageHandler /*handler*/) override {}
};

static auto makeEntry(FuzzedDataProvider& fdp, FuzzClock& clock) -> cinder::VersionedEntry {
    cinder::VersionedEntry e;
    e.value = fdp.ConsumeRandomLengthString(256);
    const bool has_ttl = fdp.ConsumeBool();
    e.setVersionAndTtl(fdp.ConsumeIntegral<cinder::Version>(), has_ttl);
    e.writer_node_hash = fdp.ConsumeIntegral<uint64_t>();
    if (has_ttl) {
        e.expires_at =
            clock.now() + std::chrono::milliseconds(fdp.ConsumeIntegralInRange<int64_t>(0, 500'000));
    }
    return e;
}

static auto snapKey(const std::pair<std::string, cinder::VersionedEntry>& kv) -> std::string {
    return kv.first + '\x00' + kv.second.value + '\x00'
        + std::to_string(kv.second.version()) + '\x00'
        + std::to_string(kv.second.writer_node_hash) + '\x00'
        + (kv.second.hasTtl() ? "1" : "0");
}

static auto snapshotOf(cinder::CacheStore& store) -> std::vector<std::string> {
    auto live = store.liveEntries();
    std::vector<std::string> out;
    out.reserve(live.size());
    for (const auto& kv : live) {
        out.push_back(snapKey(kv));
    }
    std::sort(out.begin(), out.end());
    return out;
}

// Codec roundtrips: decode inverts encode exactly, and a wrong bucket count
// rejects the digest.
static void codecRoundtrip(FuzzedDataProvider& fdp) {
    if (fdp.ConsumeBool()) {
        const size_t n = fdp.ConsumeIntegralInRange<size_t>(0, 32);
        std::vector<uint64_t> digest;
        for (size_t i = 0; i < n; ++i) {
            digest.push_back(fdp.ConsumeIntegral<uint64_t>());
        }
        auto blob = cinder::AntiEntropyManager::encodeDigest(digest);
        auto back = cinder::AntiEntropyManager::decodeDigest(blob, n);
        assert(back.has_value());
        assert(*back == digest);
        // Wrong expectation rejects.
        auto wrong = cinder::AntiEntropyManager::decodeDigest(blob, n + 1);
        assert(!wrong.has_value());
    } else {
        const size_t n = fdp.ConsumeIntegralInRange<size_t>(0, 32);
        std::vector<uint32_t> ids;
        for (size_t i = 0; i < n; ++i) {
            ids.push_back(fdp.ConsumeIntegral<uint32_t>());
        }
        auto blob = cinder::AntiEntropyManager::encodeBucketIds(ids);
        auto back = cinder::AntiEntropyManager::decodeBucketIds(blob);
        assert(back.has_value());
        assert(*back == ids);
    }
}

// Collect/apply idempotence: entries gathered from a populated store apply
// cleanly to an empty one, and re-applying the same blob is a no-op (LWW
// re-apply of identical versions changes nothing observable).
static void collectApply(FuzzedDataProvider& fdp) {
    FuzzClock clock;
    FuzzTransport transport;
    cinder::ConsistentHashRing ring;
    constexpr size_t K_CAPACITY = 64 * 1024;
    cinder::LruStore src(K_CAPACITY, &clock);
    cinder::LruStore dst(K_CAPACITY, &clock);

    const size_t n = fdp.ConsumeIntegralInRange<size_t>(0, 16);
    for (size_t i = 0; i < n; ++i) {
        auto key = fdp.ConsumeRandomLengthString(16);
        auto res = src.putVersioned(key, makeEntry(fdp, clock));
        assert(res.has_value() || res.error().code() == cinder::Errc::CapacityExceeded);
    }

    const uint32_t buckets = fdp.ConsumeIntegralInRange<uint32_t>(1, 32);
    cinder::AntiEntropyManager collect_mgr(src, ring, "fuzz-self", clock, transport, buckets);
    std::vector<uint32_t> ids;
    const size_t m = fdp.ConsumeIntegralInRange<size_t>(0, 8);
    for (size_t i = 0; i < m; ++i) {
        ids.push_back(fdp.ConsumeIntegral<uint32_t>() % buckets);
    }
    auto blob = collect_mgr.collectEntries(ids);

    cinder::AntiEntropyManager apply_mgr(dst, ring, "fuzz-self", clock, transport, buckets);
    (void)apply_mgr.applyEntries(blob);
    const auto first = snapshotOf(dst);
    (void)apply_mgr.applyEntries(blob);
    const auto second = snapshotOf(dst);
    assert(first == second);

    // Digest honesty: a store's digest is deterministic and bucket-counted.
    auto d1 = collect_mgr.computeDigest();
    auto d2 = collect_mgr.computeDigest();
    assert(d1.size() == buckets);
    assert(d1 == d2);
}

// Raw protocol handlers: arbitrary payloads must never crash, and the
// respond callback fires synchronously per the handler contract.
static void rawHandlers(FuzzedDataProvider& fdp) {
    FuzzClock clock;
    FuzzTransport transport;
    cinder::ConsistentHashRing ring;
    cinder::LruStore store(64 * 1024, &clock);
    cinder::AntiEntropyManager mgr(store, ring, "fuzz-self", clock, transport, 8);

    cinder::net::Request req;
    req.opcode = fdp.PickValueInArray(
        {cinder::net::Opcode::AntiEntropyDigest, cinder::net::Opcode::AntiEntropySync}
    );
    req.value = fdp.ConsumeRemainingBytesAsString();

    bool responded = false;
    if (req.opcode == cinder::net::Opcode::AntiEntropyDigest) {
        mgr.onDigestRequest("fuzz-peer", req, [&](cinder::net::Response) {
            responded = true;
        });
    } else {
        mgr.onSyncRequest("fuzz-peer", req, [&](cinder::net::Response) {
            responded = true;
        });
    }
    assert(responded);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 4) {
        return 0;
    }
    FuzzedDataProvider fdp(data, size);
    switch (fdp.ConsumeIntegralInRange<uint8_t>(0, 2)) {
        case 0:
            codecRoundtrip(fdp);
            break;
        case 1:
            collectApply(fdp);
            break;
        default:
            rawHandlers(fdp);
            break;
    }
    return 0;
}
