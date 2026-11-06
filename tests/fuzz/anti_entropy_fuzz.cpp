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
#include "cinder/node/anti_entropy.hpp"
#include "cinder/store/cache_store.hpp"
#include "cinder/store/lru_store.hpp"

using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::system_clock;

namespace {

    // Deterministic clock so TTL expiries are reproducible (no RealClock).
    // nowSystem is fixed in lockstep (like SimClock) so the sys<->steady
    // conversion in anti-entropy blob parse round-trips exactly — a real wall
    // clock here would let identical re-applies observe different expiries
    // across microseconds.
    class FuzzClock final : public cinder::Clock {
    public:

        auto now() const -> steady_clock::time_point override {
            return steady_clock::time_point(milliseconds(1'000'000));
        }

        auto nowSystem() const -> system_clock::time_point override {
            return system_clock::time_point(milliseconds(1'700'000'000'000LL));
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
                cinder::err<cinder::net::Response>(
                    cinder::Error(cinder::Errc::NotReady, "fuzz stub")
                )
            );
        }

        void onMessage(MessageHandler /*handler*/) override {}
    };

    auto makeEntry(FuzzedDataProvider& fdp, FuzzClock& clock) -> cinder::VersionedEntry {
        cinder::VersionedEntry e;
        e.value = fdp.ConsumeRandomLengthString(256);
        const bool has_ttl = fdp.ConsumeBool();
        e.setVersionAndTtl(fdp.ConsumeIntegral<cinder::Version>(), has_ttl);
        e.writer_node_hash = fdp.ConsumeIntegral<uint64_t>();
        if (has_ttl) {
            e.expires_at =
                clock.now() + milliseconds(fdp.ConsumeIntegralInRange<int64_t>(0, 500'000));
        }
        return e;
    }

    auto snapKey(const std::pair<std::string, cinder::VersionedEntry>& kv) -> std::string {
        return kv.first
            + '\x00'
            + kv.second.value
            + '\x00'
            + std::to_string(kv.second.version())
            + '\x00'
            + std::to_string(kv.second.writer_node_hash)
            + '\x00'
            + (kv.second.hasTtl() ? "1" : "0");
    }

    auto snapshotOf(cinder::CacheStore& store) -> std::vector<std::string> {
        auto live = store.liveEntries();
        std::vector<std::string> out;
        out.reserve(live.size());
        for (const auto& kv : live) {
            out.push_back(snapKey(kv));
        }

        std::ranges::sort(out);
        return out;
    }

    // Codec roundtrips: decode inverts encode exactly, and a wrong bucket count
    // rejects the digest. Meta rows and sync messages round-trip too, with
    // exact-size validation rejecting truncation and junk. Group digests and
    // the compact phase-1 / refinement codecs round-trip as well.
    void codecRoundtrip(FuzzedDataProvider& fdp) {
        const int mode = fdp.ConsumeIntegralInRange<int>(0, 4);
        if (mode == 0) {
            const size_t n = fdp.ConsumeIntegralInRange<size_t>(0, 32);
            std::vector<uint64_t> digest;
            digest.reserve(n);
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
        } else if (mode == 2) {
            const size_t n = fdp.ConsumeIntegralInRange<size_t>(0, 32);
            std::vector<cinder::AntiEntropyManager::MetaRow> rows;
            for (size_t i = 0; i < n; ++i) {
                rows.push_back({
                    fdp.ConsumeIntegral<uint32_t>(),
                    fdp.ConsumeBool(),
                    fdp.ConsumeIntegral<uint64_t>(),
                    fdp.ConsumeIntegral<uint64_t>(),
                    fdp.ConsumeIntegral<uint64_t>(),
                });
            }

            FuzzClock clock;
            FuzzTransport transport;
            cinder::ConsistentHashRing ring;
            cinder::LruStore store(64 * 1024, &clock);
            cinder::AntiEntropyManager mgr(store, ring, "fuzz-self", clock, transport, 8);
            auto blob = cinder::AntiEntropyManager::encodeMetaRows(rows);
            auto back = cinder::AntiEntropyManager::decodeMetaRows(blob);
            assert(back.has_value());
            assert(back->size() == rows.size());
            for (size_t i = 0; i < n; ++i) {
                assert((*back)[i].bucket == rows[i].bucket);
                assert((*back)[i].key_hash == rows[i].key_hash);
                assert((*back)[i].version == rows[i].version);
                assert((*back)[i].writer == rows[i].writer);
                assert((*back)[i].tombstone == rows[i].tombstone);
            }
        } else if (mode == 1) {
            const size_t n = fdp.ConsumeIntegralInRange<size_t>(0, 8);
            std::vector<uint64_t> groups;
            for (size_t i = 0; i < n; ++i) {
                groups.push_back(fdp.ConsumeIntegral<uint64_t>());
            }

            auto blob = cinder::AntiEntropyManager::encodeGroupDigest(groups);
            auto back = cinder::AntiEntropyManager::decodeGroupDigest(blob, n);
            assert(back.has_value());
            assert(*back == groups);
            assert(!cinder::AntiEntropyManager::decodeGroupDigest(blob, n + 1).has_value());

            cinder::AntiEntropyManager::BucketRefinement req;
            req.force_metadata = fdp.ConsumeBool();
            const size_t m = fdp.ConsumeIntegralInRange<size_t>(0, 8);
            for (size_t i = 0; i < m; ++i) {
                // Keep indices in range: out-of-range buckets are (correctly)
                // rejected by the decoder.
                req.bucket_hashes.emplace_back(
                    fdp.ConsumeIntegral<uint32_t>() % 1024,
                    fdp.ConsumeIntegral<uint64_t>()
                );
            }
            auto req_blob = cinder::AntiEntropyManager::encodeBucketRefinement(req);
            auto req_back = cinder::AntiEntropyManager::decodeBucketRefinement(req_blob, 1024);
            assert(req_back.has_value());
            assert(req_back->bucket_hashes == req.bucket_hashes);
            assert(req_back->force_metadata == req.force_metadata);
            // Truncation and out-of-range bucket indices are rejected.
            assert(!cinder::AntiEntropyManager::decodeBucketRefinement(
                        req_blob.substr(0, !req_blob.empty() ? req_blob.size() - 1 : 0),
                        1024
            )
                        .has_value());
        } else if (mode == 3) {
            const size_t n = fdp.ConsumeIntegralInRange<size_t>(0, 8);
            std::vector<uint64_t> groups;
            for (size_t i = 0; i < n; ++i) {
                groups.push_back(fdp.ConsumeIntegral<uint64_t>());
            }
            cinder::AntiEntropyManager::DigestResponse resp;
            resp.group_hashes = groups;
            auto blob = cinder::AntiEntropyManager::encodeDigestResponse(resp);
            auto back = cinder::AntiEntropyManager::decodeDigestResponse(blob, n);
            assert(back.has_value());
            assert(back->group_hashes == groups);
            assert(back->bucket_hashes.empty());
            assert(back->covered_buckets.empty());
            assert(back->rows.empty());
            std::string truncated = blob.substr(0, blob.size() - 1);
            assert(!cinder::AntiEntropyManager::decodeDigestResponse(truncated, n).has_value());
        } else {
            const size_t n = fdp.ConsumeIntegralInRange<size_t>(0, 80);
            std::vector<cinder::AntiEntropyManager::MetaRow> rows;
            rows.reserve(n);
            for (size_t i = 0; i < n; ++i) {
                rows.push_back({
                    7,
                    fdp.ConsumeBool(),
                    fdp.ConsumeIntegral<uint64_t>(),
                    fdp.ConsumeIntegral<uint64_t>(),
                    fdp.ConsumeIntegral<uint64_t>(),
                });
            }
            const size_t cells = fdp.ConsumeIntegralInRange<size_t>(3, 128);
            auto sketch = cinder::AntiEntropyManager::buildIblt(rows, cells);
            assert(sketch.has_value());
            auto blob = cinder::AntiEntropyManager::encodeIblt(*sketch);
            auto decoded = cinder::AntiEntropyManager::decodeIblt(blob);
            assert(decoded.has_value());
            assert(decoded->cells.size() == cells);
            auto diff = cinder::AntiEntropyManager::decodeIbltDifference(*decoded, *decoded, 7);
            assert(diff.has_value());
            assert(diff->remote_only.empty());
            assert(diff->local_only.empty());
            assert(!cinder::AntiEntropyManager::decodeIblt(
                        std::string_view(blob).substr(0, blob.size() - 1)
            )
                        .has_value());
        }
    }

    // Collect/apply idempotence: metadata gathered from a populated store
    // diffs against an empty one, the needlist is served, and re-applying the
    // fetched entries is a no-op (LWW re-apply of identical versions changes
    // nothing observable).
    void collectApply(FuzzedDataProvider& fdp) {
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
        cinder::AntiEntropyManager apply_mgr(dst, ring, "fuzz-self", clock, transport, buckets);

        std::vector<uint32_t> ids;
        for (uint32_t b = 0; b < buckets; ++b) {
            ids.push_back(b);
        }
        auto meta = collect_mgr.collectMetadata(ids, 1U << 20U);
        auto rows = cinder::AntiEntropyManager::decodeMetaRows(meta);
        assert(rows.has_value());
        auto diff = apply_mgr.diffRows(*rows, ids);
        // Everything the source has, the empty destination needs.
        assert(diff.need.size() == rows->size());
        assert(diff.have.empty());
        auto fetched = collect_mgr.collectEntriesForHashes(diff.need, 1U << 20U);
        auto resp = apply_mgr.decodeSync(fetched);
        assert(resp.has_value());
        assert(resp->needs.empty());
        const auto before = snapshotOf(dst);
        (void)apply_mgr.applySync(std::move(*resp));
        const auto first = snapshotOf(dst);
        assert(first.size() == before.size() + diff.need.size());
        (void)apply_mgr.applySync(std::move(*resp));
        const auto second = snapshotOf(dst);
        assert(first == second);

        // Digest honesty: a store's digest is deterministic and bucket-counted.
        auto d1 = collect_mgr.computeDigest();
        auto d2 = collect_mgr.computeDigest();
        assert(d1.size() == buckets);
        assert(d1 == d2);
        // Incremental accumulators agree with the full scan.
        assert(collect_mgr.digestForRound() == d1);
    }

    // Raw protocol handlers: arbitrary payloads must never crash, and the
    // respond callback fires synchronously per the handler contract.
    void rawHandlers(FuzzedDataProvider& fdp) {
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
            if (fdp.ConsumeBool()) {
                req.key = "bucket-refinement-v3";
                mgr.onBucketRefinement("fuzz-peer", req, [&](cinder::net::Response) {
                    responded = true;
                });
            } else {
                mgr.onDigestRequest("fuzz-peer", req, [&](cinder::net::Response) {
                    responded = true;
                });
            }
        } else {
            mgr.onSyncRequest("fuzz-peer", req, [&](cinder::net::Response) { responded = true; });
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
