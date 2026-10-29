#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fuzzer/FuzzedDataProvider.h>
#include <optional>
#include <string>
#include <vector>

#include "cinder/cluster/clock.hpp"
#include "cinder/common/types.hpp"
#include "cinder/store/lfu_store.hpp"
#include "cinder/store/lru_store.hpp"

using std::chrono::milliseconds;
using std::chrono::steady_clock;

// Deterministic clock: time only moves when the harness advances it, so
// TTL/expiry behavior is fully reproducible (no RealClock in fuzz).
class FuzzClock final : public cinder::Clock {
public:

    explicit FuzzClock(steady_clock::time_point start) :
        now_(start) {}

    auto now() const -> steady_clock::time_point override {
        return now_;
    }

    void advance(milliseconds d) {
        now_ += d;
    }

private:

    steady_clock::time_point now_;
};

// TTL classes covering both wheel paths: sub-second (next-tick reap),
// in-wheel (<=256s), and heap (>256 slots) plus the u32 extremes.
static auto pickTtl(FuzzedDataProvider& fdp) -> std::optional<milliseconds> {
    switch (fdp.ConsumeIntegralInRange<uint8_t>(0, 6)) {
        case 0:
            return std::nullopt;
        case 1:
            return milliseconds(1);
        case 2:
            return milliseconds(100);
        case 3:
            return milliseconds(5000);
        case 4:
            return milliseconds(30'000);
        case 5:
            // Heap path: beyond the 256-slot wheel.
            return milliseconds(400'000);
        default:
            return milliseconds(fdp.ConsumeIntegral<uint32_t>());
    }
}

template <typename Store>
static void runSequence(FuzzedDataProvider& fdp, Store& store, FuzzClock& clock) {
    // Small key pool forces collisions, overwrites, and eviction pressure.
    // Small values keep per-input cost bounded; one large class still
    // exercises the capacity-reject path.
    const size_t pool_size = fdp.ConsumeIntegralInRange<size_t>(1, 8);
    std::vector<std::string> keys;
    keys.reserve(pool_size);
    for (size_t i = 0; i < pool_size; ++i) {
        keys.push_back(fdp.ConsumeRandomLengthString(8));
    }
    if (keys.empty()) {
        return;
    }

    // LWW per-op guarantee: a single mutating op must never lower a
    // resident key's version (put mints higher, putVersioned rejects
    // stale). Sampled around the op itself so silent capacity eviction
    // between observations cannot produce a false positive: if the key is
    // absent before or after, there is no constraint.
    const size_t num_ops = fdp.ConsumeIntegralInRange<size_t>(1, 64);
    for (size_t i = 0; i < num_ops; ++i) {
        const std::string& key = keys[fdp.ConsumeIntegralInRange<size_t>(0, keys.size() - 1)];
        const uint8_t op = fdp.ConsumeIntegralInRange<uint8_t>(0, 6);
        switch (op) {
            case 0: {
                // put: small values usually, occasionally a large one to
                // hit the per-shard capacity-reject path.
                std::string value;
                if (fdp.ConsumeBool()) {
                    value = fdp.ConsumeRandomLengthString(16'384);
                } else {
                    value = fdp.ConsumeRandomLengthString(256);
                }
                auto ttl = pickTtl(fdp);
                auto before = store.getVersioned(key);
                auto res = store.put(key, std::move(value), ttl);
                // Either accepted or rejected for capacity — nothing else.
                assert(res.has_value() || res.error().code() == cinder::Errc::CapacityExceeded);
                if (before.has_value()) {
                    auto after = store.getVersioned(key);
                    if (after.has_value()) {
                        assert(after->version() >= before->version());
                    }
                }
                break;
            }
            case 1: {
                // putVersioned: FDP-built entry exercises the LWW
                // accept/reject path, including stale and equal versions.
                cinder::VersionedEntry entry;
                entry.value = fdp.ConsumeRandomLengthString(256);
                const auto v = fdp.ConsumeIntegral<cinder::Version>();
                const bool has_ttl = fdp.ConsumeBool();
                entry.setVersionAndTtl(v & ~(cinder::Version{1} << 63U), has_ttl);
                entry.writer_node_hash = fdp.ConsumeIntegral<uint64_t>();
                if (has_ttl) {
                    entry.expires_at =
                        clock.now() + milliseconds(fdp.ConsumeIntegralInRange<int64_t>(0, 500'000));
                }
                auto before = store.getVersioned(key);
                auto res = store.putVersioned(key, std::move(entry));
                assert(res.has_value() || res.error().code() == cinder::Errc::CapacityExceeded);
                if (before.has_value()) {
                    auto after = store.getVersioned(key);
                    if (after.has_value()) {
                        assert(after->version() >= before->version());
                    }
                }
                break;
            }
            case 2: {
                // get/getVersioned consistency: a visible key must read back
                // the same value through both APIs.
                auto plain = store.get(key);
                auto versioned = store.getVersioned(key);
                assert(plain.has_value() == versioned.has_value());
                if (plain.has_value()) {
                    assert(plain == versioned->value);
                }
                break;
            }
            case 3: {
                (void)store.remove(key);
                // Removing twice is idempotent; the key must stay gone.
                (void)store.remove(key);
                assert(!store.get(key).has_value());
                assert(!store.getVersioned(key).has_value());
                break;
            }
            case 4: {
                // Advance deterministic time (sub-second through heap-range)
                // then reap. Expired keys must stay expired.
                const auto step = fdp.ConsumeIntegralInRange<int64_t>(0, 500'000);
                clock.advance(milliseconds(step));
                (void)store.evictExpired();
                auto plain = store.get(key);
                auto versioned = store.getVersioned(key);
                assert(plain.has_value() == versioned.has_value());
                break;
            }
            case 5: {
                // Introspection: size/liveEntries must agree once expired
                // entries are reaped (size counts the index, liveEntries
                // skips expired-but-not-yet-reaped entries).
                (void)store.evictExpired();
                const size_t n = store.size();
                auto live = store.liveEntries();
                assert(live.size() == n);
                for (const auto& [k, e] : live) {
                    auto got = store.getVersioned(k);
                    assert(got.has_value());
                    assert(got->value == e.value);
                    assert(got->version() == e.version());
                }
                break;
            }
            default: {
                (void)store.evictExpired();
                break;
            }
        }
    }
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 4) {
        return 0;
    }
    FuzzedDataProvider fdp(data, size);

    // Deterministic start: fixed epoch so TTL math is reproducible.
    FuzzClock clock(steady_clock::time_point(milliseconds(1'700'000'000'000LL)));
    // Small capacity forces real eviction pressure (capacity-slice
    // rejects included); single policy per input keeps oracles exact —
    // LRU vs LFU legitimately differ in *which* key they evict.
    constexpr size_t K_CAPACITY = 4096;
    if (fdp.ConsumeBool()) {
        cinder::LfuStore store(K_CAPACITY, &clock);
        runSequence(fdp, store, clock);
    } else {
        cinder::LruStore store(K_CAPACITY, &clock);
        runSequence(fdp, store, clock);
    }
    return 0;
}
