#include <cassert>
#include <cstddef>
#include <cstdint>
#include <fuzzer/FuzzedDataProvider.h>
#include <string>
#include <utility>
#include <vector>

#include "cinder/common/status.hpp"
#include "cinder/store/ttl_wheel.hpp"

namespace {

    struct TestNode : cinder::WheelNode {
        std::string key;
    };

    // Exact-fire model: wheel slots fire when the cursor reaches them, heap
    // entries at their absolute tick. ttl_ticks == 0 wraps a full 256 (same
    // slot fires only when the cursor returns). Re-insert reschedules from
    // the current tick; heap re-inserts invalidate the stale entry via the
    // generation tag. Any early, late, duplicate, or missing fire is a bug.
    static void sequence(FuzzedDataProvider& fdp) {
        cinder::TtlWheel<TestNode> wheel;
        std::vector<std::unique_ptr<TestNode>> nodes;
        // expected[i] < 0: not scheduled (removed or already fired).
        std::vector<int64_t> expected;
        std::vector<bool> fired;

        const size_t n_ops = fdp.ConsumeIntegralInRange<size_t>(0, 128);
        // Nodes live on separate heap allocations, so locate a fired node by
        // pointer search (never pointer arithmetic across objects).
        auto indexOf = [&](const TestNode& node) -> size_t {
            for (size_t i = 0; i < nodes.size(); ++i) {
                if (nodes[i].get() == &node) {
                    return i;
                }
            }
            assert(false); // fired node not owned by the harness
            return nodes.size();
        };
        for (size_t i = 0; i < n_ops; ++i) {
            const uint8_t op = fdp.ConsumeIntegralInRange<uint8_t>(0, 3);
            if (op == 0 || nodes.empty()) {
                // Insert a fresh node.
                auto node = std::make_unique<TestNode>();
                node->key = "k" + std::to_string(nodes.size());
                const size_t ttl = fdp.ConsumeIntegralInRange<size_t>(0, 600);
                const int64_t now = static_cast<int64_t>(wheel.tickCount());
                int64_t fire_at;
                if (ttl == 0 || ttl <= cinder::TtlWheel<TestNode>::K_SLOT_COUNT) {
                    const size_t need = ttl == 0 ? cinder::TtlWheel<TestNode>::K_SLOT_COUNT : ttl;
                    fire_at = now + static_cast<int64_t>(need);
                } else {
                    fire_at = now + static_cast<int64_t>(ttl); // heap path
                }
                wheel.insert(node.get(), ttl);
                expected.push_back(fire_at);
                fired.push_back(false);
                nodes.push_back(std::move(node));
            } else if (op == 1) {
                // Remove a live node: it must never fire afterwards.
                const size_t idx = fdp.ConsumeIntegralInRange<size_t>(0, nodes.size() - 1);
                wheel.remove(nodes[idx].get());
                expected[idx] = -1;
            } else if (op == 2) {
                // Re-insert (reschedule) a live node from the current tick.
                const size_t idx = fdp.ConsumeIntegralInRange<size_t>(0, nodes.size() - 1);
                if (expected[idx] < 0) {
                    continue; // removed/fired — re-insert would revive; keep it dead
                }
                const size_t ttl = fdp.ConsumeIntegralInRange<size_t>(0, 600);
                const int64_t now = static_cast<int64_t>(wheel.tickCount());
                int64_t fire_at;
                if (ttl == 0 || ttl <= cinder::TtlWheel<TestNode>::K_SLOT_COUNT) {
                    const size_t need = ttl == 0 ? cinder::TtlWheel<TestNode>::K_SLOT_COUNT : ttl;
                    fire_at = now + static_cast<int64_t>(need);
                } else {
                    fire_at = now + static_cast<int64_t>(ttl);
                }
                wheel.insert(nodes[idx].get(), ttl);
                expected[idx] = fire_at;
            } else {
                // Advance 1..8 ticks and check every firing against the model.
                const size_t steps = fdp.ConsumeIntegralInRange<size_t>(1, 8);
                for (size_t s = 0; s < steps; ++s) {
                    wheel.tick([&](TestNode& node) {
                        const size_t idx = indexOf(node);
                        // A fired node must be scheduled and unfired.
                        assert(idx < nodes.size());
                        assert(expected[idx] >= 0);
                        assert(!fired[idx]);
                        assert(static_cast<int64_t>(wheel.tickCount()) == expected[idx]);
                        fired[idx] = true;
                        expected[idx] = -1;
                    });
                }
            }
        }

        // Liveness: drain far past every outstanding deadline; everything still
        // scheduled must fire exactly on time, nothing else may fire.
        for (size_t s = 0; s < 700; ++s) {
            wheel.tick([&](TestNode& node) {
                const size_t idx = indexOf(node);
                assert(idx < nodes.size());
                assert(expected[idx] >= 0);
                assert(!fired[idx]);
                assert(static_cast<int64_t>(wheel.tickCount()) == expected[idx]);
                fired[idx] = true;
                expected[idx] = -1;
            });
        }
        for (size_t i = 0; i < nodes.size(); ++i) {
            if (expected[i] >= 0) {
                // A live node that never fired within 700 ticks of its deadline.
                assert(false);
            }
        }
    }

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 4) {
        return 0;
    }
    FuzzedDataProvider fdp(data, size);
    sequence(fdp);
    return 0;
}
