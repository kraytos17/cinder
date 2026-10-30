#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <fuzzer/FuzzedDataProvider.h>
#include <string>
#include <string_view>
#include <vector>

#include "cinder/hashing/consistent_hash_ring.hpp"

namespace {

    // Model: the live member set. Routing queries are gated on non-empty
    // (empty-ring lookup is out of contract — production always holds self).
    // Invariants: determinism, membership (every returned node is live),
    // distinctness, and exact count min(n, members).
    static void sequence(FuzzedDataProvider& fdp) {
        cinder::ConsistentHashRing ring;
        std::vector<cinder::NodeId> members;

        const size_t n_ops = fdp.ConsumeIntegralInRange<size_t>(0, 64);
        for (size_t i = 0; i < n_ops; ++i) {
            const uint8_t op = fdp.ConsumeIntegralInRange<uint8_t>(0, 3);
            if (op == 0) {
                auto id = fdp.ConsumeRandomLengthString(8);
                ring.addNode(id);
                if (std::find(members.begin(), members.end(), id) == members.end()) {
                    members.push_back(id);
                }
            } else if (op == 1) {
                auto id = fdp.ConsumeRandomLengthString(8);
                ring.removeNode(id);
                members.erase(std::remove(members.begin(), members.end(), id), members.end());
            } else if (op == 2) {
                if (members.empty()) {
                    continue;
                }
                auto key = fdp.ConsumeRandomLengthString(16);
                auto first = ring.getNode(key);
                auto second = ring.getNode(key);
                assert(first == second); // deterministic routing
                assert(std::find(members.begin(), members.end(), first) != members.end());
            } else {
                if (members.empty()) {
                    continue;
                }
                auto key = fdp.ConsumeRandomLengthString(16);
                // Replica count is assert-bounded at 32 in the implementation.
                const int n = fdp.ConsumeIntegralInRange<int>(0, 32);
                auto got = ring.getNodes(key, n);
                assert(got.size() == static_cast<size_t>(std::min<int>(n, members.size())));
                for (size_t a = 0; a < got.size(); ++a) {
                    assert(std::find(members.begin(), members.end(), got[a]) != members.end());
                    for (size_t b = a + 1; b < got.size(); ++b) {
                        assert(got[a] != got[b]);
                    }
                }
                // Same key twice: identical replica set.
                auto again = ring.getNodes(key, n);
                assert(got == again);
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
