#pragma once

// Shared fixtures for the anti-entropy unit tests (extracted verbatim
// from anti_entropy_test.cpp when it was split by area).

#include <cstdint>
#include <string>
#include <vector>

#include "cinder/cluster/clock.hpp"
#include "cinder/cluster/transport.hpp"
#include "cinder/common/metrics.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/store/lru_store.hpp"

namespace cinder {

    class FakeTransport : public Transport {
    public:

        void sendAsync(
            const NodeId& /*to*/,
            const net::Request& /*req*/,
            SendCallback on_done
        ) override {
            on_done(ok());
        }

        void sendRequestAsync(
            const NodeId& /*to*/,
            const net::Request& /*req*/,
            RequestCallback on_done
        ) override {
            on_done(err<net::Response>(Error(Errc::NotReady, "no peer")));
        }

        void onMessage(MessageHandler /*handler*/) override {}
    };

    inline auto makeEntry(std::string value, Version version, uint64_t writer = 7)
        -> VersionedEntry {
        VersionedEntry e;
        e.value = std::move(value);
        e.setVersion(version);
        e.writer_node_hash = writer;
        return e;
    }

    inline auto allBuckets(uint32_t n) -> std::vector<uint32_t> {
        std::vector<uint32_t> ids(n);
        for (uint32_t i = 0; i < n; ++i) {
            ids[i] = i;
        }
        return ids;
    }

    struct Node {
        RealClock clock;
        LruStore store{1U << 20U};
        ConsistentHashRing ring;
        FakeTransport transport;
        MetricsCollector metrics;
    };

} // namespace cinder
