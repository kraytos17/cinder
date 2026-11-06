#pragma once

// Shared sim fixtures for the replication sim tests (extracted verbatim
// from replication_sim_test.cpp when read-repair sims moved out). Same
// tokens in every TU that includes this header, and all free functions
// are inline, so sharing is ODR-safe — including under unity builds,
// which merge these sim TUs.

#include <ankerl/unordered_dense.h>
#include <atomic>
#include <chrono>
#include <deque>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "cinder/cluster/transport.hpp"
#include "cinder/node/replication_manager.hpp"
#include "cinder/store/lru_store.hpp"
#include "sim_clock.hpp"
#include "sim_transport.hpp"

namespace cinder {

    using std::chrono::milliseconds;
    using std::chrono::seconds;
    using std::chrono::steady_clock;

    inline auto runWrite(
        ReplicationManager& mgr,
        const std::string& key,
        std::string value,
        std::optional<milliseconds> ttl,
        const std::vector<NodeId>& replicas,
        ConsistencyMode mode
    ) -> Result<void> {
        Result<void> result = err(Error(Errc::InternalError));
        mgr.writeAsync(key, std::move(value), ttl, replicas, mode, [&](Result<void> r) {
            result = r;
        });
        return result;
    }

    // Transport whose completions are deferred until the test pumps them.
    // SimTransport always completes synchronously, so it can only ever
    // exercise fanOutQuorum's inline drain. DeferredTransport additionally
    // exercises the detached async tail: complete some callbacks inline
    // (before the drain runs) and the rest afterwards (while the tail is
    // draining) to cover the handoff between the two phases. The tail runs
    // on asio's system pool, so TSan watches the shared reduce state.
    class DeferredTransport : public Transport {
    public:

        // Nodes in this set complete synchronously inside sendAsync (like a
        // loopback peer); everything else queues until completeSends().
        ankerl::unordered_dense::set<NodeId> sync_nodes;

        void sendAsync(const NodeId& to, const net::Request&, SendCallback on_done) override {
            if (sync_nodes.contains(to)) {
                on_done(ok());
                return;
            }
            pending_sends_.push_back(std::make_shared<SendCallback>(std::move(on_done)));
        }

        void sendRequestAsync(
            const NodeId&,
            const net::Request&,
            RequestCallback on_done
        ) override {
            pending_requests_.push_back(std::move(on_done));
        }

        void onMessage(MessageHandler) override {}

        auto pendingSends() const -> size_t {
            return pending_sends_.size();
        }

        auto pendingRequests() const -> size_t {
            return pending_requests_.size();
        }

        void completeSends(size_t n, bool ok = true) {
            for (size_t i = 0; i < n && !pending_sends_.empty(); ++i) {
                auto cb = std::move(pending_sends_.front());
                pending_sends_.pop_front();
                if (ok) {
                    (*cb)(cinder::ok());
                } else {
                    (*cb)(cinder::err(Error(Errc::NotReady, "deferred failure")));
                }
            }
        }

        // Borrow the i-th outstanding send without removing it: lets a test
        // deliver the same completion twice (transport double-fire) or
        // deliver out of order. The borrowed callback stays queued until a
        // later completeSends() pops it.
        auto peekSend(size_t i) -> std::shared_ptr<SendCallback> {
            EXPECT_LT(i, pending_sends_.size());
            return pending_sends_.at(i);
        }

        void completeRequest(Version version, uint64_t writer, std::string value) {
            EXPECT_FALSE(pending_requests_.empty());
            auto cb = std::move(pending_requests_.front());
            pending_requests_.pop_front();
            net::Response resp;
            resp.status = Errc::OK;
            resp.value = std::move(value);
            resp.version = version;
            resp.writer_node_hash = writer;
            cb(std::move(resp));
        }

    private:

        std::deque<std::shared_ptr<SendCallback>> pending_sends_;
        std::deque<RequestCallback> pending_requests_;
    };

    struct DeferredCluster {
        SimClock clock;
        DeferredTransport transport;
        LruStore store{1024, &clock};
        ReplicationManager mgr{store, "node1", clock, transport};
    };

    // Block (up to a generous deadline) until the async decision arrives.
    // Failure means the quorum never decided — the bug being guarded.
    inline auto awaitDecision(std::atomic<int>& done_count) -> bool {
        auto deadline = steady_clock::now() + seconds(10);
        while (done_count.load() == 0) {
            if (steady_clock::now() >= deadline) {
                return false;
            }
            std::this_thread::sleep_for(milliseconds(1));
        }
        return true;
    }

} // namespace cinder
