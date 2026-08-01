#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <queue>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cinder/cluster/transport.hpp"
#include "sim_clock.hpp"

// Shared in-process message bus. `route` accepts a message for delivery at
// clock.now() + delay_ unless the target is down. Loss is applied at delivery
// time (a queued message may be silently dropped). Deterministic: same seed →
// same delivery schedule.
class SimBus {
  public:
    using Handler = std::move_only_function<void(const cinder::NodeId& from, const cinder::net::Request&)>;

    explicit SimBus(SimClock& clock, uint64_t seed);

    void registerHandler(const cinder::NodeId& id, Handler handler);

    // Returns false if the target is down (message not accepted).
    auto route(const cinder::NodeId& from, const cinder::NodeId& to,
        const cinder::net::Request& req) -> bool;

    // Deliver every scheduled message whose time has come (<= clock.now()).
    void deliver();

    void setLossRate(double p);                        // [0,1]
    void setDelay(std::chrono::milliseconds d);
    void setReorder(bool enabled);                     // swap consecutive deliveries
    void setNodeDown(const cinder::NodeId& id);
    void setNodeUp(const cinder::NodeId& id);

    auto pending() const -> size_t;
    auto deliveredCount() const -> size_t;
    auto droppedCount() const -> size_t;

  private:
    struct Event {
        std::chrono::steady_clock::time_point deliver_at;
        cinder::NodeId from;
        cinder::NodeId to;
        cinder::net::Request req;
    };
    struct EventCmp {
        auto operator()(const Event& a, const Event& b) const -> bool {
            return a.deliver_at > b.deliver_at;
        }
    };

    SimClock& clock_;
    std::mt19937 rng_;
    std::uniform_real_distribution<double> unit_{0.0, 1.0};
    double loss_rate_ = 0.0;
    std::chrono::milliseconds delay_{0};
    bool reorder_ = false;

    std::priority_queue<Event, std::vector<Event>, EventCmp> queue_;
    std::unordered_map<cinder::NodeId, Handler> handlers_;
    std::unordered_set<cinder::NodeId> down_;

    size_t delivered_ = 0;
    size_t dropped_ = 0;
};

// Per-node view of a shared SimBus. Satisfies cinder::Transport so a
// ReplicationManager can send/register against it.
class SimTransport final : public cinder::Transport {
  public:
    SimTransport(SimBus& bus, cinder::NodeId from);

    auto send(const cinder::NodeId& to, const cinder::net::Request& req)
        -> cinder::Result<void> override;
    void onMessage(MessageHandler handler) override;

  private:
    SimBus& bus_;
    cinder::NodeId from_;
};
