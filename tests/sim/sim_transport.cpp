#include "sim_transport.hpp"

#include <algorithm>
#include <utility>

SimBus::SimBus(SimClock& clock, uint64_t seed)
    : clock_(clock),
      rng_(seed) {}

void
SimBus::registerHandler(const cinder::NodeId& id, Handler handler) {
    handlers_[id] = std::move(handler);
}

auto
SimBus::route(const cinder::NodeId& from, const cinder::NodeId& to, const cinder::net::Request& req)
    -> bool {
    if (down_.contains(to)) {
        return false;
    }

    Event ev;
    ev.deliver_at = clock_.now() + delay_;
    ev.from = from;
    ev.to = to;
    ev.req = req;
    queue_.push(std::move(ev));
    return true;
}

void
SimBus::deliver() {
    std::vector<Event> ready;
    while (!queue_.empty() && queue_.top().deliver_at <= clock_.now()) {
        ready.push_back(std::move(const_cast<Event&>(queue_.top())));
        queue_.pop();
    }
    if (reorder_ && ready.size() >= 2) {
        std::reverse(ready.begin(), ready.end());
    }
    for (auto& ev : ready) {
        if (unit_(rng_) < loss_rate_) {
            ++dropped_;
            continue;
        }

        auto it = handlers_.find(ev.to);
        if (it != handlers_.end()) {
            it->second(ev.from, ev.req);
        }
        ++delivered_;
    }
}

void
SimBus::setLossRate(double p) {
    loss_rate_ = p;
}

void
SimBus::setDelay(std::chrono::milliseconds d) {
    delay_ = d;
}

void
SimBus::setReorder(bool enabled) {
    reorder_ = enabled;
}

void
SimBus::setNodeDown(const cinder::NodeId& id) {
    down_.insert(id);
}

void
SimBus::setNodeUp(const cinder::NodeId& id) {
    down_.erase(id);
}

auto
SimBus::pending() const -> size_t {
    return queue_.size();
}

auto
SimBus::deliveredCount() const -> size_t {
    return delivered_;
}

auto
SimBus::droppedCount() const -> size_t {
    return dropped_;
}

SimTransport::SimTransport(SimBus& bus, cinder::NodeId from)
    : bus_(bus),
      from_(std::move(from)) {}

void
SimTransport::sendAsync(const cinder::NodeId& to, const cinder::net::Request& req,
    cinder::Transport::SendCallback on_done) {
    if (!bus_.route(from_, to, req)) {
        on_done(cinder::err(cinder::Error(cinder::Errc::NotReady, "target node down")));
        return;
    }
    on_done(cinder::ok());
}

void
SimTransport::onMessage(MessageHandler handler) {
    bus_.registerHandler(from_, std::move(handler));
}
