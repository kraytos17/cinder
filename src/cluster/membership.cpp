#include "cinder/cluster/membership.hpp"

#include <utility>

using std::chrono::milliseconds;
using std::chrono::steady_clock;

namespace cinder {

MembershipTable::MembershipTable(NodeId self)
    : self_(std::move(self)) {
    nodes_[self_] = {.id = self_, .host = {}, .state = NodeState::Alive, .incarnation = 0};
}

void
MembershipTable::setSelfAddress(const std::string& host, uint16_t port) {
    std::scoped_lock lock(mutex_);
    nodes_[self_].host = host;
    nodes_[self_].port = port;
}

void
MembershipTable::seed(const std::vector<ClusterConfig::NodeConfig>& peers) {
    std::scoped_lock lock(mutex_);
    for (const auto& peer : peers) {
        if (peer.id == self_) {
            continue;
        }

        NodeInfo info;
        info.id = peer.id;
        info.host = peer.host;
        info.port = peer.port;
        info.state = NodeState::Alive;
        info.joined_at = steady_clock::now();
        nodes_[peer.id] = info;
    }
}

void
MembershipTable::applyRumor(const NodeId& /*from*/, const NodeInfo& rumor) {
    bool changed = false;
    {
        std::scoped_lock lock(mutex_);

        auto it = nodes_.find(rumor.id);
        if (it == nodes_.end()) {
            if (rumor.state != NodeState::Alive) {
                return;
            }

            NodeInfo fresh = rumor;
            fresh.joined_at = steady_clock::now();
            nodes_[rumor.id] = fresh;
            changed = true; // newly-discovered node — observers must rebuild
        } else {
            NodeInfo& local = it->second;
            if (rumor.incarnation < local.incarnation) {
                return; // stale rumor — ignore
            }
            if (rumor.incarnation == local.incarnation && rumor.state == local.state) {
                return; // no change
            }
            if (rumor.id == self_ && rumor.state != NodeState::Alive) {
                changed = refuteSelfRumor(rumor);
            } else {
                bool state_changed = local.state != rumor.state;
                bool incarnation_changed = local.incarnation != rumor.incarnation;
                local.state = rumor.state;
                local.incarnation = rumor.incarnation;
                if (!local.host.empty() && rumor.host.empty()) {
                    local.host = rumor.host;
                }
                if (local.port == 0 && rumor.port != 0) {
                    local.port = rumor.port;
                }
                changed = state_changed || incarnation_changed;
            }
        }
    }
    if (changed) {
        fireCallbacks();
    }
}

void
MembershipTable::markSuspect(const NodeId& id) {
    bool changed = false;
    {
        std::scoped_lock lock(mutex_);
        auto it = nodes_.find(id);
        if (it == nodes_.end() || it->second.state == NodeState::Dead) {
            return;
        }
        if (it->second.state == NodeState::Suspect) {
            return;
        }
        it->second.state = NodeState::Suspect;
        changed = true;
    }
    if (changed) {
        fireCallbacks();
    }
}

void
MembershipTable::markDead(const NodeId& id) {
    bool changed = false;
    {
        std::scoped_lock lock(mutex_);
        auto it = nodes_.find(id);
        if (it == nodes_.end() || it->second.state == NodeState::Dead) {
            return;
        }
        it->second.state = NodeState::Dead;
        changed = true;
    }
    if (changed) {
        fireCallbacks();
    }
}

void
MembershipTable::markAlive(const NodeId& id, uint64_t incarnation) {
    bool changed = false;
    {
        std::scoped_lock lock(mutex_);
        auto it = nodes_.find(id);
        if (it == nodes_.end()) {
            return;
        }

        NodeInfo& info = it->second;
        if (info.state == NodeState::Alive && info.incarnation >= incarnation) {
            return;
        }
        // A node recovering from Dead/Suspect starts a fresh quarantine window.
        if (info.state != NodeState::Alive) {
            info.joined_at = steady_clock::now();
        }

        info.state = NodeState::Alive;
        info.incarnation = incarnation;
        changed = true;
    }
    if (changed) {
        fireCallbacks();
    }
}

auto
MembershipTable::get(const NodeId& id) const -> std::optional<NodeInfo> {
    std::scoped_lock lock(mutex_);
    auto it = nodes_.find(id);
    if (it == nodes_.end()) {
        return std::nullopt;
    }
    return it->second;
}

auto
MembershipTable::aliveCount() const -> size_t {
    std::scoped_lock lock(mutex_);
    size_t count = 0;
    for (const auto& [id, info] : nodes_) {
        (void)id;
        if (info.state == NodeState::Alive) {
            ++count;
        }
    }
    return count;
}

auto
MembershipTable::visibleAliveCount() const -> size_t {
    std::scoped_lock lock(mutex_);
    size_t count = 0;
    for (const auto& [id, info] : nodes_) {
        (void)id;
        if (info.state == NodeState::Alive) {
            ++count;
        }
    }
    return count;
}

auto
MembershipTable::expectedClusterSize() const -> size_t {
    std::scoped_lock lock(mutex_);
    return nodes_.size();
}

auto
MembershipTable::isDegraded() const -> bool {
    std::scoped_lock lock(mutex_);
    size_t total = nodes_.size();
    if (total == 0) {
        return true;
    }
    size_t visible = 0;
    for (const auto& [id, info] : nodes_) {
        (void)id;
        if (info.state == NodeState::Alive) {
            ++visible;
        }
    }
    return visible < total / 2 + 1;
}

auto
MembershipTable::snapshot() const -> std::vector<NodeInfo> {
    std::scoped_lock lock(mutex_);
    std::vector<NodeInfo> result;
    result.reserve(nodes_.size());
    for (const auto& [id, info] : nodes_) {
        (void)id;
        result.push_back(info);
    }
    return result;
}

auto
MembershipTable::isQuarantined(
    const NodeId& id, steady_clock::time_point now, milliseconds quarantine) const -> bool {
    if (quarantine.count() <= 0) {
        return false;
    }

    std::scoped_lock lock(mutex_);
    auto it = nodes_.find(id);
    if (it == nodes_.end() || it->second.state != NodeState::Alive) {
        return false;
    }
    return now - it->second.joined_at < quarantine;
}

void
MembershipTable::onChange(ChangeCallback cb) {
    std::scoped_lock lock(mutex_);
    cbs_.push_back(std::move(cb));
}

void
MembershipTable::fireCallbacks() {
    std::vector<ChangeCallback> cbs;
    {
        std::scoped_lock lock(mutex_);
        cbs = cbs_;
    }
    for (auto& cb : cbs) {
        cb();
    }
}

bool
MembershipTable::refuteSelfRumor(const NodeInfo& rumor) {
    NodeInfo& self = nodes_[self_];
    bool changed = false;
    if (self.incarnation <= rumor.incarnation) {
        self.incarnation = rumor.incarnation + 1;
        changed = true;
    }
    if (self.state != NodeState::Alive) {
        self.state = NodeState::Alive;
        changed = true;
    }
    return changed;
}
} // namespace cinder
