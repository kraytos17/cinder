#include "cinder/cluster/membership.hpp"

#include <utility>

namespace cinder {

MembershipTable::MembershipTable(NodeId self)
    : self_(std::move(self)) {
    nodes_[self_] = {.id = self_, .host = {}, .state = NodeState::Alive, .incarnation = 0};
}

void
MembershipTable::seed(const std::vector<ClusterConfig::NodeConfig>& peers) {
    std::scoped_lock lock(mutex_);
    for (const auto& peer : peers) {
        if (peer.id == self_) {
            continue;
        }
        nodes_[peer.id] = {
            .id = peer.id,
            .host = peer.host,
            .port = peer.port,
            .state = NodeState::Alive,
        };
    }
}

void
MembershipTable::applyRumor(const NodeId& /*from*/, const NodeInfo& rumor) {
    std::scoped_lock lock(mutex_);

    auto it = nodes_.find(rumor.id);
    if (it == nodes_.end()) {
        // Unknown node: accept if the rumor claims it Alive (a peer we don't
        // know about shouldn't be propagated as suspect/dead without a base).
        if (rumor.state != NodeState::Alive) {
            return;
        }
        nodes_[rumor.id] = rumor;
        return;
    }

    NodeInfo& local = it->second;
    if (rumor.incarnation < local.incarnation) {
        return; // stale rumor — ignore
    }
    if (rumor.incarnation == local.incarnation && rumor.state == local.state) {
        return; // no change
    }
    // Refute a Suspect/Dead rumor about ourselves with a higher incarnation.
    if (rumor.id == self_ && rumor.state != NodeState::Alive) {
        refuteSelfRumor(rumor);
        return;
    }

    bool changed = local.state != rumor.state || local.incarnation != rumor.incarnation;
    local.state = rumor.state;
    local.incarnation = rumor.incarnation;
    if (!local.host.empty() && rumor.host.empty()) {
        local.host = rumor.host;
    }
    if (local.port == 0 && rumor.port != 0) {
        local.port = rumor.port;
    }
    if (changed) {
        for (auto& cb : cbs_) {
            cb();
        }
    }
}

void
MembershipTable::markSuspect(const NodeId& id) {
    std::scoped_lock lock(mutex_);
    auto it = nodes_.find(id);
    if (it == nodes_.end() || it->second.state == NodeState::Dead) {
        return;
    }
    if (it->second.state == NodeState::Suspect) {
        return;
    }

    it->second.state = NodeState::Suspect;
    for (auto& cb : cbs_) {
        cb();
    }
}

void
MembershipTable::markDead(const NodeId& id) {
    std::scoped_lock lock(mutex_);
    auto it = nodes_.find(id);
    if (it == nodes_.end() || it->second.state == NodeState::Dead) {
        return;
    }

    it->second.state = NodeState::Dead;
    for (auto& cb : cbs_) {
        cb();
    }
}

void
MembershipTable::markAlive(const NodeId& id, uint64_t incarnation) {
    std::scoped_lock lock(mutex_);
    auto it = nodes_.find(id);
    if (it == nodes_.end()) {
        return;
    }

    NodeInfo& info = it->second;
    if (info.state == NodeState::Alive && info.incarnation >= incarnation) {
        return;
    }

    info.state = NodeState::Alive;
    info.incarnation = incarnation;
    for (auto& cb : cbs_) {
        cb();
    }
}

auto
MembershipTable::get(const NodeId& id) const -> const NodeInfo* {
    std::scoped_lock lock(mutex_);
    auto it = nodes_.find(id);
    if (it == nodes_.end()) {
        return nullptr;
    }
    return &it->second;
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

void
MembershipTable::onChange(ChangeCallback cb) {
    std::scoped_lock lock(mutex_);
    cbs_.push_back(std::move(cb));
}

void
MembershipTable::refuteSelfRumor(const NodeInfo& rumor) {
    NodeInfo& self = nodes_[self_];
    if (self.incarnation <= rumor.incarnation) {
        self.incarnation = rumor.incarnation + 1;
    }
    if (self.state != NodeState::Alive) {
        self.state = NodeState::Alive;
        for (auto& cb : cbs_) {
            cb();
        }
    }
}
} // namespace cinder
