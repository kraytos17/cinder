#include "cinder/cluster/gossip.hpp"

#include <charconv>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "cinder/common/logger.hpp"
#include "cinder/net/protocol.hpp"

using std::chrono::milliseconds;

namespace cinder {

GossipManager::GossipManager(Clock& clock, Transport& transport, MembershipTable& table,
    const NodeId& self, milliseconds gossip_interval)
    : clock_(clock),
      transport_(transport),
      table_(table),
      self_(self),
      gossip_interval_(gossip_interval) {}

void
GossipManager::leave() {
    // Bump incarnation so the Dead rumor about self overrides any stale Alive
    // rumor held by peers.
    table_.markDead(self_);

    Logger::info("cinder gossip: broadcasting leave to all peers");
    std::vector<NodeId> targets;
    {
        std::scoped_lock lk(peers_mutex_);
        targets = peers_;
    }

    for (const auto& peer : targets) {
        Logger::debug("cinder gossip: sending leave to peer={}", peer);
        sendView(peer);
    }
}

void
GossipManager::start() {
    {
        std::scoped_lock lk(peers_mutex_);
        rebuildPeersLocked();
    }
    Logger::info("cinder gossip: started interval={}ms",
        std::chrono::duration_cast<milliseconds>(gossip_interval_).count());

    // Late joiners discovered via gossip/failure-detection must be added
    // dynamically after start(); each arrival fires this callback.
    table_.onChange([this] {
        std::scoped_lock lk(peers_mutex_);
        rebuildPeersLocked();
    });
}

void
GossipManager::tick() {
    NodeId target;
    {
        std::scoped_lock lk(peers_mutex_);
        if (peers_.empty()) {
            Logger::debug("cinder gossip: no peers to gossip to");
            return;
        }

        static thread_local std::mt19937 rng(
            static_cast<uint32_t>(clock_.now().time_since_epoch().count()));
        std::uniform_int_distribution<size_t> dist(0, peers_.size() - 1);
        target = peers_[dist(rng)];
    }
    Logger::debug("cinder gossip: gossip round target={}", target);
    sendView(target);
}

void
GossipManager::handleMessage(const NodeId& from, const net::Request& req) {
    auto rumors = decodeView(req.value);
    Logger::debug("cinder gossip: state disseminated from={} entries={}", from, rumors.size());
    for (const auto& rumor : rumors) {
        table_.applyRumor(from, rumor);
    }
}

void
GossipManager::rebuildPeersLocked() {
    peers_.clear();
    for (const auto& info : table_.snapshot()) {
        if (info.id != self_) {
            peers_.push_back(info.id);
        }
    }
    std::sort(peers_.begin(), peers_.end());
}

void
GossipManager::sendView(const NodeId& to) {
    net::Request req;
    req.opcode = net::Opcode::Gossip;
    req.value = encodeView(table_.snapshot());
    transport_.sendAsync(to, req, [](Result<void> /*r*/) {});
}

auto
GossipManager::encodeView(const std::vector<NodeInfo>& view) -> std::string {
    std::string out;
    for (const auto& info : view) {
        if (!out.empty()) {
            out.push_back(';');
        }

        out += info.id;
        out.push_back('@');
        out += info.host;
        out.push_back(':');
        out += std::to_string(info.port);
        out.push_back(':');
        switch (info.state) {
            case NodeState::Alive:
                out += "alive";
                break;
            case NodeState::Suspect:
                out += "suspect";
                break;
            case NodeState::Dead:
                out += "dead";
                break;
        }
        out.push_back(':');
        out += std::to_string(info.incarnation);
    }
    return out;
}

namespace gossip {

// Parse one "id@host:port:state:incarnation" entry. Returns false on malformed
// input.
auto
parseEntry(std::string_view entry, cinder::NodeInfo& out) -> bool {
    auto at = entry.find('@');
    auto colon1 = entry.find(':', at);
    auto colon2 =
        colon1 == std::string_view::npos ? std::string_view::npos : entry.find(':', colon1 + 1);
    auto colon3 =
        colon2 == std::string_view::npos ? std::string_view::npos : entry.find(':', colon2 + 1);
    if (at == std::string_view::npos || colon1 == std::string_view::npos
        || colon2 == std::string_view::npos || colon3 == std::string_view::npos) {
        return false;
    }

    out.id = std::string(entry.substr(0, at));
    out.host = std::string(entry.substr(at + 1, colon1 - at - 1));
    auto port_str = entry.substr(colon1 + 1, colon2 - colon1 - 1);
    uint16_t port = 0;
    auto [port_end, port_ec] =
        std::from_chars(port_str.data(), port_str.data() + port_str.size(), port);
    if (port_ec != std::errc{} || port_end != port_str.data() + port_str.size()) {
        return false;
    }

    out.port = port;
    auto state_str = entry.substr(colon2 + 1, colon3 - colon2 - 1);
    if (state_str == "alive") {
        out.state = cinder::NodeState::Alive;
    } else if (state_str == "suspect") {
        out.state = cinder::NodeState::Suspect;
    } else if (state_str == "dead") {
        out.state = cinder::NodeState::Dead;
    } else {
        return false;
    }

    auto inc_str = entry.substr(colon3 + 1);
    uint64_t incarnation = 0;
    auto [inc_end, inc_ec] =
        std::from_chars(inc_str.data(), inc_str.data() + inc_str.size(), incarnation);
    if (inc_ec != std::errc{} || inc_end != inc_str.data() + inc_str.size()) {
        return false;
    }
    out.incarnation = incarnation;
    return true;
}
} // namespace gossip

auto
GossipManager::decodeView(std::string_view value) -> std::vector<NodeInfo> {
    std::vector<NodeInfo> result;
    size_t malformed = 0;
    size_t start = 0;
    while (start < value.size()) {
        auto end = value.find(';', start);
        auto entry =
            value.substr(start, end == std::string_view::npos ? value.size() - start : end - start);
        if (!entry.empty()) {
            NodeInfo info;
            if (gossip::parseEntry(entry, info)) {
                result.push_back(std::move(info));
            } else {
                ++malformed;
            }
        }
        if (end == std::string_view::npos) {
            break;
        }
        start = end + 1;
    }
    if (malformed > 0) {
        Logger::debug("cinder gossip: {} malformed entries rejected", malformed);
    }
    return result;
}
} // namespace cinder
