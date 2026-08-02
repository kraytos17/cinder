#include "cinder/node/cache_node_server.hpp"

#include "cinder/common/logger.hpp"

using namespace std::chrono;

namespace cinder {

auto
parsePeer(const std::string& peer, ClusterConfig::NodeConfig& out) -> bool {
    auto at = peer.rfind('@');
    if (at == std::string::npos) {
        return false;
    }

    auto colon = peer.find(':', at);
    if (colon == std::string::npos) {
        return false;
    }
    // Empty node id or empty host is malformed — reject.
    if (at == 0 || colon == at + 1) {
        return false;
    }

    out.id = peer.substr(0, at);
    out.host = peer.substr(at + 1, colon - at - 1);
    out.port = static_cast<uint16_t>(std::stoi(peer.substr(colon + 1)));
    return true;
}

CacheNodeServer::CacheNodeServer(CacheNodeServerOptions options)
    : store_(options.capacity),
      transport_(io_),
      repl_(store_, options.node_id, clock_, transport_),
      table_(options.node_id),
      detector_(clock_, transport_, table_, options.node_id, options.ping_interval,
          options.suspect_timeout),
      gossip_(clock_, transport_, table_, options.gossip_interval),
      server_(io_, options.port, store_, ring_, options.node_id, clock_, &repl_,
          options.replica_factor, options.mode, &gossip_),
      replay_timer_(io_),
      gossip_timer_(io_),
      signals_(io_) {
    ring_.addNode(options.node_id);
    table_.seed(options.peers);

    ClusterConfig config;
    config.nodes.push_back({options.node_id, "127.0.0.1", options.port});
    for (const auto& peer : options.peers) {
        ring_.addNode(peer.id);
        config.nodes.push_back(peer);
    }

    transport_.setConfig(config);
    table_.onChange([this] { rebuildRing(); });
}

auto
CacheNodeServer::start() -> Result<void> {
    return server_.start();
}

void
CacheNodeServer::run() {
    signals_.async_wait([this](std::error_code, int) {
        Logger::info("shutting down...");
        shutdown();
    });

    detector_.start();
    gossip_.start();
    scheduleReplay();
    scheduleGossip();
    io_.run();
}

void
CacheNodeServer::shutdown() {
    replay_timer_.cancel();
    gossip_timer_.cancel();
    server_.shutdown();
    io_.stop();
}

void
CacheNodeServer::scheduleReplay() {
    replay_timer_.expires_after(seconds(1));
    replay_timer_.async_wait([this](std::error_code ec) {
        if (ec) {
            return;
        }
        if (repl_.hintCount() > 0) {
            repl_.replayHints([](size_t replayed) {
                if (replayed > 0) {
                    Logger::info("replayed {} hinted write(s)", replayed);
                }
            });
        }
        scheduleReplay();
    });
}

void
CacheNodeServer::scheduleGossip() {
    gossip_timer_.expires_after(gossip_.gossipInterval());
    gossip_timer_.async_wait([this](std::error_code ec) {
        if (ec) {
            return;
        }

        detector_.tick();
        gossip_.tick();
        scheduleGossip();
    });
}

void
CacheNodeServer::rebuildRing() {
    for (const auto& info : table_.snapshot()) {
        if (info.state == NodeState::Alive) {
            ring_.addNode(info.id);
        } else {
            ring_.removeNode(info.id);
        }
    }
}
} // namespace cinder
