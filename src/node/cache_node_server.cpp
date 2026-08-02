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

    out.id = peer.substr(0, at);
    out.host = peer.substr(at + 1, colon - at - 1);
    out.port = static_cast<uint16_t>(std::stoi(peer.substr(colon + 1)));
    return true;
}

CacheNodeServer::CacheNodeServer(CacheNodeServerOptions options)
    : store_(options.capacity),
      transport_(io_),
      repl_(store_, options.node_id, clock_, transport_),
      server_(io_, options.port, store_, ring_, options.node_id, &repl_, options.replica_factor,
          options.mode),
      replay_timer_(io_),
      signals_(io_) {
    ring_.addNode(options.node_id);

    ClusterConfig config;
    config.nodes.push_back({options.node_id, "127.0.0.1", options.port});
    for (const auto& peer : options.peers) {
        ring_.addNode(peer.id);
        config.nodes.push_back(peer);
    }
    transport_.setConfig(config);
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

    scheduleReplay();
    io_.run();
}

void
CacheNodeServer::shutdown() {
    replay_timer_.cancel();
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
} // namespace cinder
