#include "cinder/node/cache_node_server.hpp"

#include <csignal>

#include "cinder/common/logger.hpp"

using std::chrono::seconds;

namespace cinder {

CacheNodeServer::CacheNodeServer(CacheNodeServerOptions options)
    : node_id_(options.node_id),
      ping_interval_(options.ping_interval),
      quarantine_interval_(options.quarantine_interval),
      store_(options.capacity),
      transport_(io_),
      repl_(store_, options.node_id, clock_, transport_),
      table_(options.node_id),
      detector_(clock_, transport_, table_, options.node_id, options.suspect_timeout),
      gossip_(clock_, transport_, table_, options.gossip_interval),
      shard_(store_, ring_, transport_, table_, options.node_id, clock_, options.replica_factor,
          options.quarantine_interval),
      server_(io_, options.port, store_, ring_, options.node_id, clock_, &repl_,
          options.replica_factor, options.mode, &gossip_),
      replay_timer_(io_),
      gossip_timer_(io_),
      probe_timer_(io_),
      evict_timer_(io_),
      quarantine_timer_(io_),
      signals_(io_) {
    signals_.add(SIGINT);
    signals_.add(SIGTERM);
    ring_.addNode(options.node_id);
    table_.seed(options.peers);
    table_.setSelfAddress("127.0.0.1", options.port);

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
    scheduleProbe();
    scheduleEvict();
    io_.run();
}

void
CacheNodeServer::shutdown() {
    replay_timer_.cancel();
    gossip_timer_.cancel();
    probe_timer_.cancel();
    evict_timer_.cancel();
    quarantine_timer_.cancel();
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

        gossip_.tick();
        scheduleGossip();
    });
}

void
CacheNodeServer::scheduleProbe() {
    probe_timer_.expires_after(ping_interval_);
    probe_timer_.async_wait([this](std::error_code ec) {
        if (ec) {
            return;
        }

        detector_.tick();
        scheduleProbe();
    });
}

void
CacheNodeServer::scheduleEvict() {
    evict_timer_.expires_after(seconds(1));
    evict_timer_.async_wait([this](std::error_code ec) {
        if (ec) {
            return;
        }

        store_.evictExpired();
        scheduleEvict();
    });
}

void
CacheNodeServer::rebuildRing() {
    for (const auto& info : table_.snapshot()) {
        if (info.id == node_id_) {
            continue;
        }

        transport_.addAddr(info.id, info.host, info.port);
        if (info.state == NodeState::Alive) {
            ring_.addNode(info.id);
        } else {
            ring_.removeNode(info.id);
        }
    }
    // Push keys this node no longer owns to their new ring owners. If some were
    // deferred because their owner is still quarantined, retry once the window
    // has elapsed.
    if (shard_.rebalance()) {
        scheduleRebalance();
    }
}

void
CacheNodeServer::scheduleRebalance() {
    if (quarantine_interval_.count() <= 0) {
        return;
    }
    quarantine_timer_.expires_after(quarantine_interval_);
    quarantine_timer_.async_wait([this](std::error_code ec) {
        if (ec) {
            return;
        }
        // Keep retrying while any owner is still inside its quarantine window
        // (e.g. a flapping node keeps resetting joined_at).
        if (shard_.rebalance()) {
            scheduleRebalance();
        }
    });
}
} // namespace cinder
