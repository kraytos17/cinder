#include "cinder/node/cache_node_server.hpp"

#include <chrono>
#include <csignal>

#include "cinder/common/config.hpp"
#include "cinder/common/logger.hpp"
#include "cinder/store/lfu_store.hpp"
#include "cinder/store/lru_store.hpp"

using std::chrono::seconds;

namespace cinder {
namespace {
auto
makeStore(const CacheNodeServerOptions& opts, Clock* clock) -> std::unique_ptr<CacheStore> {
    if (opts.eviction_policy == "lfu") {
        return std::make_unique<LfuStore>(opts.capacity, clock);
    }
    return std::make_unique<LruStore>(opts.capacity, clock);
}
} // namespace

#ifdef CINDER_ENABLE_TLS
namespace {
auto
initSslContext(const CacheNodeServerOptions& opts) -> std::optional<asio::ssl::context> {
    if (!opts.tls_enabled || opts.tls_cert_file.empty() || opts.tls_key_file.empty()) {
        return std::nullopt;
    }

    asio::ssl::context ctx(asio::ssl::context::tlsv12_server);
    ctx.use_certificate_chain_file(opts.tls_cert_file);
    ctx.use_private_key_file(opts.tls_key_file, asio::ssl::context::pem);
    if (!opts.tls_ca_file.empty()) {
        ctx.load_verify_file(opts.tls_ca_file);
        ctx.set_verify_mode(asio::ssl::verify_peer);
    }
    return ctx;
}
} // namespace
#endif

CacheNodeServer::CacheNodeServer(CacheNodeServerOptions options)
    : node_id_(options.node_id),
      ping_interval_(options.ping_interval),
      quarantine_interval_(options.quarantine_interval),
      io_threads_(options.io_threads),
#ifdef CINDER_ENABLE_TLS
      ssl_ctx_(initSslContext(options)),
#endif
      store_(makeStore(options, &clock_)),
      persistence_(
          PersistenceManager::Options{
              .data_dir = options.data_dir,
              .enabled = options.persistence_enabled,
              .snapshot_interval_s = options.snapshot_interval_s,
              .max_wal_entries = options.max_wal_entries,
          },
          *store_, &clock_),
      transport_(io_
#ifdef CINDER_ENABLE_TLS
          ,
          ssl_ctx_ ? &*ssl_ctx_ : nullptr
#endif
          ),
      repl_(*store_, options.node_id, clock_, transport_),
      table_(options.node_id),
      detector_(clock_, transport_, table_, options.node_id, options.suspect_timeout),
      gossip_(clock_, transport_, table_, options.node_id, options.gossip_interval),
      shard_(*store_, ring_, transport_, table_, options.node_id, clock_, options.replica_factor,
          options.quarantine_interval),
      server_(io_, options.port, *store_, ring_, options.node_id, clock_, &repl_,
          options.replica_factor, options.mode, &gossip_, options.metrics_port, &metrics_,
          [this]() { return formatConfigJson(current_config_); }
#ifdef CINDER_ENABLE_TLS
          ,
          ssl_ctx_ ? &*ssl_ctx_ : nullptr
#endif
          ),
      replay_timer_(io_),
      gossip_timer_(io_),
      probe_timer_(io_),
      evict_timer_(io_),
      quarantine_timer_(io_),
      compact_timer_(io_),
      config_reload_timer_(io_),
      signals_(io_),
      metrics_port_(options.metrics_port),
      current_config_(options.config),
      config_path_(options.config_path) {
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
    transport_.setRpcTimeout(options.rpc_timeout);
    table_.onChange([this] { rebuildRing(); });
    if (persistence_.enabled()) {
        store_->setPersistence(&persistence_);
    }

    store_->setMetrics(&metrics_);
    repl_.setMetrics(&metrics_);
    detector_.setMetrics(&metrics_);
    gossip_.setMetrics(&metrics_);
    shard_.setMetrics(&metrics_);
    Logger::info("eviction policy: {}", options.eviction_policy);
}

auto
CacheNodeServer::start() -> Result<void> {
    return server_.start();
}

void
CacheNodeServer::run() {
    // Recover from disk before serving
    if (persistence_.enabled()) {
        auto res = persistence_.recover();
        if (!res.has_value()) {
            Logger::error("persistence recovery failed: {}", res.error().message());
            return;
        }
        Logger::info("recovered {} entries from disk", store_->size());
    }

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
    if (persistence_.enabled()) {
        scheduleCompact();
    }

    scheduleConfigReload();
    unsigned workers = io_threads_ > 0 ? static_cast<unsigned>(io_threads_) : 1;
    if (io_threads_ == 0) {
        auto hw = std::thread::hardware_concurrency();
        workers = hw == 0 ? 1 : std::min(4U, hw);
    }

    Logger::info("cinder node: io threads={}", workers);
    if (metrics_port_ > 0) {
        Logger::info("cinder node: metrics endpoint on port={}", metrics_port_);
    }
    if (workers <= 1) {
        io_.run();
    } else {
        std::vector<std::thread> pool;
        pool.reserve(workers - 1);
        for (unsigned i = 1; i < workers; ++i) {
            pool.emplace_back([this]() { io_.run(); });
        }

        io_.run(); // this thread participates too
        for (auto& w : pool) {
            w.join();
        }
        // Catch stragglers: handlers that enqueued WAL writes while the pool
        // was winding down after persistence_.shutdown() ran.
        if (persistence_.enabled()) {
            persistence_.flush();
        }
    }
}

void
CacheNodeServer::shutdown() {
    replay_timer_.cancel();
    gossip_timer_.cancel();
    probe_timer_.cancel();
    evict_timer_.cancel();
    quarantine_timer_.cancel();
    compact_timer_.cancel();
    config_reload_timer_.cancel();
    if (persistence_.enabled()) {
        persistence_.shutdown();
    }

    // Close server first so no new requests are accepted and in-flight writes
    // complete before peers are notified of shutdown. This prevents broken-pipe
    // errors: peers would otherwise close their connections after receiving the
    // leave gossip while the server still has pending response writes.
    server_.shutdown();
    while (io_.poll() > 0) {
    }

    // Now broadcast leave — peers will close their connections, but our server
    // is already closed so there are no pending writes to fail.
    gossip_.leave();
    while (io_.poll() > 0) {
    }

    transport_.shutdown();
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

        store_->evictExpired();
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

void
CacheNodeServer::scheduleCompact() {
    compact_timer_.expires_after(seconds(persistence_.snapshotInterval()));
    compact_timer_.async_wait([this](std::error_code ec) {
        if (ec) {
            return;
        }
        if (auto result = persistence_.compact(); !result.has_value()) {
            Logger::error("compact failed: {}", result.error().message());
        }
        scheduleCompact();
    });
}

void
CacheNodeServer::scheduleConfigReload() {
    config_reload_timer_.expires_after(seconds(5));
    config_reload_timer_.async_wait([this](std::error_code ec) {
        if (ec) {
            return;
        }
        applyConfig();
        scheduleConfigReload();
    });
}

void
CacheNodeServer::applyConfig() {
    if (config_path_.empty()) {
        return;
    }

    auto new_config = loadConfig(config_path_);
    if (!new_config.has_value()) {
        return;
    }

    auto changed = diffConfig(current_config_, *new_config);
    if (changed.empty()) {
        return;
    }

    std::string changed_str;
    for (size_t i = 0; i < changed.size(); ++i) {
        if (i > 0) {
            changed_str += ", ";
        }
        changed_str += changed[i];
    }

    Logger::info("config changed: {}", changed_str);
    current_config_ = *new_config;
    for (const auto& field : changed) {
        if (field == "log_level") {
            Logger::setLevel(logLevelFromString(new_config->log_level));
        } else if (field == "ping_interval_ms") {
            Logger::warn("config field '{}' requires restart to take effect", field);
        } else if (field == "suspect_timeout_ms") {
            detector_.setSuspectTimeout(milliseconds(new_config->suspect_timeout_ms));
        } else if (field == "gossip_interval_ms") {
            gossip_.setGossipInterval(milliseconds(new_config->gossip_interval_ms));
        } else if (field == "rpc_timeout_ms") {
            transport_.setRpcTimeout(milliseconds(new_config->rpc_timeout_ms));
        } else if (field == "capacity") {
            store_->setCapacity(new_config->capacity);
        } else if (field == "quarantine_interval_ms") {
            quarantine_interval_ = milliseconds(new_config->quarantine_interval_ms);
        } else {
            Logger::warn("config field '{}' requires restart to take effect", field);
        }
    }
}
} // namespace cinder
