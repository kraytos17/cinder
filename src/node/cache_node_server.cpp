#include "cinder/node/cache_node_server.hpp"

#include <chrono>
#include <csignal>
#include <string_view>

#include "cinder/common/config.hpp"
#include "cinder/common/tracing.hpp"
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

auto
isIdentityField(std::string_view field) -> bool {
    // Identity fields can never hot-apply and normally differ only because
    // CLI flags shadow the file — keep them out of the change report.
    return field == "node_id" || field == "port" || field == "peers" || field == "replica_factor"
           || field == "consistency";
}
} // namespace

void
CacheNodeServer::syncEffectiveConfig() {
    current_config_.node_id = node_id_;
    current_config_.port = port_;
    current_config_.capacity = capacity_;
    current_config_.peers = peers_;
    current_config_.replica_factor = replica_factor_;
    current_config_.consistency = (mode_ == ConsistencyMode::Quorum) ? "quorum" : "async";
}

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
    : ping_interval_(options.ping_interval),
      quarantine_interval_(options.quarantine_interval),
      store_(makeStore(options, &clock_)),
#ifdef CINDER_ENABLE_TLS
      ssl_ctx_(initSslContext(options)),
#endif
      anti_entropy_interval_(options.anti_entropy_interval),
      capacity_(options.capacity),
      peers_(options.peers),
      node_id_(options.node_id),
      config_path_(options.config_path),
      anti_entropy_(*store_, ring_, options.node_id, clock_, transport_,
          options.anti_entropy_buckets, &metrics_),
      signals_(io_),
      shard_(*store_, ring_, transport_, table_, options.node_id, clock_, options.replica_factor,
          options.quarantine_interval),
      replay_timer_(io_),
      gossip_timer_(io_),
      probe_timer_(io_),
      evict_timer_(io_),
      quarantine_timer_(io_),
      rebalance_timer_(io_),
      compact_timer_(io_),
      config_reload_timer_(io_),
      anti_entropy_timer_(io_),
      gossip_(clock_, transport_, table_, options.node_id, options.gossip_interval),
      table_(options.node_id),
      detector_(clock_, transport_, table_, options.node_id, options.suspect_timeout),
      persistence_(
          PersistenceManager::Options{
              .data_dir = options.data_dir,
              .enabled = options.persistence_enabled,
              .snapshot_interval_s = options.snapshot_interval_s,
              .max_wal_entries = options.max_wal_entries,
          },
          *store_, &clock_),
      transport_(io_, options.node_id, options.shared_secret
#ifdef CINDER_ENABLE_TLS
          ,
          ssl_ctx_ ? &*ssl_ctx_ : nullptr
#endif
          ),
      current_config_(options.config),
      server_(io_, options.port, *store_, ring_, options.node_id, clock_, &repl_,
          options.replica_factor, options.mode, &gossip_, options.metrics_port, &metrics_,
          [this]() { return formatConfigJson(current_config_); }, &anti_entropy_,
          options.shared_secret
#ifdef CINDER_ENABLE_TLS
          ,
          ssl_ctx_ ? &*ssl_ctx_ : nullptr
#endif
          ),
      repl_(*store_, options.node_id, clock_, transport_),
      io_threads_(options.io_threads),
      replica_factor_(options.replica_factor),
      port_(options.port),
      metrics_port_(options.metrics_port),
      mode_(options.mode) {
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

    syncEffectiveConfig();
    table_.onChange([this] { rebuildRing(); });
    if (persistence_.enabled()) {
        store_->setPersistence(&persistence_);
    }

    store_->setMetrics(&metrics_);
    repl_.setMetrics(&metrics_);
    detector_.setMetrics(&metrics_);
    gossip_.setMetrics(&metrics_);
    shard_.setMetrics(&metrics_);
    anti_entropy_.setMetrics(&metrics_);

    server_.setAdminCallbacks({
        .info_getter = [this]() -> std::string {
        return formatNodeInfoJson(node_id_,
            current_config_,
            metrics_.shardMetrics().live.current_bytes.load(),
            metrics_.shardMetrics().live.current_entries.load());
    },
        .cluster_getter = [this]() -> std::string { return formatClusterJson(table_.snapshot()); },
        .ring_getter = [this]() -> std::string { return formatRingJson(node_id_); },
        .compact_trigger =
            [this]() {
        if (persistence_.enabled()) {
            auto result = persistence_.compact();
            if (!result.has_value()) {
                Event::error("admin compact failed", {{"reason", result.error().message()}});
            }
        }
    },
        .config_reload_trigger = [this]() { applyConfig(); },
        .shutdown_trigger = [this]() { asio::post(io_, [this]() { shutdown(); }); },
    });

    // Redirect hints carry the owner's address (learned via gossip) so clients
    // that were never configured with the owner can still follow them.
    server_.setAddrResolver(
        [this](const NodeId& id) -> std::optional<std::pair<std::string, uint16_t>> {
        auto info = table_.get(id);
        if (!info.has_value() || info->host.empty() || info->port == 0) {
            return std::nullopt;
        }
        return std::make_pair(info->host, info->port);
    });
    if (options.listen_fd >= 0) {
        server_.setListenFd(options.listen_fd);
    }

    Event::info("eviction policy", {{"policy", options.eviction_policy}});
    Event::info("anti-entropy",
        {{"interval_ms", std::to_string(options.anti_entropy_interval.count())},
            {"buckets", std::to_string(options.anti_entropy_buckets)}});
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
            Event::error("persistence recovery failed", {{"reason", res.error().message()}});
            return;
        }
        Event::info("recovered entries from disk", {{"count", std::to_string(store_->size())}});
    }

    signals_.async_wait([this](std::error_code, int) {
        Event::info("shutting down...");
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
    if (anti_entropy_interval_.count() > 0) {
        scheduleAntiEntropy();
    }

    scheduleConfigReload();
    unsigned workers = io_threads_ > 0 ? static_cast<unsigned>(io_threads_) : 1;
    if (io_threads_ == 0) {
        auto hw = std::thread::hardware_concurrency();
        workers = hw == 0 ? 1 : std::min(4U, hw);
    }

    Event::info("node started", {{"io_threads", std::to_string(workers)}});
    if (metrics_port_ > 0) {
        Event::info("metrics endpoint", {{"port", std::to_string(metrics_port_)}});
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
    rebalance_timer_.cancel();
    compact_timer_.cancel();
    config_reload_timer_.cancel();
    anti_entropy_timer_.cancel();
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
                    Event::info("replayed hinted writes", {{"count", std::to_string(replayed)}});
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
    // Push keys this node no longer owns to their new ring owners. The scan
    // + migration burst is debounced (see scheduleRebalanceDebounced): rapid
    // membership flaps collapse into a single run over the latest ring.
    scheduleRebalanceDebounced();
}

void
CacheNodeServer::scheduleRebalanceDebounced() {
    // Coalesce bursts: a pending run already covers the latest ring view.
    if (rebalance_pending_) {
        return;
    }

    rebalance_pending_ = true;
    rebalance_timer_.expires_after(milliseconds(200));
    rebalance_timer_.async_wait([this](std::error_code ec) {
        rebalance_pending_ = false;
        if (ec) {
            return;
        }
        // If some migrations were deferred because their owner is still
        // quarantined, retry once the window has elapsed.
        if (shard_.rebalance()) {
            scheduleRebalance();
        }
    });
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
            Event::error("compact failed", {{"reason", result.error().message()}});
        }
        scheduleCompact();
    });
}

void
CacheNodeServer::scheduleAntiEntropy() {
    if (anti_entropy_interval_.count() <= 0) {
        return;
    }

    anti_entropy_timer_.expires_after(anti_entropy_interval_);
    anti_entropy_timer_.async_wait([this](std::error_code ec) {
        if (ec) {
            return;
        }
        // Skip background repair while the cluster view is degraded: with
        // fewer than a majority visible, partner selection may flap and
        // repair against stale membership.
        if (!table_.isDegraded()) {
            anti_entropy_.runRound(replica_factor_);
        }
        scheduleAntiEntropy();
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
    Span span("config.reload");
    if (config_path_.empty()) {
        return;
    }

    auto new_config = loadConfig(config_path_);
    if (!new_config.has_value()) {
        Event::error("config reload failed", {{"reason", new_config.error().message()}});
        return;
    }

    auto changed = diffConfig(current_config_, *new_config);
    if (changed.empty()) {
        return;
    }

    std::string changed_str;
    std::string identity_str;
    std::vector<std::string> hot;
    for (const auto& field : changed) {
        if (isIdentityField(field)) {
            if (!identity_str.empty()) {
                identity_str += ", ";
            }
            identity_str += field;
        } else {
            if (!changed_str.empty()) {
                changed_str += ", ";
            }
            changed_str += field;
            hot.push_back(field);
        }
    }
    if (!identity_str.empty()) {
        Event::debug("config reload ignores identity fields", {{"fields", identity_str}});
    }
    if (hot.empty()) {
        current_config_ = *new_config;
        syncEffectiveConfig();
        return;
    }

    Event::info("config changed", {{"fields", changed_str}});
    current_config_ = *new_config;
    syncEffectiveConfig();
    for (const auto& field : hot) {
        if (field == "log_level") {
            setLogLevel(logLevelFromString(new_config->log_level));
        } else if (field == "ping_interval_ms") {
            Event::warn("config field requires restart", {{"field", field}});
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
        } else if (field == "anti_entropy_interval_ms") {
            bool was_disabled = anti_entropy_interval_.count() <= 0;
            anti_entropy_interval_ = milliseconds(new_config->anti_entropy_interval_ms);
            if (was_disabled && anti_entropy_interval_.count() > 0) {
                scheduleAntiEntropy();
            }
        } else if (field == "anti_entropy_buckets") {
            Event::warn("config field requires restart", {{"field", field}});
        } else {
            Event::warn("config field requires restart", {{"field", field}});
        }
    }
}
} // namespace cinder
