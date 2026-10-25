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
        auto makeStore(const CacheNodeServerOptions& opts, Clock* clock)
            -> std::unique_ptr<CacheStore> {
            if (opts.eviction_policy == "lfu") {
                return std::make_unique<LfuStore>(opts.capacity, clock);
            }
            return std::make_unique<LruStore>(opts.capacity, clock);
        }

        auto isIdentityField(std::string_view field) -> bool {
            // Identity fields can never hot-apply and normally differ only because
            // CLI flags shadow the file — keep them out of the change report.
            return field
                == "node_id"
                || field
                == "port"
                || field
                == "peers"
                || field
                == "replica_factor"
                || field
                == "consistency";
        }

        struct ConfigChangeSet {
            std::vector<std::string> hot;
            std::string changed_str;
            std::string identity_str;
        };

        auto partitionConfigChanges(const std::vector<std::string>& changed) -> ConfigChangeSet {
            ConfigChangeSet out;
            for (const auto& field : changed) {
                if (isIdentityField(field)) {
                    if (!out.identity_str.empty()) {
                        out.identity_str += ", ";
                    }
                    out.identity_str += field;
                } else {
                    if (!out.changed_str.empty()) {
                        out.changed_str += ", ";
                    }

                    out.changed_str += field;
                    out.hot.push_back(field);
                }
            }
            return out;
        }
    } // namespace

    void CacheNodeServer::syncEffectiveConfig() {
        current_config_.node_id = node_id_;
        current_config_.port = port_;
        current_config_.capacity = capacity_;
        current_config_.peers = peers_;
        current_config_.replica_factor = replica_factor_;
        current_config_.consistency = (mode_ == ConsistencyMode::Quorum) ? "quorum" : "async";
    }

#ifdef CINDER_ENABLE_TLS
    namespace {
        auto initSslContext(const CacheNodeServerOptions& opts)
            -> std::optional<asio::ssl::context> {
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

    void CacheNodeServer::seedCluster(const CacheNodeServerOptions& options) {
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
        // Async migration failures keep the local key copy; retry the rebalance
        // after a short delay so a briefly-unreachable new owner can recover.
        shard_.setOnMigrationFailed([this] { scheduleMigrationRetry(); });
        if (persistence_.enabled()) {
            store_->setPersistence(&persistence_);
        }
    }

    void CacheNodeServer::wireMetrics() {
        store_->setMetrics(&metrics_);
        repl_.setMetrics(&metrics_);
        detector_.setMetrics(&metrics_);
        gossip_.setMetrics(&metrics_);
        shard_.setMetrics(&metrics_);
        anti_entropy_.setMetrics(&metrics_);
    }

    void CacheNodeServer::installAdminCallbacks() {
        server_.setAdminCallbacks({
            .info_getter = [this]() -> std::string {
            Config snapshot;
            {
                std::scoped_lock lock(config_mutex_);
                snapshot = current_config_;
            }

            return formatNodeInfoJson(
                node_id_,
                snapshot,
                metrics_.shardMetrics().live.current_bytes.load(),
                metrics_.shardMetrics().live.current_entries.load()
            );
        },
            .cluster_getter = [this]() -> std::string {
            return formatClusterJson(table_.snapshot());
        },
            .ring_getter = [this]() -> std::string { return formatRingJson(node_id_); },
            .compact_trigger =
                [this]() {
            if (persistence_.enabled()) {
                auto result = persistence_.compact();
                if (!result.has_value()) {
                    CINDER_ERROR("admin compact failed", {"reason", result.error().message()});
                }
            }
        },
            .config_reload_trigger = [this]() { applyConfig(); },
            .shutdown_trigger = [this]() { asio::post(io_, [this]() { shutdown(); }); },
        });
    }

    void CacheNodeServer::installAddrResolver() {
        // Redirect hints carry the owner's address (learned via gossip) so clients
        // that were never configured with the owner can still follow them.
        server_.setAddrResolver(
            [this](const NodeId& id) -> std::optional<std::pair<std::string, uint16_t>> {
            auto info = table_.get(id);
            if (!info.has_value() || info->host.empty() || info->port == 0) {
                return std::nullopt;
            }
            return std::make_pair(info->host, info->port);
        }
        );
    }

    CacheNodeServer::CacheNodeServer(CacheNodeServerOptions options) :
        ping_interval_(options.ping_interval),
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
        signals_(io_),
        timers_(io_),
        table_(options.node_id),
        transport_(
            io_,
            options.node_id,
            options.shared_secret
#ifdef CINDER_ENABLE_TLS
            ,
            ssl_ctx_ ? &*ssl_ctx_ : nullptr
#endif
        ),
        gossip_(clock_, transport_, table_, options.node_id, options.gossip_interval),
        detector_(clock_, transport_, table_, options.node_id, options.suspect_timeout),
        repl_(*store_, options.node_id, clock_, transport_),
        shard_(
            *store_,
            ring_,
            transport_,
            table_,
            options.node_id,
            clock_,
            options.replica_factor,
            options.quarantine_interval
        ),
        anti_entropy_(
            *store_,
            ring_,
            options.node_id,
            clock_,
            transport_,
            options.anti_entropy_buckets,
            &metrics_
        ),
        persistence_(
            PersistenceManager::Options{
                .data_dir = options.data_dir,
                .enabled = options.persistence_enabled,
                .snapshot_interval_s = options.snapshot_interval_s,
                .max_wal_entries = options.max_wal_entries,
            },
            *store_,
            &clock_
        ),
        current_config_(options.config),
        server_(
            io_,
            options.port,
            *store_,
            ring_,
            options.node_id,
            clock_,
            &repl_,
            options.replica_factor,
            options.mode,
            &gossip_,
            options.metrics_port,
            &metrics_,
            [this]() {
                Config snapshot;
                {
                    std::scoped_lock lock(config_mutex_);
                    snapshot = current_config_;
                }
                return formatConfigJson(snapshot);
            },
            &anti_entropy_,
            options.shared_secret
#ifdef CINDER_ENABLE_TLS
            ,
            ssl_ctx_ ? &*ssl_ctx_ : nullptr
#endif
        ),
        io_threads_(options.io_threads),
        replica_factor_(options.replica_factor),
        port_(options.port),
        metrics_port_(options.metrics_port),
        mode_(options.mode) {
        seedCluster(options);
        wireMetrics();
        installAdminCallbacks();
        installAddrResolver();
        if (options.listen_fd >= 0) {
            server_.setListenFd(options.listen_fd);
        }

        CINDER_INFO("eviction policy", {"policy", options.eviction_policy});
        CINDER_INFO(
            "anti-entropy",
            {"interval_ms", options.anti_entropy_interval.count()},
            {"buckets", options.anti_entropy_buckets}
        );
    }

    auto CacheNodeServer::start() -> Result<void> {
        return server_.start();
    }

    void CacheNodeServer::run() {
        // Recover from disk before serving
        if (persistence_.enabled()) {
            auto res = persistence_.recover();
            if (!res.has_value()) {
                CINDER_ERROR("persistence recovery failed", {"reason", res.error().message()});
                return;
            }
            CINDER_INFO("recovered entries from disk", {"count", store_->size()});
        }

        signals_.async_wait([this](std::error_code, int) {
            CINDER_INFO("shutting down...");
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
        if (anti_entropy_interval_.load().count() > 0) {
            scheduleAntiEntropy();
        }

        scheduleConfigReload();
        unsigned workers = io_threads_ > 0 ? static_cast<unsigned>(io_threads_) : 1;
        if (io_threads_ == 0) {
            auto hw = std::thread::hardware_concurrency();
            workers = hw == 0 ? 1 : std::min(4U, hw);
        }

        CINDER_INFO("node started", {"io_threads", workers});
        if (metrics_port_ > 0) {
            CINDER_INFO("metrics endpoint", {"port", metrics_port_});
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

    void CacheNodeServer::shutdown() {
        timers_.cancelAll();
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

    void CacheNodeServer::scheduleReplay() {
        timers_.replay.expires_after(seconds(1));
        timers_.replay.async_wait([this](std::error_code ec) {
            if (ec) {
                return;
            }
            if (repl_.hintCount() > 0) {
                repl_.replayHints([](size_t replayed) {
                    if (replayed > 0) {
                        CINDER_INFO("replayed hinted writes", {"count", replayed});
                    }
                });
            }
            scheduleReplay();
        });
    }

    void CacheNodeServer::scheduleGossip() {
        timers_.gossip.expires_after(gossip_.gossipInterval());
        timers_.gossip.async_wait([this](std::error_code ec) {
            if (ec) {
                return;
            }

            gossip_.tick();
            scheduleGossip();
        });
    }

    void CacheNodeServer::scheduleProbe() {
        timers_.probe.expires_after(ping_interval_);
        timers_.probe.async_wait([this](std::error_code ec) {
            if (ec) {
                return;
            }

            detector_.tick();
            scheduleProbe();
        });
    }

    void CacheNodeServer::scheduleEvict() {
        timers_.evict.expires_after(seconds(1));
        timers_.evict.async_wait([this](std::error_code ec) {
            if (ec) {
                return;
            }

            store_->evictExpired();
            scheduleEvict();
        });
    }

    void CacheNodeServer::rebuildRing() {
        size_t alive_count = 0;
        size_t removed_count = 0;
        for (const auto& info : table_.snapshot()) {
            if (info.id == node_id_) {
                continue;
            }

            transport_.addAddr(info.id, info.host, info.port);
            if (info.state == NodeState::Alive) {
                ring_.addNode(info.id);
                ++alive_count;
            } else {
                ring_.removeNode(info.id);
                ++removed_count;
            }
        }
        CINDER_INFO("ring rebuilt", {"alive", alive_count}, {"removed", removed_count});
        // Push keys this node no longer owns to their new ring owners. The scan
        // + migration burst is debounced (see scheduleRebalanceDebounced): rapid
        // membership flaps collapse into a single run over the latest ring.
        scheduleRebalanceDebounced();
    }

    void CacheNodeServer::scheduleRebalanceDebounced() {
        // Coalesce bursts: a pending run already covers the latest ring view.
        if (timers_.rebalance_pending) {
            CINDER_DEBUG("rebalance debounce coalesced");
            return;
        }

        CINDER_DEBUG("rebalance debounce scheduled", {"delay_ms", 200});
        timers_.rebalance_pending = true;
        timers_.rebalance.expires_after(milliseconds(200));
        timers_.rebalance.async_wait([this](std::error_code ec) {
            timers_.rebalance_pending = false;
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

    void CacheNodeServer::scheduleRebalance() {
        if (quarantine_interval_.load().count() <= 0) {
            return;
        }

        timers_.quarantine.expires_after(quarantine_interval_.load());
        timers_.quarantine.async_wait([this](std::error_code ec) {
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

    void CacheNodeServer::scheduleMigrationRetry() {
        // A debounced rebalance is already queued and covers the latest ring view.
        if (timers_.migration_retry_pending || timers_.rebalance_pending) {
            CINDER_DEBUG(
                "migration retry coalesced",
                {"retry_pending", timers_.migration_retry_pending},
                {"rebalance_pending", timers_.rebalance_pending}
            );
            return;
        }

        CINDER_INFO("migration retry scheduled", {"delay_ms", 2000});
        timers_.migration_retry_pending = true;
        metrics_.clusterMetrics().rebalance_migration_retries.fetch_add(
            1,
            std::memory_order_relaxed
        );

        timers_.migration_retry.expires_after(milliseconds(2000));
        timers_.migration_retry.async_wait([this](std::error_code ec) {
            timers_.migration_retry_pending = false;
            if (ec) {
                return;
            }

            CINDER_INFO("migration retry fired");
            // Re-run through the debounce gate so rapid membership changes that
            // arrived while waiting collapse into a single scan.
            scheduleRebalanceDebounced();
        });
    }

    void CacheNodeServer::scheduleCompact() {
        timers_.compact.expires_after(seconds(persistence_.snapshotInterval()));
        timers_.compact.async_wait([this](std::error_code ec) {
            if (ec) {
                return;
            }
            if (auto result = persistence_.compact(); !result.has_value()) {
                CINDER_ERROR("compact failed", {"reason", result.error().message()});
            }
            scheduleCompact();
        });
    }

    void CacheNodeServer::scheduleAntiEntropy() {
        if (anti_entropy_interval_.load().count() <= 0) {
            return;
        }

        timers_.anti_entropy.expires_after(anti_entropy_interval_.load());
        timers_.anti_entropy.async_wait([this](std::error_code ec) {
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

    void CacheNodeServer::scheduleConfigReload() {
        timers_.config_reload.expires_after(seconds(5));
        timers_.config_reload.async_wait([this](std::error_code ec) {
            if (ec) {
                return;
            }

            applyConfig();
            scheduleConfigReload();
        });
    }

    void CacheNodeServer::applyHotField(std::string_view field, const Config& cfg) {
        if (field == "log_level") {
            setLogLevel(logLevelFromString(cfg.log_level));
        } else if (field == "suspect_timeout_ms") {
            detector_.setSuspectTimeout(milliseconds(cfg.suspect_timeout_ms));
        } else if (field == "gossip_interval_ms") {
            gossip_.setGossipInterval(milliseconds(cfg.gossip_interval_ms));
        } else if (field == "rpc_timeout_ms") {
            transport_.setRpcTimeout(milliseconds(cfg.rpc_timeout_ms));
        } else if (field == "capacity") {
            store_->setCapacity(cfg.capacity);
        } else if (field == "quarantine_interval_ms") {
            quarantine_interval_ = milliseconds(cfg.quarantine_interval_ms);
        } else if (field == "anti_entropy_interval_ms") {
            bool was_disabled = anti_entropy_interval_.load().count() <= 0;
            anti_entropy_interval_ = milliseconds(cfg.anti_entropy_interval_ms);
            if (was_disabled && anti_entropy_interval_.load().count() > 0) {
                scheduleAntiEntropy();
            }
        } else {
            // ping_interval_ms, anti_entropy_buckets, and unknown fields need restart.
            CINDER_WARN("config field requires restart", {"field", field});
        }
    }

    void CacheNodeServer::applyConfig() {
        Span span("config.reload");
        if (config_path_.empty()) {
            return;
        }

        auto new_config = loadConfig(config_path_);
        if (!new_config.has_value()) {
            CINDER_ERROR("config reload failed", {"reason", new_config.error().message()});
            return;
        }

        // Serialize whole reloads against each other and against admin readers:
        // concurrent applyConfig runs (timer + admin trigger) would otherwise
        // race on current_config_, and readers would race the writers.
        std::scoped_lock lock(config_mutex_);
        auto changed = diffConfig(current_config_, *new_config);
        if (changed.empty()) {
            return;
        }

        auto split = partitionConfigChanges(changed);
        if (!split.identity_str.empty()) {
            CINDER_DEBUG("config reload ignores identity fields", {"fields", split.identity_str});
        }
        if (split.hot.empty()) {
            current_config_ = *new_config;
            syncEffectiveConfig();
            return;
        }

        CINDER_INFO("config changed", {"fields", split.changed_str});
        current_config_ = *new_config;
        syncEffectiveConfig();
        for (const auto& field : split.hot) {
            applyHotField(field, *new_config);
        }
    }
} // namespace cinder
