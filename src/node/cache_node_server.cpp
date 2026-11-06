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
            std::unique_ptr<CacheStore> store;
            if (opts.eviction_policy == "lfu") {
                store = std::make_unique<LfuStore>(opts.capacity, clock);
            } else {
                store = std::make_unique<LruStore>(opts.capacity, clock);
            }

            store->setTombstoneGcInterval(opts.tombstone_gc_interval);
            return store;
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
        table_.seed(options.peers);
        table_.setSelfAddress("127.0.0.1", options.port);

        ClusterConfig config;
        config.nodes.push_back({options.node_id, "127.0.0.1", options.port});
        std::vector<NodeId> ring_nodes{options.node_id};
        ring_nodes.reserve(options.peers.size() + 1);
        for (const auto& peer : options.peers) {
            ring_nodes.push_back(peer.id);
            config.nodes.push_back(peer);
        }

        ring_.setNodes(ring_nodes);
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
            .info_getter = [this] -> std::string {
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
            .cluster_getter = [this] -> std::string {
            return formatClusterJson(table_.snapshot());
        },
            .ring_getter = [this] -> std::string { return formatRingJson(node_id_); },
            .compact_trigger =
                [this] {
            if (persistence_.enabled()) {
                auto result = persistence_.compact();
                if (!result.has_value()) {
                    CINDER_ERROR("admin compact failed", {"reason", result.error().message()});
                }
            }
        },
            .config_reload_trigger = [this] { applyConfig(); },
            .shutdown_trigger = [this] { asio::post(io_, [this] { shutdown(); }); },
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
        anti_entropy_interval_(options.anti_entropy.interval),
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
        operations_(*store_, ring_, node_id_, options.replica_factor, &repl_, options.mode),
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
            options.anti_entropy.buckets,
            &metrics_,
            options.anti_entropy.fetch_bytes,
            options.anti_entropy.full_rehash_interval
        ),
        persistence_(
            PersistenceManager::Options{
                .data_dir = options.data_dir,
                .enabled = options.persistence_enabled,
                .tuning = options.persistence,
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
            [this] {
                Config snapshot;
                {
                    std::scoped_lock lock(config_mutex_);
                    snapshot = current_config_;
                }
                return formatConfigJson(snapshot);
            },
            &anti_entropy_,
            options.shared_secret,
            options.client_shared_secret
#ifdef CINDER_ENABLE_TLS
            ,
            ssl_ctx_ ? &*ssl_ctx_ : nullptr
#endif
        ),
        io_threads_(options.io_threads),
        replica_factor_(options.replica_factor),
        port_(options.port),
        metrics_port_(options.metrics_port),
        mode_(options.mode),
        client_shared_secret_(options.client_shared_secret),
        tls_enabled_(options.tls_enabled) {
        server_.setCacheOperations(operations_);
        seedCluster(options);
        wireMetrics();
        installAdminCallbacks();
        installAddrResolver();
        if (options.listen_fd >= 0) {
            server_.setListenFd(options.listen_fd);
        }
        if (options.metrics_listen_fd >= 0) {
            server_.setMetricsListenFd(options.metrics_listen_fd);
        }
        if (options.persistence_enabled && !options.data_dir.empty()) {
            repl_.setHintJournal(options.data_dir + "/hints.journal");
        }

        CINDER_INFO("eviction policy", {"policy", options.eviction_policy});
        if (options.max_hint_bytes > 0) {
            repl_.setMaxHintBytes(options.max_hint_bytes);
            CINDER_INFO("max hint bytes", {"bytes", options.max_hint_bytes});
        }
        CINDER_INFO(
            "anti-entropy",
            {"interval_ms", options.anti_entropy.interval.count()},
            {"buckets", options.anti_entropy.buckets},
            {"fetch_bytes", options.anti_entropy.fetch_bytes},
            {"full_rehash_interval_ms", options.anti_entropy.full_rehash_interval.count()}
        );
    }

    auto CacheNodeServer::start() -> Result<void> {
        if (!client_shared_secret_.empty()) {
#ifndef CINDER_ENABLE_CRYPTO
            return err(Error(Errc::InvalidArgument, "client auth requires a crypto-enabled build"));
#else
            if (!tls_enabled_) {
                return err(Error(
                    Errc::InvalidArgument,
                    "client auth requires TLS (replayable bearer token)"
                ));
            }
#ifdef CINDER_ENABLE_TLS
            if (!ssl_ctx_.has_value()) {
                return err(
                    Error(Errc::InvalidArgument, "client auth requires TLS certificate and key")
                );
            }
#endif
#endif
        }
        return server_.start();
    }

    void CacheNodeServer::run() {
        // Startup root: fresh trace for the recovery sequence below (timer
        // callbacks later start their own runs; nothing here has an inbound
        // parent). Braced to close before io_.run(): a function-scope span
        // would linger its thread-local context on the runner thread for the
        // process lifetime and background callbacks would join it.
        {
            Span startup_span("node.start", SpanContext{});
            // Recover durable hints before serving: replayed once the (possibly
            // returned) replica is reachable again.
            if (auto recovered = repl_.recoverHints(); recovered > 0) {
                CINDER_INFO("recovered hints", {"count", recovered});
            }
            // Recover from disk before serving
            if (persistence_.enabled()) {
                auto res = persistence_.recover();
                if (!res.has_value()) {
                    CINDER_ERROR("persistence recovery failed", {"reason", res.error().message()});
                    startup_span.setStatus(SpanStatus::Error);
                    return;
                }
                CINDER_INFO("recovered entries from disk", {"count", store_->size()});
            }
            startup_span.setStatus(SpanStatus::Ok);
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
                pool.emplace_back([this] { io_.run(); });
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
        // Shutdown root: fresh trace for the drain sequence below.
        Span span("node.shutdown", SpanContext{});
        timers_.cancelAll();
        repl_.syncHints();
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
        span.setStatus(SpanStatus::Ok);
    }

    auto CacheNodeServer::nextTimerRun() -> uint64_t {
        return timer_run_.fetch_add(1, std::memory_order_relaxed) + 1;
    }

    void CacheNodeServer::armTimer(
        steady_timer& timer,
        steady_timer::duration interval,
        std::string_view span_name,
        std::function<SpanStatus(Span&)> body
    ) {
        timer.expires_after(interval);
        timer.async_wait([this,
                          &timer,
                          interval,
                          name = std::string(span_name),
                          body = std::move(body)](std::error_code ec) mutable {
            if (ec) {
                return;
            }

            // Run root (see scheduleEvict): the timer fires on an io thread
            // with unrelated TLS, so mint a fresh trace for this pass.
            Span span(name, SpanContext{});
            span.setAttribute({"run", nextTimerRun()});
            span.setStatus(body(span));
            armTimer(timer, interval, name, std::move(body));
        });
    }

    void CacheNodeServer::scheduleReplay() {
        armTimer(timers_.replay, seconds(1), "timer.replay", [this](Span&) {
            if (repl_.hintCount() > 0) {
                repl_.replayHints([](size_t replayed) {
                    if (replayed > 0) {
                        CINDER_INFO("replayed hinted writes", {"count", replayed});
                    }
                });
            }
            return SpanStatus::Ok;
        });
    }

    void CacheNodeServer::scheduleGossip() {
        armTimer(timers_.gossip, gossip_.gossipInterval(), "timer.gossip", [this](Span&) {
            gossip_.tick();
            return SpanStatus::Ok;
        });
    }

    void CacheNodeServer::scheduleProbe() {
        armTimer(timers_.probe, ping_interval_, "timer.probe", [this](Span&) {
            detector_.tick();
            return SpanStatus::Ok;
        });
    }

    void CacheNodeServer::scheduleEvict() {
        armTimer(timers_.evict, seconds(1), "timer.evict", [this](Span&) {
            store_->evictExpired();
            // Steady-state WAL drain (batched, ~1s cadence): bounds the
            // pending queue (and the crash-loss window) between periodic
            // compactions. The drain also advances the entry counter behind
            // the max_wal_entries auto-compact threshold.
            if (persistence_.enabled()) {
                persistence_.flush();
                if (persistence_.walNeedsCompact()) {
                    scheduleCompactDebounced();
                }
            }
            return SpanStatus::Ok;
        });
    }

    void CacheNodeServer::rebuildRing() {
        // Child of the triggering mutation (gossip apply, suspect/dead mark):
        // fireCallbacks runs synchronously on the mutator's thread, so the
        // ambient context is the cause. Falls back to a fresh trace when
        // invoked without one.
        Span span("cluster.rebuild_ring", captureTraceContext());
        size_t alive_count = 0;
        size_t removed_count = 0;
        const auto members = table_.snapshot();

        std::vector<NodeId> ring_nodes;
        ring_nodes.reserve(members.size() + 1);
        ring_nodes.push_back(node_id_);
        for (const auto& info : members) {
            if (info.id == node_id_) {
                continue;
            }

            transport_.addAddr(info.id, info.host, info.port);
            if (info.state == NodeState::Alive) {
                ring_nodes.push_back(info.id);
                ++alive_count;
            } else {
                ++removed_count;
            }
        }

        ring_.setNodes(ring_nodes);
        CINDER_INFO("ring rebuilt", {"alive", alive_count}, {"removed", removed_count});
        span.setStatus(SpanStatus::Ok);
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

            Span span("timer.rebalance", SpanContext{});
            span.setAttribute({"run", nextTimerRun()});
            // If some migrations were deferred because their owner is still
            // quarantined, retry once the window has elapsed.
            if (shard_.rebalance()) {
                scheduleRebalance();
            }
            span.setStatus(SpanStatus::Ok);
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

            Span span("timer.quarantine", SpanContext{});
            span.setAttribute({"run", nextTimerRun()});
            // Keep retrying while any owner is still inside its quarantine window
            // (e.g. a flapping node keeps resetting joined_at).
            if (shard_.rebalance()) {
                scheduleRebalance();
            }
            span.setStatus(SpanStatus::Ok);
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

            Span span("timer.migration_retry", SpanContext{});
            span.setAttribute({"run", nextTimerRun()});
            CINDER_INFO("migration retry fired");
            // Re-run through the debounce gate so rapid membership changes that
            // arrived while waiting collapse into a single scan.
            scheduleRebalanceDebounced();
            span.setStatus(SpanStatus::Ok);
        });
    }

    void CacheNodeServer::scheduleCompact() {
        armTimer(
            timers_.compact,
            seconds(persistence_.snapshotInterval()),
            "timer.compact",
            [this](Span&) {
            if (auto result = persistence_.compact(); !result.has_value()) {
                CINDER_ERROR("compact failed", {"reason", result.error().message()});
                return SpanStatus::Error;
            }
            return SpanStatus::Ok;
        }
        );
    }

    void CacheNodeServer::scheduleCompactDebounced() {
        // Coalesce bursts: at most one out-of-cycle compact queued. The flag
        // clears when it runs. A periodic compact racing it is serialized by
        // the persistence mutex — a redundant back-to-back snapshot is wasted
        // work, never incorrect.
        if (timers_.compact_pending) {
            return;
        }

        timers_.compact_pending = true;
        asio::post(io_, [this] {
            timers_.compact_pending = false;
            Span span("timer.compact", SpanContext{});
            span.setAttribute({"run", nextTimerRun()});
            if (auto result = persistence_.compact(); !result.has_value()) {
                CINDER_ERROR("compact failed", {"reason", result.error().message()});
                span.setStatus(SpanStatus::Error);
            } else {
                span.setStatus(SpanStatus::Ok);
            }
        });
    }

    void CacheNodeServer::scheduleAntiEntropy() {
        if (anti_entropy_interval_.load().count() <= 0) {
            return;
        }

        armTimer(
            timers_.anti_entropy,
            anti_entropy_interval_.load(),
            "timer.anti_entropy",
            [this](Span& span) {
            // Skip background repair while the cluster view is degraded: with
            // fewer than a majority visible, partner selection may flap and
            // repair against stale membership.
            if (!table_.isDegraded()) {
                anti_entropy_.runRound(replica_factor_);
            } else {
                span.setAttribute({"outcome", "skipped"});
                span.setAttribute({"reason", "degraded-view"});
            }
            return SpanStatus::Ok;
        }
        );
    }

    void CacheNodeServer::scheduleConfigReload() {
        armTimer(timers_.config_reload, seconds(5), "timer.config_reload", [this](Span&) {
            applyConfig();
            return SpanStatus::Ok;
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
            capacity_ = cfg.capacity;
            store_->setCapacity(cfg.capacity);
            // syncEffectiveConfig() (called before this loop) restores
            // current_config_.capacity from the stale capacity_ member, so
            // re-assert the just-loaded value while still under config_mutex_.
            current_config_.capacity = cfg.capacity;
        } else if (field == "quarantine_interval_ms") {
            quarantine_interval_ = milliseconds(cfg.quarantine_interval_ms);
        } else if (field == "max_hint_bytes") {
            repl_.setMaxHintBytes(cfg.max_hint_bytes);
        } else if (field == "tombstone_gc_ms") {
            store_->setTombstoneGcInterval(milliseconds(cfg.tombstone_gc_ms));
        } else if (field == "anti_entropy_interval_ms") {
            bool was_disabled = anti_entropy_interval_.load().count() <= 0;
            anti_entropy_interval_ = cfg.anti_entropy.interval;
            if (was_disabled && anti_entropy_interval_.load().count() > 0) {
                scheduleAntiEntropy();
            }
        } else if (field == "anti_entropy_fetch_bytes") {
            anti_entropy_.setFetchBytes(cfg.anti_entropy.fetch_bytes);
        } else if (field == "anti_entropy_full_rehash_interval_ms") {
            anti_entropy_.setFullRehashInterval(cfg.anti_entropy.full_rehash_interval);
        } else {
            // ping_interval_ms, anti_entropy_buckets, and unknown fields need restart.
            CINDER_WARN("config field requires restart", {"field", field});
        }
    }

    void CacheNodeServer::applyConfig() {
        Span span("config.reload", captureTraceContext());
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
