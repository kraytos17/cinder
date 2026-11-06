#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "cinder/common/status.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/node/replication_manager.hpp"
#include "cinder/store/cache_store.hpp"

namespace cinder {

    // Transport-independent cache operations for node-facing adapters.
    //
    // Interface contracts for asynchronous operations:
    // - Reads the local store for RF=1; for RF>1, asks the ReplicationManager
    //   to collect the configured read quorum and perform read repair.
    // - A winning tombstone is exposed as NotFound (never as a value).
    // - Writes and reads use the same replica plan for a key. Writes apply
    //   locally for RF=1 and delegate versioning, TTL conversion, and replica
    //   completion to ReplicationManager for RF>1.
    // - Completion fires once. It may run inline or on ReplicationManager's
    //   completion executor; adapters marshal results to any thread-affine
    //   response state themselves.
    // - Ownership redirects remain adapter policy. A miss here means NotFound;
    //   TCP may add its existing owner-miss redirect behavior at its seam.
    class CacheOperations {
    public:

        using GetResult = Result<std::string>;
        using GetCallback = std::move_only_function<void(GetResult)>;
        using WriteCallback = std::move_only_function<void(Result<void>)>;

        CacheOperations(
            CacheStore& store,
            const ConsistentHashRing& ring,
            const NodeId& node_id,
            int replica_factor,
            ReplicationManager* replication = nullptr,
            ConsistencyMode mode = ConsistencyMode::Async
        ) :
            store_(store),
            ring_(ring),
            node_id_(node_id),
            replica_factor_(replica_factor),
            replication_(replication),
            mode_(mode) {}

        CacheOperations(const CacheOperations&) = delete;
        auto operator=(const CacheOperations&) -> CacheOperations& = delete;

        void setAsync(
            const std::string& key,
            std::string value,
            std::optional<milliseconds> ttl,
            WriteCallback on_done,
            uint64_t trace_id = 0,
            uint64_t span_id = 0
        ) const {
            if (replica_factor_ <= 1 || replication_ == nullptr) {
                on_done(store_.put(key, std::move(value), ttl));
                return;
            }
            replication_->writeAsync(
                key,
                std::move(value),
                ttl,
                replicaNodesFor(key),
                mode_,
                std::move(on_done),
                trace_id,
                span_id
            );
        }

        void deleteAsync(
            const std::string& key,
            WriteCallback on_done,
            uint64_t trace_id = 0,
            uint64_t span_id = 0
        ) const {
            if (replica_factor_ <= 1 || replication_ == nullptr) {
                on_done(store_.removeVersioned(key, store_.mintVersion(), 0));
                return;
            }
            replication_->deleteAsync(
                key,
                replicaNodesFor(key),
                mode_,
                std::move(on_done),
                trace_id,
                span_id
            );
        }

        void getAsync(
            const std::string& key,
            GetCallback on_done,
            uint64_t trace_id = 0,
            uint64_t span_id = 0
        ) const {
            if (replica_factor_ <= 1 || replication_ == nullptr) {
                auto value = store_.get(key);
                if (value) {
                    on_done(ok(std::move(*value)));
                } else {
                    on_done(err<std::string>(Error(Errc::NotFound, "key not found")));
                }
                return;
            }
            replication_->readAsync(
                key,
                replicaNodesFor(key),
                static_cast<size_t>(replica_factor_),
                [done = std::move(on_done)](Result<VersionedEntry> result) mutable {
                if (!result) {
                    done(err<std::string>(result.error()));
                } else if (result->isTombstone()) {
                    done(err<std::string>(Error(Errc::NotFound, "key not found")));
                } else {
                    done(ok(std::move(result->value)));
                }
            },
                trace_id,
                span_id
            );
        }

    private:

        [[nodiscard]]
        auto replicaNodesFor(const std::string& key) const -> std::vector<NodeId> {
            const auto nodes = ring_.getNodes(key, replica_factor_);
            std::vector<NodeId> replicas;
            replicas.reserve(nodes.size());
            for (const auto& node : nodes) {
                if (node != node_id_) {
                    replicas.push_back(node);
                }
            }
            return replicas;
        }

        CacheStore& store_;
        const ConsistentHashRing& ring_;
        NodeId node_id_;
        int replica_factor_;
        ReplicationManager* replication_;
        ConsistencyMode mode_;
    };
} // namespace cinder
