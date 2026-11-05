#pragma once

#include <functional>
#include <string>
#include <vector>

#include "cinder/common/status.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/node/replication_manager.hpp"
#include "cinder/store/cache_store.hpp"

namespace cinder {

    // Transport-independent cache operations for node-facing adapters.
    //
    // Interface contract for getAsync:
    // - Reads the local store for RF=1; for RF>1, asks the ReplicationManager
    //   to collect the configured read quorum and perform read repair.
    // - A winning tombstone is exposed as NotFound (never as a value).
    // - Callback fires exactly once under ReplicationManager's callback
    //   contract: inline for a synchronous quorum, otherwise on its completion
    //   executor. Adapters must marshal the result to their own executor if
    //   their response state is thread-affine.
    // - Ownership redirects remain adapter policy. A miss here means NotFound;
    //   TCP may add its existing owner-miss redirect behavior at its seam.
    class CacheOperations {
    public:

        using GetResult = Result<std::string>;
        using GetCallback = std::move_only_function<void(GetResult)>;

        CacheOperations(
            CacheStore& store,
            const ConsistentHashRing& ring,
            const NodeId& node_id,
            int replica_factor,
            ReplicationManager* replication = nullptr
        ) :
            store_(store),
            ring_(ring),
            node_id_(node_id),
            replica_factor_(replica_factor),
            replication_(replication) {}

        CacheOperations(const CacheOperations&) = delete;
        auto operator=(const CacheOperations&) -> CacheOperations& = delete;

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

            const auto nodes = ring_.getNodes(key, replica_factor_);
            std::vector<NodeId> replicas;
            replicas.reserve(nodes.size());
            for (const auto& node : nodes) {
                if (node != node_id_) {
                    replicas.push_back(node);
                }
            }

            replication_->readAsync(
                key,
                replicas,
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

        CacheStore& store_;
        const ConsistentHashRing& ring_;
        NodeId node_id_;
        int replica_factor_;
        ReplicationManager* replication_;
    };

} // namespace cinder
