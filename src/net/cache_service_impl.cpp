#ifdef CINDER_ENABLE_GRPC

#include <future>
#include <optional>
#include <string>

#include "cinder/common/config.hpp"
#include "cinder/common/tracing.hpp"
#include "cinder/net/grpc_gateway.hpp"
#include "cinder/v1/cache.grpc.pb.h"

using std::chrono::milliseconds;

namespace cinder::grpc {
namespace {

auto
toProtoStatus(Errc code) -> cinder::v1::StatusCode {
    switch (code) {
        case Errc::OK:
            return cinder::v1::STATUS_CODE_UNSPECIFIED;
        case Errc::NotFound:
            return cinder::v1::STATUS_CODE_NOT_FOUND;
        case Errc::CapacityExceeded:
            return cinder::v1::STATUS_CODE_CAPACITY_EXCEEDED;
        case Errc::InvalidArgument:
            return cinder::v1::STATUS_CODE_INVALID_ARGUMENT;
        case Errc::TtlExpired:
            return cinder::v1::STATUS_CODE_TTL_EXPIRED;
        case Errc::NotSupported:
            return cinder::v1::STATUS_CODE_NOT_SUPPORTED;
        case Errc::Internal:
            return cinder::v1::STATUS_CODE_INTERNAL_ERROR;
        case Errc::Timeout:
            return cinder::v1::STATUS_CODE_TIMEOUT;
        case Errc::NotReady:
            return cinder::v1::STATUS_CODE_NOT_READY;
    }
    return cinder::v1::STATUS_CODE_INTERNAL_ERROR;
}

// Implements the CinderCacheService gRPC service, mirroring the dispatch
// logic in TcpConnection::handleRequest.
class CinderCacheServiceImpl final : public cinder::v1::CinderCacheService::Service {
  public:

    explicit CinderCacheServiceImpl(CacheNodeServer::GatewayHandle handle)
        : handle_(std::move(handle)) {}

    static auto makeTraceId() -> std::pair<uint64_t, uint64_t> {
        static std::atomic<uint64_t> next_id{1};
        static std::atomic<uint64_t> next_span{1};
        return {next_id.fetch_add(1, std::memory_order_relaxed),
            next_span.fetch_add(1, std::memory_order_relaxed)};
    }

    ::grpc::Status Get(::grpc::ServerContext* /*ctx*/, const cinder::v1::GetRequest* req,
        cinder::v1::GetResponse* resp) override {
        auto [trace_id, span_id] = makeTraceId();
        cinder::Span span("grpc.get", span_id);
        if (handle_.replica_factor > 1) {
            auto nodes = handle_.ring.getNodes(req->key(), handle_.replica_factor);
            std::vector<NodeId> replicas;
            for (const auto& n : nodes) {
                if (n != handle_.node_id) {
                    replicas.push_back(n);
                }
            }

            std::promise<Result<VersionedEntry>> promise;
            auto future = promise.get_future();
            handle_.repl.readAsync(req->key(),
                replicas,
                static_cast<size_t>(handle_.replica_factor),
                [&promise](Result<VersionedEntry> result) { promise.set_value(std::move(result)); },
                span.traceId(),
                span.spanId());

            auto result = future.get();
            if (result.has_value()) {
                resp->set_status(cinder::v1::STATUS_CODE_UNSPECIFIED);
                resp->set_value(result->value);
            } else {
                resp->set_status(toProtoStatus(result.error().code()));
            }
            return grpc::Status::OK;
        }

        auto val = handle_.store.get(req->key());
        if (val.has_value()) {
            resp->set_status(cinder::v1::STATUS_CODE_UNSPECIFIED);
            resp->set_value(*val);
        } else {
            resp->set_status(cinder::v1::STATUS_CODE_NOT_FOUND);
        }
        return grpc::Status::OK;
    }

    ::grpc::Status Set(::grpc::ServerContext* /*ctx*/, const cinder::v1::SetRequest* req,
        cinder::v1::SetResponse* resp) override {
        auto [trace_id, span_id] = makeTraceId();
        cinder::Span span("grpc.set", span_id);
        auto owner = handle_.ring.getNode(req->key());
        if (owner != handle_.node_id) {
            resp->set_status(cinder::v1::STATUS_CODE_NOT_READY);
            return grpc::Status::OK;
        }

        std::optional<milliseconds> ttl;
        if (req->ttl_ms() > 0) {
            ttl = milliseconds(req->ttl_ms());
        }
        if (handle_.replica_factor > 1) {
            auto nodes = handle_.ring.getNodes(req->key(), handle_.replica_factor);
            std::vector<NodeId> replicas;
            for (const auto& n : nodes) {
                if (n != handle_.node_id) {
                    replicas.push_back(n);
                }
            }

            std::promise<Result<void>> promise;
            auto future = promise.get_future();
            handle_.repl.writeAsync(req->key(),
                req->value(),
                ttl,
                replicas,
                handle_.mode,
                [&promise](Result<void> result) { promise.set_value(std::move(result)); },
                span.traceId(),
                span.spanId());

            auto result = future.get();
            resp->set_status(result.has_value() ? cinder::v1::STATUS_CODE_UNSPECIFIED
                                                : toProtoStatus(result.error().code()));
        } else {
            auto result = handle_.store.put(req->key(), req->value(), ttl);
            resp->set_status(result.has_value() ? cinder::v1::STATUS_CODE_UNSPECIFIED
                                                : toProtoStatus(result.error().code()));
        }
        return grpc::Status::OK;
    }

    ::grpc::Status Delete(::grpc::ServerContext* /*ctx*/, const cinder::v1::DeleteRequest* req,
        cinder::v1::DeleteResponse* resp) override {
        cinder::Span span("grpc.delete");
        handle_.store.remove(req->key());
        resp->set_status(cinder::v1::STATUS_CODE_UNSPECIFIED);
        return grpc::Status::OK;
    }

    ::grpc::Status GetVersioned(::grpc::ServerContext* /*ctx*/,
        const cinder::v1::GetVersionedRequest* req,
        cinder::v1::GetVersionedResponse* resp) override {
        auto entry = handle_.store.getVersioned(req->key());
        if (entry.has_value()) {
            resp->set_status(cinder::v1::STATUS_CODE_UNSPECIFIED);
            resp->set_value(entry->value);
            resp->set_version(entry->version);
            resp->set_writer_node_hash(entry->writer_node_hash);
            resp->set_expires_at_ms(0);
        } else {
            resp->set_status(cinder::v1::STATUS_CODE_NOT_FOUND);
        }
        return grpc::Status::OK;
    }

    ::grpc::Status Ping(::grpc::ServerContext* /*ctx*/, const cinder::v1::PingRequest* /*req*/,
        cinder::v1::PingResponse* resp) override {
        cinder::Span span("grpc.ping");
        resp->set_status(cinder::v1::STATUS_CODE_UNSPECIFIED);
        return grpc::Status::OK;
    }

    ::grpc::Status MultiGet(::grpc::ServerContext* /*ctx*/, const cinder::v1::MultiGetRequest* req,
        cinder::v1::MultiGetResponse* resp) override {
        cinder::Span span("grpc.multi_get");
        for (const auto& key : req->keys()) {
            auto* r = resp->add_responses();
            auto val = handle_.store.get(key);
            if (val.has_value()) {
                r->set_status(cinder::v1::STATUS_CODE_UNSPECIFIED);
                r->set_value(*val);
            } else {
                r->set_status(cinder::v1::STATUS_CODE_NOT_FOUND);
            }
        }
        return grpc::Status::OK;
    }

    ::grpc::Status MultiSet(::grpc::ServerContext* /*ctx*/, const cinder::v1::MultiSetRequest* req,
        cinder::v1::MultiSetResponse* resp) override {
        cinder::Span span("grpc.multi_set");
        for (const auto& set_req : req->requests()) {
            auto* r = resp->add_responses();
            std::optional<milliseconds> ttl;
            if (set_req.ttl_ms() > 0) {
                ttl = milliseconds(set_req.ttl_ms());
            }

            auto owner = handle_.ring.getNode(set_req.key());
            if (owner != handle_.node_id) {
                r->set_status(cinder::v1::STATUS_CODE_NOT_READY);
                continue;
            }

            auto result = handle_.store.put(set_req.key(), set_req.value(), ttl);
            r->set_status(result.has_value() ? cinder::v1::STATUS_CODE_UNSPECIFIED
                                             : toProtoStatus(result.error().code()));
        }
        return grpc::Status::OK;
    }

    ::grpc::Status Info(::grpc::ServerContext* /*ctx*/, const cinder::v1::InfoRequest* /*req*/,
        cinder::v1::InfoResponse* resp) override {
        cinder::Span span("grpc.info");
        resp->set_json(formatNodeInfoJson(handle_.node_id,
            handle_.config,
            handle_.metrics.shardMetrics().live.current_bytes.load(),
            handle_.metrics.shardMetrics().live.current_entries.load()));
        return grpc::Status::OK;
    }

    ::grpc::Status ClusterInfo(::grpc::ServerContext* /*ctx*/,
        const cinder::v1::ClusterInfoRequest* /*req*/,
        cinder::v1::ClusterInfoResponse* resp) override {
        cinder::Span span("grpc.cluster_info");
        resp->set_json(formatClusterJson(handle_.table.snapshot()));
        return grpc::Status::OK;
    }

    ::grpc::Status RingInfo(::grpc::ServerContext* /*ctx*/,
        const cinder::v1::RingInfoRequest* /*req*/, cinder::v1::RingInfoResponse* resp) override {
        cinder::Span span("grpc.ring_info");
        resp->set_json(formatRingJson(handle_.node_id));
        return grpc::Status::OK;
    }

  private:

    CacheNodeServer::GatewayHandle handle_;
};
} // namespace

auto
createCacheService(CacheNodeServer::GatewayHandle handle) -> std::unique_ptr<grpc::Service> {
    return std::make_unique<CinderCacheServiceImpl>(std::move(handle));
}
} // namespace cinder::grpc
#endif
