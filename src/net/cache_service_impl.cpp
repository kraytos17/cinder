#ifdef CINDER_ENABLE_GRPC

#include <atomic>
#include <optional>
#include <string>
#include <string_view>

#include "cinder/common/config.hpp"
#include "cinder/common/hmac.hpp"
#include "cinder/common/tracing.hpp"
#include "cinder/net/grpc_gateway.hpp"
#include "cinder/v1/cache.grpc.pb.h"

using std::chrono::milliseconds;

namespace cinder::grpc {
    namespace {

        auto toProtoStatus(Errc code) -> cinder::v1::StatusCode {
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
                case Errc::InternalError:
                    return cinder::v1::STATUS_CODE_INTERNAL_ERROR;
                case Errc::Timeout:
                    return cinder::v1::STATUS_CODE_TIMEOUT;
                case Errc::NotReady:
                    return cinder::v1::STATUS_CODE_NOT_READY;
                case Errc::PermissionDenied:
                    return cinder::v1::STATUS_CODE_PERMISSION_DENIED;
                case Errc::CorruptData:
                    return cinder::v1::STATUS_CODE_CORRUPT_DATA;
            }
            return cinder::v1::STATUS_CODE_INTERNAL_ERROR;
        }

        // Per-RPC reactor for the async Get/Set/Delete handlers. Heap-allocated
        // per call, returned to gRPC, and destroyed in OnDone; the response
        // proto is owned by gRPC until then, so holding the pointer is safe.
        // Completion runs on whatever thread CacheOperations uses (inline for
        // local operations, the replication executor otherwise), so Finish is
        // guarded to fire exactly once: a client cancel races the operation
        // result, and a cancelled RPC must still be Finished.
        template <typename Response> class UnaryReactor final : public ::grpc::ServerUnaryReactor {
        public:

            explicit UnaryReactor(Response* resp) :
                resp_(resp) {}

            auto response() -> Response* {
                return resp_;
            }

            void finishOnce(::grpc::Status status) {
                bool expected = false;
                if (finished_.compare_exchange_strong(expected, true)) {
                    Finish(std::move(status));
                }
            }

            void OnDone() override {
                delete this;
            }

            void OnCancel() override {}

        private:

            Response* resp_;
            std::atomic<bool> finished_{false};
        };

        // Implements the CinderCacheService gRPC service, mirroring the dispatch
        // logic in TcpConnection::handleRequest. Get/Set/Delete use the callback
        // API so replication waits never block a gRPC worker thread; the
        // remaining local-read RPCs stay synchronous (hybrid server).
        class CinderCacheServiceImpl final :
            public cinder::v1::CinderCacheService::
                WithCallbackMethod_Get<cinder::v1::CinderCacheService::WithCallbackMethod_Set<
                    cinder::v1::CinderCacheService::
                        WithCallbackMethod_Delete<cinder::v1::CinderCacheService::Service>
                >> {
        public:

            explicit CinderCacheServiceImpl(
                CacheNodeServer::GatewayHandle handle,
                std::string client_shared_secret = {}
            ) :
                handle_(std::move(handle)),
                client_shared_secret_(std::move(client_shared_secret)) {}

            // Client auth over gRPC metadata. The bearer is the hex encoding
            // of the static client token (raw bytes are not valid metadata).
            // Returns OK when authorized, UNAUTHENTICATED otherwise. The
            // presented value is never logged.
            auto checkAuth(const ::grpc::ServerContextBase* ctx) -> ::grpc::Status {
                if (client_shared_secret_.empty()) {
                    return ::grpc::Status::OK;
                }

                const auto& metadata = ctx->client_metadata();
                auto range = metadata.equal_range("authorization");
                std::string presented;
                size_t count = 0;
                for (auto it = range.first; it != range.second; ++it) {
                    ++count;
                    presented.assign(it->second.data(), it->second.size());
                }
                if (count != 1) {
                    return {::grpc::StatusCode::UNAUTHENTICATED, "missing credentials"};
                }

                constexpr std::string_view k_prefix = "Bearer ";
                std::string_view presented_view(presented);
                if (presented_view.size()
                    != k_prefix.size()
                    + 64
                    || !presented_view.starts_with(k_prefix)) {
                    return {::grpc::StatusCode::UNAUTHENTICATED, "invalid credentials"};
                }

                auto expected_token = generateClientAuthToken(client_shared_secret_);
                if (expected_token.size() != 32) {
                    return {::grpc::StatusCode::UNAUTHENTICATED, "invalid credentials"};
                }

                auto expected_hex = toHex(expected_token);
                auto given_hex = presented.substr(k_prefix.size());
                if (!constantTimeEqualAscii(given_hex, expected_hex)) {
                    return {::grpc::StatusCode::UNAUTHENTICATED, "invalid credentials"};
                }
                return ::grpc::Status::OK;
            }

            static auto toHex(std::string_view bytes) -> std::string {
                constexpr char k_digits[] = "0123456789abcdef";
                std::string out;
                out.reserve(bytes.size() * 2);
                for (char c : bytes) {
                    const auto v = static_cast<unsigned int>(static_cast<unsigned char>(c));
                    out.push_back(k_digits[(v >> 4U) & 0xFU]);
                    out.push_back(k_digits[v & 0xFU]);
                }
                return out;
            }

            static auto constantTimeEqualAscii(std::string_view a, std::string_view b) -> bool {
                if (a.size() != b.size()) {
                    return false;
                }

                auto diff = 0U;
                for (size_t i = 0; i < a.size(); ++i) {
                    const auto ca = static_cast<unsigned int>(static_cast<unsigned char>(a[i]));
                    const auto cb = static_cast<unsigned int>(static_cast<unsigned char>(b[i]));
                    diff |= (ca ^ cb);
                }
                return diff == 0U;
            }

            // Inbound trace metadata contract (see docs/tracing.md): ASCII
            // decimal uint64 trace/span ids under "cinder-trace-id" and
            // "cinder-span-id". Absent or malformed values yield an invalid
            // context and the RPC starts a fresh trace rather than failing.
            // Metadata keys are lowercase per gRPC convention; only the first
            // value of each key is honored. Never logs metadata contents.
            static auto traceParentFromMetadata(const ::grpc::ServerContextBase* ctx)
                -> cinder::SpanContext {
                std::string_view trace_str;
                std::string_view span_str;
                const auto& metadata = ctx->client_metadata();
                if (auto it = metadata.find("cinder-trace-id"); it != metadata.end()) {
                    trace_str = {it->second.data(), it->second.size()};
                }
                if (auto it = metadata.find("cinder-span-id"); it != metadata.end()) {
                    span_str = {it->second.data(), it->second.size()};
                }
                if (trace_str.empty() || span_str.empty()) {
                    return {};
                }
                return cinder::parseSpanContext(trace_str, span_str);
            }

            ::grpc::ServerUnaryReactor* Get(
                ::grpc::CallbackServerContext* ctx,
                const cinder::v1::GetRequest* req,
                cinder::v1::GetResponse* resp
            ) override {
                auto parent = traceParentFromMetadata(ctx);
                if (auto auth = checkAuth(ctx); !auth.ok()) {
                    cinder::Span span("grpc.get", parent);
                    span.setStatus(cinder::SpanStatus::Error);
                    auto* reactor = new UnaryReactor<cinder::v1::GetResponse>(resp);
                    reactor->finishOnce(auth);
                    return reactor;
                }

                auto* reactor = new UnaryReactor<cinder::v1::GetResponse>(resp);
                handle_.operations
                    .getAsync(req->key(), [reactor, parent](CacheOperations::GetResult result) {
                    cinder::Span span("grpc.get", parent);
                    auto* resp = reactor->response();
                    if (result.has_value()) {
                        resp->set_status(cinder::v1::STATUS_CODE_UNSPECIFIED);
                        resp->set_value(*result);
                        span.setStatus(cinder::SpanStatus::Ok);
                    } else {
                        resp->set_status(toProtoStatus(result.error().code()));
                        span.setStatus(cinder::SpanStatus::Error);
                    }
                    reactor->finishOnce(::grpc::Status::OK);
                }, parent.trace_id, parent.span_id);
                return reactor;
            }

            ::grpc::ServerUnaryReactor* Set(
                ::grpc::CallbackServerContext* ctx,
                const cinder::v1::SetRequest* req,
                cinder::v1::SetResponse* resp
            ) override {
                auto parent = traceParentFromMetadata(ctx);
                if (auto auth = checkAuth(ctx); !auth.ok()) {
                    cinder::Span span("grpc.set", parent);
                    span.setStatus(cinder::SpanStatus::Error);
                    auto* reactor = new UnaryReactor<cinder::v1::SetResponse>(resp);
                    reactor->finishOnce(auth);
                    return reactor;
                }

                auto owner = handle_.ring.getNode(req->key());
                if (owner != handle_.node_id) {
                    cinder::Span span("grpc.set", parent);
                    resp->set_status(cinder::v1::STATUS_CODE_NOT_READY);
                    span.setStatus(cinder::SpanStatus::Ok);
                    auto* reactor = new UnaryReactor<cinder::v1::SetResponse>(resp);
                    reactor->finishOnce(::grpc::Status::OK);
                    return reactor;
                }

                std::optional<milliseconds> ttl;
                if (req->ttl_ms() > 0) {
                    ttl = milliseconds(req->ttl_ms());
                }

                auto* reactor = new UnaryReactor<cinder::v1::SetResponse>(resp);
                handle_.operations.setAsync(
                    req->key(),
                    req->value(),
                    ttl,
                    [reactor, parent](Result<void> result) {
                    cinder::Span span("grpc.set", parent);
                    auto* resp = reactor->response();
                    resp->set_status(
                        result.has_value() ? cinder::v1::STATUS_CODE_UNSPECIFIED
                                           : toProtoStatus(result.error().code())
                    );
                    span.setStatus(
                        result.has_value() ? cinder::SpanStatus::Ok : cinder::SpanStatus::Error
                    );
                    reactor->finishOnce(::grpc::Status::OK);
                },
                    parent.trace_id,
                    parent.span_id
                );
                return reactor;
            }

            ::grpc::ServerUnaryReactor* Delete(
                ::grpc::CallbackServerContext* ctx,
                const cinder::v1::DeleteRequest* req,
                cinder::v1::DeleteResponse* resp
            ) override {
                auto parent = traceParentFromMetadata(ctx);
                if (auto auth = checkAuth(ctx); !auth.ok()) {
                    cinder::Span span("grpc.delete", parent);
                    span.setStatus(cinder::SpanStatus::Error);
                    auto* reactor = new UnaryReactor<cinder::v1::DeleteResponse>(resp);
                    reactor->finishOnce(auth);
                    return reactor;
                }

                auto* reactor = new UnaryReactor<cinder::v1::DeleteResponse>(resp);
                handle_.operations.deleteAsync(req->key(), [reactor, parent](Result<void> result) {
                    cinder::Span span("grpc.delete", parent);
                    auto* resp = reactor->response();
                    resp->set_status(
                        result.has_value() ? cinder::v1::STATUS_CODE_UNSPECIFIED
                                           : toProtoStatus(result.error().code())
                    );
                    span.setStatus(
                        result.has_value() ? cinder::SpanStatus::Ok : cinder::SpanStatus::Error
                    );
                    reactor->finishOnce(::grpc::Status::OK);
                }, parent.trace_id, parent.span_id);
                return reactor;
            }

            ::grpc::Status GetVersioned(
                ::grpc::ServerContext* ctx,
                const cinder::v1::GetVersionedRequest* req,
                cinder::v1::GetVersionedResponse* resp
            ) override {
                cinder::Span span("grpc.get_versioned", traceParentFromMetadata(ctx));
                if (auto auth = checkAuth(ctx); !auth.ok()) {
                    span.setStatus(cinder::SpanStatus::Error);
                    return auth;
                }

                auto entry = handle_.store.getVersioned(req->key());
                if (entry.has_value() && !entry->isTombstone()) {
                    resp->set_status(cinder::v1::STATUS_CODE_UNSPECIFIED);
                    resp->set_value(entry->value);
                    resp->set_version(entry->version());
                    resp->set_writer_node_hash(entry->writer_node_hash);
                    resp->set_expires_at_ms(0);
                } else {
                    // Absent or deleted: same client-visible NOT_FOUND. The
                    // tombstone marker itself is an internal replication
                    // concern (quorum votes, read repair), not exposed here
                    // to avoid a proto change.
                    resp->set_status(cinder::v1::STATUS_CODE_NOT_FOUND);
                }

                span.setStatus(cinder::SpanStatus::Ok);
                return ::grpc::Status::OK;
            }

            ::grpc::Status Ping(
                ::grpc::ServerContext* ctx,
                const cinder::v1::PingRequest* /*req*/,
                cinder::v1::PingResponse* resp
            ) override {
                cinder::Span span("grpc.ping", traceParentFromMetadata(ctx));
                if (auto auth = checkAuth(ctx); !auth.ok()) {
                    span.setStatus(cinder::SpanStatus::Error);
                    return auth;
                }

                resp->set_status(cinder::v1::STATUS_CODE_UNSPECIFIED);
                span.setStatus(cinder::SpanStatus::Ok);
                return ::grpc::Status::OK;
            }

            ::grpc::Status MultiGet(
                ::grpc::ServerContext* ctx,
                const cinder::v1::MultiGetRequest* req,
                cinder::v1::MultiGetResponse* resp
            ) override {
                cinder::Span span("grpc.multi_get", traceParentFromMetadata(ctx));
                if (auto auth = checkAuth(ctx); !auth.ok()) {
                    span.setStatus(cinder::SpanStatus::Error);
                    return auth;
                }
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

                span.setStatus(cinder::SpanStatus::Ok);
                return ::grpc::Status::OK;
            }

            ::grpc::Status MultiSet(
                ::grpc::ServerContext* ctx,
                const cinder::v1::MultiSetRequest* req,
                cinder::v1::MultiSetResponse* resp
            ) override {
                cinder::Span span("grpc.multi_set", traceParentFromMetadata(ctx));
                if (auto auth = checkAuth(ctx); !auth.ok()) {
                    span.setStatus(cinder::SpanStatus::Error);
                    return auth;
                }

                bool failed = false;
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
                    r->set_status(
                        result.has_value() ? cinder::v1::STATUS_CODE_UNSPECIFIED
                                           : toProtoStatus(result.error().code())
                    );
                    failed = failed || !result.has_value();
                }

                span.setStatus(failed ? cinder::SpanStatus::Error : cinder::SpanStatus::Ok);
                return ::grpc::Status::OK;
            }

            ::grpc::Status Info(
                ::grpc::ServerContext* ctx,
                const cinder::v1::InfoRequest* /*req*/,
                cinder::v1::InfoResponse* resp
            ) override {
                cinder::Span span("grpc.info", traceParentFromMetadata(ctx));
                if (auto auth = checkAuth(ctx); !auth.ok()) {
                    span.setStatus(cinder::SpanStatus::Error);
                    return auth;
                }

                resp->set_json(formatNodeInfoJson(
                    handle_.node_id,
                    handle_.config,
                    handle_.metrics.shardMetrics().live.current_bytes.load(),
                    handle_.metrics.shardMetrics().live.current_entries.load()
                ));

                span.setStatus(cinder::SpanStatus::Ok);
                return ::grpc::Status::OK;
            }

            ::grpc::Status ClusterInfo(
                ::grpc::ServerContext* ctx,
                const cinder::v1::ClusterInfoRequest* /*req*/,
                cinder::v1::ClusterInfoResponse* resp
            ) override {
                cinder::Span span("grpc.cluster_info", traceParentFromMetadata(ctx));
                if (auto auth = checkAuth(ctx); !auth.ok()) {
                    span.setStatus(cinder::SpanStatus::Error);
                    return auth;
                }

                resp->set_json(formatClusterJson(handle_.table.snapshot()));
                span.setStatus(cinder::SpanStatus::Ok);
                return ::grpc::Status::OK;
            }

            ::grpc::Status RingInfo(
                ::grpc::ServerContext* ctx,
                const cinder::v1::RingInfoRequest* /*req*/,
                cinder::v1::RingInfoResponse* resp
            ) override {
                cinder::Span span("grpc.ring_info", traceParentFromMetadata(ctx));
                if (auto auth = checkAuth(ctx); !auth.ok()) {
                    span.setStatus(cinder::SpanStatus::Error);
                    return auth;
                }

                resp->set_json(formatRingJson(handle_.node_id));
                span.setStatus(cinder::SpanStatus::Ok);
                return ::grpc::Status::OK;
            }

        private:

            CacheNodeServer::GatewayHandle handle_;
            std::string client_shared_secret_;
        };
    } // namespace

    auto createCacheService(CacheNodeServer::GatewayHandle handle, std::string client_shared_secret)
        -> std::unique_ptr<::grpc::Service> {
        return std::make_unique<CinderCacheServiceImpl>(
            std::move(handle),
            std::move(client_shared_secret)
        );
    }
} // namespace cinder::grpc
#endif
