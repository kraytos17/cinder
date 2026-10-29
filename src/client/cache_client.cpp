#include "cinder/client/cache_client.hpp"

#include <algorithm>

#include "cinder/common/tracing.hpp"

using std::chrono::milliseconds;

namespace cinder {

    CacheClient::CacheClient(ClusterConfig config) :
        pool_(config, io_ctx_),
        max_retries_(config.max_retries),
        base_backoff_ms_(config.base_backoff_ms) {
        for (const auto& n : config.nodes) {
            ring_.addNode(n.id);
        }

        io_work_ = std::make_unique<asio::executor_work_guard<asio::io_context::executor_type>>(
            asio::make_work_guard(io_ctx_)
        );
        io_thread_ = std::jthread([this](std::stop_token) { io_ctx_.run(); });
    }

    CacheClient::~CacheClient() {
        io_work_ = nullptr;
        io_ctx_.stop();
    }

    auto CacheClient::routePrimary(std::string_view key) const -> NodeId {
        return ring_.getNode(key);
    }

    auto CacheClient::preferredNode(const std::string& key) const -> NodeId {
        std::scoped_lock lock(learned_mu_);
        if (auto it = learned_.find(key); it != learned_.end()) {
            return it->second;
        }
        return ring_.getNode(key);
    }

    void CacheClient::learnOwner(const std::string& key, const NodeId& node) {
        std::scoped_lock lock(learned_mu_);
        if (learned_.size() >= K_MAX_LEARNED_OWNERS) {
            learned_.clear();
        }
        learned_.insert_or_assign(key, node);
    }

    void CacheClient::forgetOwner(const std::string& key) {
        std::scoped_lock lock(learned_mu_);
        learned_.erase(key);
    }

    auto CacheClient::tryFollowRedirect(
        const std::string& key,
        const Result<net::Response>& res,
        const NodeId& node,
        std::vector<NodeId>& visited,
        int hops
    ) -> std::optional<NodeId> {
        if (!res.has_value() || res.value().status != Errc::NotReady) {
            return std::nullopt;
        }
        // Ownership redirect — learn the true owner and follow immediately
        // (no backoff: staleness, not congestion). Follows don't consume
        // the transport attempt budget (a redirect on the final attempt
        // must still be followed); hops + the visited set bound the chain
        // instead. When the server attached an address, register it first
        // so nodes outside the client config are reachable.
        auto target = parseRedirectTarget(res.value().value.value_or(""));
        bool fresh = target.has_value()
            && target->id
            != node
            && std::find(visited.begin(), visited.end(), target->id)
            == visited.end();
        if (!fresh || hops >= K_MAX_REDIRECT_HOPS) {
            return std::nullopt;
        }

        CINDER_DEBUG("redirect", {"key", key}, {"from", node}, {"to", target->id});
        if (target->hasAddress()) {
            pool_.addAddr(target->id, target->host, target->port);
        }

        learnOwner(key, target->id);
        visited.push_back(node);
        return target->id;
    }

    auto CacheClient::refreshAfterFailure(const std::string& key) -> NodeId {
        forgetOwner(key);
        return preferredNode(key);
    }

    auto CacheClient::sendToOwner(const std::string& key, const net::Request& req)
        -> Result<net::Response> {
        Result<net::Response> res = err<net::Response>(Error(Errc::NotReady, "no attempts made"));
        auto node = preferredNode(key);
        // Nodes already visited this call: ring views can disagree mid-rebalance
        // (A redirects to B while B still redirects to A). Revisiting ends the
        // chain instead of burning the budget on a ping-pong.
        std::vector<NodeId> visited;
        int hops = 0;
        for (int attempt = 0; attempt <= max_retries_; ++attempt) {
            CINDER_TRACE(
                "route",
                {"key", key},
                {"primary", node},
                {"attempt", attempt},
                {"max", max_retries_}
            );

            res = pool_.send(node, req);
            if (auto next = tryFollowRedirect(key, res, node, visited, hops)) {
                node = *next;
                ++hops;
                --attempt; // the follow-up send reuses this attempt's budget
                continue;
            }
            if (!res.has_value()) {
                // Transport failure — a learned owner may be stale (node gone or
                // unknown); forget it so the next attempt re-resolves via the ring.
                node = refreshAfterFailure(key);
            }
            if (!retryable(res) || attempt == max_retries_) {
                return res;
            }
            std::this_thread::sleep_for(
                milliseconds(jitterBackoff(retryBackoff(attempt, base_backoff_ms_)))
            );
        }
        return res;
    }

    auto CacheClient::set(
        const std::string& key,
        const std::string& value,
        std::optional<milliseconds> ttl
    ) -> Result<void> {
        Span span("client.set");
        net::Request req{.opcode = net::Opcode::Set, .key = key, .value = value, .ttl = ttl};
        return sendToOwner(key, req).and_then([](const net::Response& res) -> Result<void> {
            if (res.status != Errc::OK) {
                return err(Error(res.status));
            }
            return ok();
        });
    }

    auto CacheClient::get(const std::string& key) -> std::optional<std::string> {
        Span span("client.get");
        net::Request req{.opcode = net::Opcode::Get, .key = key, .value = {}};
        return sendToOwner(key, req)
            .transform([](net::Response res) {
            return std::move(res.value);
        }).value_or(std::nullopt);
    }

    auto CacheClient::remove(const std::string& key) -> bool {
        Span span("client.remove");
        net::Request req{.opcode = net::Opcode::Del, .key = key, .value = {}};
        return sendToOwner(key, req)
            .and_then([](const net::Response& res) -> Result<bool> {
            return ok(res.status == Errc::OK);
        }).value_or(false);
    }

    auto CacheClient::multiGet(const std::vector<std::string>& keys)
        -> ankerl::unordered_dense::map<std::string, std::string> {
        Span span("client.multi_get");
        // Group keys by their ring owner, then pipeline all Gets per node.
        ankerl::unordered_dense::map<NodeId, std::vector<std::string>> by_owner;
        for (const auto& key : keys) {
            by_owner[routePrimary(key)].push_back(key);
        }

        ankerl::unordered_dense::map<std::string, std::string> result;
        for (const auto& [node, node_keys] : by_owner) {
            std::vector<net::Request> reqs;
            reqs.reserve(node_keys.size());
            for (const auto& key : node_keys) {
                reqs.push_back({.opcode = net::Opcode::Get, .key = key, .value = {}});
            }

            Result<std::vector<net::Response>> res =
                err<std::vector<net::Response>>(Error(Errc::NotReady, "no attempts made"));
            for (int attempt = 0; attempt <= max_retries_; ++attempt) {
                CINDER_TRACE(
                    "batch",
                    {"node", node},
                    {"keys", keys.size()},
                    {"attempt", attempt},
                    {"max", max_retries_}
                );

                res = pool_.sendBatch(node, reqs);
                if (!retryable(res) || attempt == max_retries_) {
                    break;
                }
                std::this_thread::sleep_for(
                    milliseconds(jitterBackoff(retryBackoff(attempt, base_backoff_ms_)))
                );
            }
            if (!res.has_value()) {
                continue; // node unreachable — those keys are simply missing
            }

            std::vector<std::string> redirected;
            for (size_t i = 0; i < node_keys.size() && i < res.value().size(); i++) {
                auto& resp = res.value()[i];
                if (resp.status == Errc::OK && resp.value.has_value()) {
                    result.emplace(node_keys[i], *resp.value);
                } else if (
                    resp.status
                    == Errc::NotReady
                    && parseRedirectTarget(resp.value.value_or("")).has_value()
                ) {
                    // Stale ring view — the key moved. Re-issue through the
                    // redirect-following single-key path (which learns the owner).
                    redirected.push_back(node_keys[i]);
                }
            }
            for (const auto& k : redirected) {
                net::Request single{.opcode = net::Opcode::Get, .key = k, .value = {}};
                auto one = sendToOwner(k, single);
                if (one.has_value()
                    && one.value().status
                    == Errc::OK
                    && one.value().value.has_value()) {
                    result.emplace(k, *one.value().value);
                }
            }
        }
        return result;
    }

    auto CacheClient::getVersioned(const std::string& key) -> std::optional<VersionedValue> {
        Span span("client.get_versioned");
        net::Request req{.opcode = net::Opcode::GetVersioned, .key = key, .value = {}};
        auto res = sendToOwner(key, req);
        if (!res.has_value()) {
            return std::nullopt;
        }

        net::Response r = std::move(res.value());
        if (r.status != Errc::OK || !r.value.has_value()) {
            return std::nullopt;
        }

        VersionedValue out;
        out.value = std::move(*r.value);
        out.version = r.version;
        out.writer_node_hash = r.writer_node_hash;
        out.expires_at = r.expires_at;
        return out;
    }

    auto CacheClient::ttl(const std::string& key) -> std::optional<milliseconds> {
        auto got = getVersioned(key);
        if (!got.has_value() || !got->expires_at.has_value()) {
            return std::nullopt;
        }

        auto remaining = *got->expires_at - std::chrono::system_clock::now();
        if (remaining <= std::chrono::system_clock::duration::zero()) {
            return milliseconds{0};
        }
        return std::chrono::duration_cast<milliseconds>(remaining);
    }

    auto CacheClient::multiSet(
        const std::vector<std::pair<std::string, std::string>>& kvs,
        std::optional<milliseconds> ttl
    ) -> size_t {
        Span span("client.multi_set");
        // Group keys by their ring owner, then pipeline all Sets per node —
        // the same shape as multiGet, but with Set requests.
        ankerl::unordered_dense::map<NodeId, std::vector<std::pair<std::string, std::string>>>
            by_owner;
        for (const auto& kv : kvs) {
            by_owner[routePrimary(kv.first)].push_back(kv);
        }

        size_t stored = 0;
        for (const auto& [node, node_kvs] : by_owner) {
            std::vector<net::Request> reqs;
            reqs.reserve(node_kvs.size());
            for (const auto& [key, value] : node_kvs) {
                reqs.push_back(
                    {.opcode = net::Opcode::Set, .key = key, .value = value, .ttl = ttl}
                );
            }

            Result<std::vector<net::Response>> res =
                err<std::vector<net::Response>>(Error(Errc::NotReady, "no attempts made"));
            for (int attempt = 0; attempt <= max_retries_; ++attempt) {
                CINDER_TRACE(
                    "batch",
                    {"node", node},
                    {"keys", kvs.size()},
                    {"attempt", attempt},
                    {"max", max_retries_}
                );

                res = pool_.sendBatch(node, reqs);
                if (!retryable(res) || attempt == max_retries_) {
                    break;
                }
                std::this_thread::sleep_for(
                    milliseconds(jitterBackoff(retryBackoff(attempt, base_backoff_ms_)))
                );
            }
            if (!res.has_value()) {
                continue; // node unreachable — those keys are simply not stored
            }

            std::vector<std::pair<std::string, std::string>> redirected;
            for (size_t i = 0; i < node_kvs.size() && i < res.value().size(); i++) {
                if (res.value()[i].status == Errc::OK) {
                    ++stored;
                } else if (
                    res.value()[i].status
                    == Errc::NotReady
                    && parseRedirectTarget(res.value()[i].value.value_or("")).has_value()
                ) {
                    // Stale ring view — the key moved. Re-issue through the
                    // redirect-following single-key path (which learns the owner).
                    redirected.push_back(node_kvs[i]);
                }
            }
            for (const auto& [k, v] : redirected) {
                net::Request single{
                    .opcode = net::Opcode::Set,
                    .key = k,
                    .value = v,
                    .ttl = ttl,
                };

                auto one = sendToOwner(k, single);
                if (one.has_value() && one.value().status == Errc::OK) {
                    ++stored;
                }
            }
        }
        return stored;
    }
} // namespace cinder
