#include "cinder/client/cache_client.hpp"

#include <algorithm>

#include "cinder/common/tracing.hpp"

using std::chrono::milliseconds;

namespace cinder {

CacheClient::CacheClient(ClusterConfig config)
    : pool_(config, io_ctx_),
      max_retries_(config.max_retries),
      base_backoff_ms_(config.base_backoff_ms) {
    for (const auto& n : config.nodes) {
        ring_.addNode(n.id);
    }

    io_work_ = std::make_unique<asio::executor_work_guard<asio::io_context::executor_type>>(
        asio::make_work_guard(io_ctx_));
    io_thread_ = std::jthread([this](std::stop_token) { io_ctx_.run(); });
}

CacheClient::~CacheClient() {
    io_work_ = nullptr;
    io_ctx_.stop();
}

auto
CacheClient::routePrimary(std::string_view key) const -> NodeId {
    return ring_.getNode(key);
}

auto
CacheClient::preferredNode(const std::string& key) const -> NodeId {
    std::scoped_lock lock(learned_mu_);
    if (auto it = learned_.find(key); it != learned_.end()) {
        return it->second;
    }
    return ring_.getNode(key);
}

void
CacheClient::learnOwner(const std::string& key, const NodeId& node) {
    std::scoped_lock lock(learned_mu_);
    if (learned_.size() >= K_MAX_LEARNED_OWNERS) {
        learned_.clear();
    }
    learned_.insert_or_assign(key, node);
}

void
CacheClient::forgetOwner(const std::string& key) {
    std::scoped_lock lock(learned_mu_);
    learned_.erase(key);
}

auto
CacheClient::sendToOwner(const std::string& key, const net::Request& req) -> Result<net::Response> {
    Result<net::Response> res = err<net::Response>(Error(Errc::NotReady, "no attempts made"));
    auto node = preferredNode(key);
    // Nodes already visited this call: ring views can disagree mid-rebalance
    // (A redirects to B while B still redirects to A). Revisiting ends the
    // chain instead of burning the budget on a ping-pong.
    std::vector<NodeId> visited;
    int hops = 0;
    for (int attempt = 0; attempt <= max_retries_; ++attempt) {
        Event::trace("route",
            {{"key", key},
                {"primary", node},
                {"attempt", std::to_string(attempt)},
                {"max", std::to_string(max_retries_)}});

        res = pool_.send(node, req);
        if (res.has_value() && res.value().status == Errc::NotReady) {
            // Ownership redirect — learn the true owner and follow immediately
            // (no backoff: staleness, not congestion). Follows don't consume
            // the transport attempt budget (a redirect on the final attempt
            // must still be followed); hops + the visited set bound the chain
            // instead. When the server attached an address, register it first
            // so nodes outside the client config are reachable.
            auto target = parseRedirectTarget(res.value().value.value_or(""));
            bool fresh = target.has_value() && target->id != node
                         && std::find(visited.begin(), visited.end(), target->id) == visited.end();

            if (fresh && hops < K_MAX_REDIRECT_HOPS) {
                Event::debug("redirect", {{"key", key}, {"from", node}, {"to", target->id}});
                if (target->hasAddress()) {
                    pool_.addAddr(target->id, target->host, target->port);
                }

                learnOwner(key, target->id);
                visited.push_back(node);
                node = target->id;
                ++hops;
                --attempt; // the follow-up send reuses this attempt's budget
                continue;
            }
        }
        if (!res.has_value()) {
            // Transport failure — a learned owner may be stale (node gone or
            // unknown); forget it so the next attempt re-resolves via the ring.
            forgetOwner(key);
            node = preferredNode(key);
        }
        if (!retryable(res) || attempt == max_retries_) {
            return res;
        }
        std::this_thread::sleep_for(
            milliseconds(jitterBackoff(retryBackoff(attempt, base_backoff_ms_))));
    }
    return res;
}

auto
CacheClient::set(const std::string& key, const std::string& value, std::optional<milliseconds> ttl)
    -> Result<void> {
    Span span("client.set");
    net::Request req{.opcode = net::Opcode::Set, .key = key, .value = value, .ttl = ttl};
    return sendToOwner(key, req).and_then([](const net::Response& res) -> Result<void> {
        if (res.status != Errc::OK) {
            return err(Error(res.status));
        }
        return ok();
    });
}

auto
CacheClient::get(const std::string& key) -> std::optional<std::string> {
    Span span("client.get");
    net::Request req{.opcode = net::Opcode::Get, .key = key, .value = {}};
    return sendToOwner(key, req).transform([](net::Response res) {
        return std::move(res.value);
    }).value_or(std::nullopt);
}

auto
CacheClient::remove(const std::string& key) -> bool {
    Span span("client.remove");
    net::Request req{.opcode = net::Opcode::Del, .key = key, .value = {}};
    return sendToOwner(key, req)
        .and_then([](const net::Response& res) -> Result<bool> {
        return ok(res.status == Errc::OK);
    }).value_or(false);
}

auto
CacheClient::multiGet(const std::vector<std::string>& keys)
    -> std::unordered_map<std::string, std::string> {
    Span span("client.multi_get");
    // Group keys by their ring owner, then pipeline all Gets per node.
    std::unordered_map<NodeId, std::vector<std::string>> by_owner;
    for (const auto& key : keys) {
        by_owner[routePrimary(key)].push_back(key);
    }

    std::unordered_map<std::string, std::string> result;
    for (const auto& [node, node_keys] : by_owner) {
        std::vector<net::Request> reqs;
        reqs.reserve(node_keys.size());
        for (const auto& key : node_keys) {
            reqs.push_back({.opcode = net::Opcode::Get, .key = key, .value = {}});
        }

        Result<std::vector<net::Response>> res =
            err<std::vector<net::Response>>(Error(Errc::NotReady, "no attempts made"));
        for (int attempt = 0; attempt <= max_retries_; ++attempt) {
            Event::trace("batch",
                {{"node", node},
                    {"keys", std::to_string(keys.size())},
                    {"attempt", std::to_string(attempt)},
                    {"max", std::to_string(max_retries_)}});

            res = pool_.sendBatch(node, reqs);
            if (!retryable(res) || attempt == max_retries_) {
                break;
            }
            std::this_thread::sleep_for(
                milliseconds(jitterBackoff(retryBackoff(attempt, base_backoff_ms_))));
        }
        if (!res.has_value()) {
            continue; // node unreachable — those keys are simply missing
        }

        std::vector<std::string> redirected;
        for (size_t i = 0; i < node_keys.size() && i < res.value().size(); i++) {
            auto& resp = res.value()[i];
            if (resp.status == Errc::OK && resp.value.has_value()) {
                result.emplace(node_keys[i], *resp.value);
            } else if (resp.status == Errc::NotReady
                       && parseRedirectTarget(resp.value.value_or("")).has_value()) {
                // Stale ring view — the key moved. Re-issue through the
                // redirect-following single-key path (which learns the owner).
                redirected.push_back(node_keys[i]);
            }
        }
        for (const auto& k : redirected) {
            net::Request single{.opcode = net::Opcode::Get, .key = k, .value = {}};
            auto one = sendToOwner(k, single);
            if (one.has_value() && one.value().status == Errc::OK
                && one.value().value.has_value()) {
                result.emplace(k, *one.value().value);
            }
        }
    }
    return result;
}
} // namespace cinder
