#include "cinder/client/cache_client.hpp"

#include "cinder/common/logger.hpp"

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
CacheClient::sendToOwner(const std::string& key, const net::Request& req) -> Result<net::Response> {
    Result<net::Response> res = err<net::Response>(Error(Errc::NotReady, "no attempts made"));
    for (int attempt = 0; attempt <= max_retries_; ++attempt) {
        auto node = routePrimary(key);
        Logger::trace("cinder client: route key={} primary={} attempt={}/{}",
            key,
            node,
            attempt,
            max_retries_);

        res = pool_.send(node, req);
        if (res.has_value() && res.value().status == Errc::NotReady) {
            // The node redirected us to the ring owner — follow once, then give up.
            auto target = parseRedirect(res.value().value.value_or(""));
            if (target.has_value() && *target != node) {
                Logger::debug("cinder client: redirect key={} from={} to={}", key, node, *target);
                res = pool_.send(*target, req);
            }
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
    net::Request req{net::Opcode::Set, key, value, ttl};
    return sendToOwner(key, req).and_then([](net::Response res) -> Result<void> {
        if (res.status != Errc::OK) {
            return err(Error(res.status));
        }
        return ok();
    });
}

auto
CacheClient::get(const std::string& key) -> std::optional<std::string> {
    net::Request req{net::Opcode::Get, key, {}, std::nullopt};
    return sendToOwner(key, req).transform([](net::Response res) {
        return std::move(res.value);
    }).value_or(std::nullopt);
}

auto
CacheClient::remove(const std::string& key) -> bool {
    net::Request req{net::Opcode::Del, key, {}, std::nullopt};
    return sendToOwner(key, req)
        .and_then([](net::Response res) -> Result<bool> {
        return ok(res.status == Errc::OK);
    }).value_or(false);
}

auto
CacheClient::multiGet(const std::vector<std::string>& keys)
    -> std::unordered_map<std::string, std::string> {
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
            reqs.push_back({net::Opcode::Get, key, {}, std::nullopt});
        }

        Result<std::vector<net::Response>> res =
            err<std::vector<net::Response>>(Error(Errc::NotReady, "no attempts made"));
        for (int attempt = 0; attempt <= max_retries_; ++attempt) {
            Logger::trace("cinder client: batch node={} keys={} attempt={}/{}",
                node,
                node_keys.size(),
                attempt,
                max_retries_);

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
        for (size_t i = 0; i < node_keys.size() && i < res.value().size(); i++) {
            auto& resp = res.value()[i];
            if (resp.status == Errc::OK && resp.value.has_value()) {
                result.emplace(node_keys[i], *resp.value);
            }
        }
    }
    return result;
}
} // namespace cinder
