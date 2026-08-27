#include "cinder/client/cache_client.hpp"

using std::chrono::milliseconds;

namespace cinder {

CacheClient::CacheClient(ClusterConfig config)
    : pool_(config, io_ctx_) {
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
    auto node = routePrimary(key);
    auto res = pool_.send(node, req);
    if (!res.has_value() || res.value().status != Errc::NotReady) {
        return res;
    }

    // The node redirected us to the ring owner — follow once, then give up.
    auto target = parseRedirect(res.value().value.value_or(""));
    if (!target.has_value() || *target == node) {
        return res;
    }
    return pool_.send(*target, req);
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

        auto res = pool_.sendBatch(node, reqs);
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
