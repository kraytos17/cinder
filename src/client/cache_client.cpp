#include "cinder/client/cache_client.hpp"

namespace cinder {

CacheClient::CacheClient(ClusterConfig config)
    : pool_(config, io_ctx_) {
    for (const auto& n : config.nodes) {
        ring_.addNode(n.id);
    }
}

CacheClient::~CacheClient() = default;

auto
CacheClient::routePrimary(std::string_view key) const -> NodeId {
    return ring_.getNode(key);
}

auto
CacheClient::set(const std::string& key, const std::string& value,
    std::optional<std::chrono::milliseconds> ttl) -> Result<void> {
    auto node = routePrimary(key);
    net::Request req{net::Opcode::Set, key, value, ttl};
    return pool_.send(node, req).and_then([](net::Response res) -> Result<void> {
        if (res.status != Errc::OK) {
            return err(Error(res.status));
        }
        return ok();
    });
}

auto
CacheClient::get(const std::string& key) -> std::optional<std::string> {
    auto node = routePrimary(key);
    net::Request req{net::Opcode::Get, key, {}, std::nullopt};
    auto res = pool_.send(node, req);
    if (!res.has_value()) {
        return std::nullopt;
    }
    return std::move(res.value().value);
}

auto
CacheClient::remove(const std::string& key) -> bool {
    auto node = routePrimary(key);
    net::Request req{net::Opcode::Del, key, {}, std::nullopt};
    return pool_.send(node, req)
        .and_then([](net::Response res) -> Result<bool> {
        return ok(res.status == Errc::OK);
    }).value_or(false);
}
} // namespace cinder
