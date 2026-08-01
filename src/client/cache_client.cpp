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
    auto res = pool_.send(node, req);
    if (!res.hasValue()) {
        return err(res.error());
    }
    if (res.value().status != Errc::OK) {
        return err(Error(res.value().status));
    }
    return ok();
}

auto
CacheClient::get(const std::string& key) -> std::optional<std::string> {
    auto node = routePrimary(key);
    net::Request req{net::Opcode::Get, key, {}, std::nullopt};
    auto res = pool_.send(node, req);
    if (!res.hasValue()) {
        return std::nullopt;
    }
    return std::move(res.value().value);
}

auto
CacheClient::remove(const std::string& key) -> bool {
    auto node = routePrimary(key);
    net::Request req{net::Opcode::Del, key, {}, std::nullopt};
    auto res = pool_.send(node, req);
    return res.hasValue() && res.value().status == Errc::OK;
}
} // namespace cinder
