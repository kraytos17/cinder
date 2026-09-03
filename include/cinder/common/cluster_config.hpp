#pragma once

#include <string>
#include <vector>

#include "cinder/common/types.hpp"

namespace cinder {

struct ClusterConfig {
    struct NodeConfig {
        NodeId id;
        std::string host;
        uint16_t port = 0;
    };

    std::vector<NodeConfig> nodes;
    // Retry policy for transient failures (Timeout, NotReady, transport errors).
    int max_retries = 2;      // attempts beyond the first (0 = no retry)
    int base_backoff_ms = 25; // exponential: base * 2^attempt, ±20% jitter
    // Per-RPC deadline for the connection pool (0 = no timeout).
    int rpc_timeout_ms = 5'000;
};
} // namespace cinder
