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
};
} // namespace cinder
