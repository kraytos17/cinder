#pragma once

#include <functional>

#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"
#include "cinder/net/protocol.hpp"

namespace cinder {

class Transport {
  public:

    Transport() = default;
    virtual ~Transport() = default;
    Transport(const Transport&) = delete;
    auto operator=(const Transport&) -> Transport& = delete;
    Transport(Transport&&) = delete;
    auto operator=(Transport&&) -> Transport& = delete;

    using MessageHandler = std::move_only_function<void(const NodeId& from, const net::Request&)>;

    virtual auto send(const NodeId& to, const net::Request& req) -> Result<void> = 0;
    virtual void onMessage(MessageHandler handler) = 0;
};
} // namespace cinder
