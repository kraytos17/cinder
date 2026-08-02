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
    using SendCallback = std::move_only_function<void(Result<void>)>;

    // Non-blocking: initiates an async send to `to` and invokes `on_done` on
    // the io thread when the peer has acknowledged (or failed). Never blocks.
    virtual void sendAsync(const NodeId& to, const net::Request& req, SendCallback on_done) = 0;
    virtual void onMessage(MessageHandler handler) = 0;
};
} // namespace cinder
