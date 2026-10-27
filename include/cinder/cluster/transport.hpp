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

        using MessageHandler =
            std::move_only_function<void(const NodeId& from, const net::Request&)>;
        using SendCallback = std::move_only_function<void(Result<void>)>;
        using RequestCallback = std::move_only_function<void(Result<net::Response>)>;

        // Non-blocking: initiates an async send to `to` and invokes `on_done` on
        // the io thread when the peer has acknowledged (or failed). Never blocks.
        // The callback fires exactly once per call, with either success or a
        // terminal failure (including timeout/cancel). fanOutQuorum ignores
        // duplicate deliveries defensively, but exactly-once is the contract.
        virtual void sendAsync(const NodeId& to, const net::Request& req, SendCallback on_done) = 0;

        // Non-blocking request-response: sends `req` and invokes `on_done` with the
        // peer's decoded Response. Used for GetVersioned fan-out (read repair).
        // Same exactly-once callback contract as sendAsync.
        virtual void sendRequestAsync(
            const NodeId& to,
            const net::Request& req,
            RequestCallback on_done
        ) = 0;

        virtual void onMessage(MessageHandler handler) = 0;
    };
} // namespace cinder
