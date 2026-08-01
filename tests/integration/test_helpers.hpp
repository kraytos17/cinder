#pragma once

#include <asio.hpp>
#include <chrono>
#include <cstring>
#include <gtest/gtest.h>
#include <span>
#include <thread>

#include "cinder/net/protocol.hpp"

namespace cinder::net::test {

static auto
readResponse(asio::ip::tcp::socket& socket) -> Result<Response> {
    std::array<std::byte, 65'536> buf{};
    asio::error_code ec;
    (void)asio::read(socket, asio::buffer(buf.data(), K_FRAME_HEADER_SIZE), ec);
    if (ec) {
        return err<Response>(Error(Errc::InternalError, "read header failed"));
    }

    uint32_t net_len = 0;
    std::memcpy(&net_len, &buf[3], sizeof(net_len));
    size_t payload_len = std::byteswap(net_len);
    if (payload_len > buf.size() - K_FRAME_HEADER_SIZE) {
        return err<Response>(Error(Errc::InvalidArgument, "response too large"));
    }
    if (payload_len > 0) {
        (void)asio::read(socket,
            asio::buffer(buf.data() + K_FRAME_HEADER_SIZE, payload_len), ec);
        if (ec) {
            return err<Response>(Error(Errc::InternalError, "read payload failed"));
        }
    }
    return decodeResponse(
        std::span<const std::byte>(buf.data(), K_FRAME_HEADER_SIZE + payload_len));
}

static auto
waitForPort(int port, int max_retries = 50) -> bool {
    asio::io_context io;
    for (int i = 0; i < max_retries; i++) {
        asio::ip::tcp::socket sock(io);
        asio::error_code ec;
        sock.connect(asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), port), ec);
        if (!ec) {
            sock.close();
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
}

} // namespace cinder::net::test
