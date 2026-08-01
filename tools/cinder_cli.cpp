#include <asio.hpp>
#include <CLI/CLI.hpp>
#include <cstring>
#include <iostream>
#include <string>

#include "cinder/net/protocol.hpp"

using asio::ip::tcp;

static auto
statusString(cinder::Errc status) -> std::string_view {
    switch (status) {
        case cinder::Errc::OK:
            return "OK";
        case cinder::Errc::NotFound:
            return "(not found)";
        case cinder::Errc::CapacityExceeded:
            return "(capacity exceeded)";
        case cinder::Errc::InvalidArgument:
            return "(invalid argument)";
        case cinder::Errc::TtlExpired:
            return "(ttl expired)";
        case cinder::Errc::NotSupported:
            return "(not supported)";
        case cinder::Errc::InternalError:
            return "(internal error)";
        case cinder::Errc::Timeout:
            return "(timeout)";
        case cinder::Errc::NotReady:
            return "(not ready)";
    }
    return "(unknown)";
}

static auto
connect(asio::io_context& io, const std::string& host, uint16_t port)
    -> cinder::Result<tcp::socket> {
    tcp::socket socket(io);
    tcp::resolver resolver(io);
    asio::error_code ec;

    auto endpoints = resolver.resolve(host, std::to_string(port), ec);
    if (ec) {
        return cinder::err<tcp::socket>(
            cinder::Error(cinder::Errc::InternalError, "resolve failed"));
    }

    (void)asio::connect(socket, endpoints, ec);
    if (ec) {
        return cinder::err<tcp::socket>(
            cinder::Error(cinder::Errc::InternalError, "connect failed"));
    }
    return cinder::ok(std::move(socket));
}

static auto
sendRequest(tcp::socket& socket, const cinder::net::Request& req)
    -> cinder::Result<cinder::net::Response> {

    auto encoded = cinder::net::encode(req);
    if (!encoded.hasValue()) {
        return cinder::err<cinder::net::Response>(
            cinder::Error(cinder::Errc::InvalidArgument, "encode failed"));
    }

    asio::error_code ec;
    (void)asio::write(socket, asio::buffer(encoded.value()), ec);
    if (ec) {
        return cinder::err<cinder::net::Response>(
            cinder::Error(cinder::Errc::InternalError, "write failed"));
    }

    std::array<std::byte, 65'536> buf{};
    (void)asio::read(socket, asio::buffer(buf.data(), cinder::net::K_FRAME_HEADER_SIZE), ec);
    if (ec) {
        return cinder::err<cinder::net::Response>(
            cinder::Error(cinder::Errc::InternalError, "read header failed"));
    }

    uint32_t net_len = 0;
    std::memcpy(&net_len, &buf[3], sizeof(net_len));
    size_t payload_len = std::byteswap(net_len);
    if (payload_len > buf.size() - cinder::net::K_FRAME_HEADER_SIZE) {
        return cinder::err<cinder::net::Response>(
            cinder::Error(cinder::Errc::InvalidArgument, "response too large"));
    }
    if (payload_len > 0) {
        (void)asio::read(
            socket, asio::buffer(buf.data() + cinder::net::K_FRAME_HEADER_SIZE, payload_len), ec);
        if (ec) {
            return cinder::err<cinder::net::Response>(
                cinder::Error(cinder::Errc::InternalError, "read payload failed"));
        }
    }
    return cinder::net::decodeResponse(
        std::span<const std::byte>(buf.data(), cinder::net::K_FRAME_HEADER_SIZE + payload_len));
}

auto
main(int argc, char* argv[]) -> int {
    CLI::App app{"Cinder cache client"};

    std::string host = "127.0.0.1";
    uint16_t port = 7'000;
    app.add_option("--host", host, "Server host");
    app.add_option("-p,--port", port, "Server port");

    std::string cmd;
    std::string key;
    std::string value;
    int ttl_ms = 0;

    app.add_option("command", cmd, "get|set|del|ping")->required();
    app.add_option("key", key, "Key");
    app.add_option("value", value, "Value (set only)");
    app.add_option("--ttl", ttl_ms, "TTL in ms (set only)");

    CLI11_PARSE(app, argc, argv);

    asio::io_context io;
    auto sock_result = connect(io, host, port);
    if (!sock_result.hasValue()) {
        std::cerr << "connect failed\n";
        return 1;
    }
    auto& socket = sock_result.value();

    cinder::net::Request req;
    if (cmd == "get") {
        req.opcode = cinder::net::Opcode::Get;
        req.key = key;
    } else if (cmd == "set") {
        req.opcode = cinder::net::Opcode::Set;
        req.key = key;
        req.value = value;
        if (ttl_ms > 0) {
            req.ttl = std::chrono::milliseconds(ttl_ms);
        }
    } else if (cmd == "del") {
        req.opcode = cinder::net::Opcode::Del;
        req.key = key;
    } else if (cmd == "ping") {
        req.opcode = cinder::net::Opcode::Ping;
    } else {
        std::cerr << "unknown command: " << cmd << "\n";
        return 1;
    }

    auto res = sendRequest(socket, req);
    if (!res.hasValue()) {
        std::cerr << "request failed: " << res.error().message() << "\n";
        return 1;
    }

    auto& response = res.value();
    if (cmd == "ping") {
        std::cout << "pong\n";
    } else if (response.value.has_value()) {
        std::cout << response.value.value() << "\n";
    } else {
        std::cout << statusString(response.status) << "\n";
    }
    return 0;
}
