#include <asio.hpp>
#include <CLI/CLI.hpp>
#include <iostream>
#include <string>

#include "cinder/net/protocol.hpp"

using asio::ip::tcp;

static auto
connect(const std::string& host, uint16_t port) -> cinder::Result<tcp::socket> {
    asio::io_context io;
    tcp::socket socket(io);
    tcp::resolver resolver(io);
    asio::error_code ec;
    auto endpoints = resolver.resolve(host, std::to_string(port), ec);
    if (ec) {
        return cinder::err<tcp::socket>(
            cinder::Error(cinder::Errc::InternalError, "resolve failed"));
    }

    asio::connect(socket, endpoints, ec);
    if (ec) {
        return cinder::err<tcp::socket>(
            cinder::Error(cinder::Errc::InternalError, "connect failed"));
    }
    return cinder::ok(std::move(socket));
}

static auto
send_request(tcp::socket& socket, const cinder::net::Request& req)
    -> cinder::Result<cinder::net::Response> {

    auto encoded = cinder::net::encode(req);
    if (!encoded.has_value()) {
        return cinder::err<cinder::net::Response>(
            cinder::Error(cinder::Errc::InvalidArgument, "encode failed"));
    }

    asio::error_code ec;
    asio::write(socket, asio::buffer(encoded.value()), ec);
    if (ec) {
        return cinder::err<cinder::net::Response>(
            cinder::Error(cinder::Errc::InternalError, "write failed"));
    }

    std::array<std::byte, 4'096> buf;
    auto n = socket.read_some(asio::buffer(buf), ec);
    if (ec) {
        return cinder::err<cinder::net::Response>(
            cinder::Error(cinder::Errc::InternalError, "read failed"));
    }
    return cinder::net::decode_response(std::span<const std::byte>(buf.data(), n));
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
    auto sock_result = connect(host, port);
    if (!sock_result.has_value()) {
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

    auto res = send_request(socket, req);
    if (!res.has_value()) {
        std::cerr << "request failed\n";
        return 1;
    }

    auto& response = res.value();
    if (response.value.has_value()) {
        std::cout << response.value.value() << "\n";
    } else {
        std::cout << static_cast<int>(response.status) << "\n";
    }
    return 0;
}
