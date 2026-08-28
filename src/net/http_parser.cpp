#include "cinder/net/http_parser.hpp"

namespace cinder::net {

auto
parseHttpRequest(std::string_view raw) -> std::optional<HttpRequest> {
    // Expect "METHOD PATH HTTP/x.y\r\n..."
    auto first_space = raw.find(' ');
    if (first_space == std::string_view::npos) {
        return std::nullopt;
    }

    auto second_space = raw.find(' ', first_space + 1);
    if (second_space == std::string_view::npos) {
        return std::nullopt;
    }

    auto crlf = raw.find("\r\n", second_space);
    if (crlf == std::string_view::npos) {
        return std::nullopt;
    }

    HttpRequest req;
    req.method = std::string(raw.substr(0, first_space));
    req.path = std::string(raw.substr(first_space + 1, second_space - first_space - 1));
    req.version = std::string(raw.substr(second_space + 1, crlf - second_space - 1));
    return req;
}

auto
formatHttpResponse(std::string_view body) -> std::string {
    std::string resp;
    resp += "HTTP/1.0 200 OK\r\n";
    resp += "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n";
    resp += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    resp += "Connection: close\r\n";
    resp += "\r\n";
    resp += body;
    return resp;
}

auto
formatHttp404() -> std::string {
    constexpr std::string_view BODY = "404 Not Found";
    std::string resp;
    resp += "HTTP/1.0 404 Not Found\r\n";
    resp += "Content-Type: text/plain\r\n";
    resp += "Content-Length: " + std::to_string(BODY.size()) + "\r\n";
    resp += "Connection: close\r\n";
    resp += "\r\n";
    resp += BODY;
    return resp;
}
} // namespace cinder::net
