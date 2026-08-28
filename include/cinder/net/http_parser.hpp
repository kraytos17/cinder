#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace cinder::net {

struct HttpRequest {
    std::string method;
    std::string path;
    std::string version;
};

// Parse a minimal HTTP request (GET only, no body). Returns std::nullopt on
// malformed input.
auto
parseHttpRequest(std::string_view raw) -> std::optional<HttpRequest>;

// Format an HTTP 200 response with text/plain; version=1.0.
auto
formatHttpResponse(std::string_view body) -> std::string;

// Format an HTTP 404 response.
auto
formatHttp404() -> std::string;
} // namespace cinder::net
