#pragma once

#include <cstdlib>
#include <string>

namespace cinder::tools {

    // Resolve the client authentication secret for CLI tools: an explicit
    // `--client-secret` flag wins, otherwise the `CINDER_CLIENT_SECRET`
    // environment variable, otherwise empty (client auth disabled).
    inline auto resolveClientSecret(const std::string& flag_value) -> std::string {
        if (!flag_value.empty()) {
            return flag_value;
        }
        // NOLINTNEXTLINE(concurrency-mt-unsafe): single-threaded startup
        if (const char* env = std::getenv("CINDER_CLIENT_SECRET")) {
            return env;
        }

        return {};
    }

} // namespace cinder::tools
