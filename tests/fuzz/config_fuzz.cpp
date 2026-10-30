#include <cassert>
#include <cstddef>
#include <cstdint>
#include <fuzzer/FuzzedDataProvider.h>
#include <string>
#include <vector>

#include "cinder/common/config.hpp"
#include "cinder/node/cache_node_server.hpp"

namespace {

    // parsePeer contract: true implies non-empty id and host (the parser
    // rejects empty id, empty host, missing port, and out-of-range ports).
    // Deterministic by construction (pure string function).
    static void peer(FuzzedDataProvider& fdp) {
        auto input = fdp.ConsumeRemainingBytesAsString();
        cinder::ClusterConfig::NodeConfig first;
        cinder::ClusterConfig::NodeConfig second;
        const bool ok1 = cinder::parsePeer(input, first);
        const bool ok2 = cinder::parsePeer(input, second);
        assert(ok1 == ok2);
        if (ok1) {
            assert(!first.id.empty());
            assert(!first.host.empty());
            assert(first.id == second.id);
            assert(first.host == second.host);
            assert(first.port == second.port);
        }
    }

    // parsePeersString appends valid entries and returns their count: the
    // return value must equal the number of appended entries, each with a
    // non-empty id and host.
    static void peers(FuzzedDataProvider& fdp) {
        // Mix well-formed entries with raw junk so both the accept and reject
        // paths interleave, comma-separated like the real flag value.
        std::string input;
        const size_t parts = fdp.ConsumeIntegralInRange<size_t>(0, 8);
        for (size_t i = 0; i < parts; ++i) {
            if (i > 0) {
                input.push_back(',');
            }
            if (fdp.ConsumeBool()) {
                input += fdp.ConsumeRandomLengthString(8);
                input.push_back('@');
                input += fdp.ConsumeRandomLengthString(8);
                input.push_back(':');
                input += fdp.ConsumeRandomLengthString(8);
            } else {
                input += fdp.ConsumeRandomLengthString(16);
            }
        }

        cinder::Config cfg;
        const size_t count = cinder::parsePeersString(input, cfg);
        assert(count == cfg.peers.size());
        for (const auto& nc : cfg.peers) {
            assert(!nc.id.empty());
            assert(!nc.host.empty());
        }
    }

    // Total functions and self-consistency: log levels never crash,
    // diffConfig of a config with itself is empty, formatters never crash.
    static void misc(FuzzedDataProvider& fdp) {
        (void)cinder::logLevelFromString(fdp.ConsumeRemainingBytesAsString());

        cinder::Config cfg;
        cfg.node_id = fdp.ConsumeRandomLengthString(16);
        cfg.log_level = fdp.ConsumeRandomLengthString(8);
        const size_t n_peers = fdp.ConsumeIntegralInRange<size_t>(0, 4);
        for (size_t i = 0; i < n_peers; ++i) {
            cinder::ClusterConfig::NodeConfig nc;
            nc.id = fdp.ConsumeRandomLengthString(8);
            nc.host = fdp.ConsumeRandomLengthString(8);
            nc.port = fdp.ConsumeIntegral<uint16_t>();
            cfg.peers.push_back(std::move(nc));
        }
        assert(cinder::diffConfig(cfg, cfg).empty());
        (void)cinder::formatConfigJson(cfg); // NOLINT
        (void)cinder::formatClusterJson({}); // NOLINT
    }

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 2) {
        return 0;
    }
    FuzzedDataProvider fdp(data, size);
    switch (fdp.ConsumeIntegralInRange<uint8_t>(0, 2)) {
        case 0:
            peer(fdp);
            break;
        case 1:
            peers(fdp);
            break;
        default:
            misc(fdp);
            break;
    }
    return 0;
}
