#include <cinder/node/anti_entropy.hpp>
#include <cinder/store/lru_store.hpp>
#include <cstddef>
#include <cstdint>
#include <string_view>

// Stub transport for fuzzing — accepts any request and responds with errors.
// We only need the transport to exist for the AntiEntropyManager constructor;
// the fuzz targets call the static decode/apply functions directly.
class FuzzTransport final : public cinder::Transport {
  public:

    void sendAsync(const cinder::NodeId& /*to*/, const cinder::net::Request& /*req*/,
        SendCallback on_done) override {
        on_done(cinder::ok());
    }

    void sendRequestAsync(const cinder::NodeId& /*to*/, const cinder::net::Request& /*req*/,
        RequestCallback on_done) override {
        on_done(
            cinder::err<cinder::net::Response>(cinder::Error(cinder::Errc::NotReady, "fuzz stub")));
    }

    void onMessage(MessageHandler /*handler*/) override {}
};

extern "C" int
LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 4) {
        return 0;
    }

    std::string_view input(reinterpret_cast<const char*>(data), size);
    // Split fuzz data: first byte = selector, rest = payload.
    // Selector determines which decode/apply function to call.
    uint8_t selector = data[0];
    std::string_view payload(input.data() + 1, input.size() - 1);
    switch (selector % 5) {
        case 0: {
            // Fuzz decodeDigest with various bucket counts.
            uint32_t buckets = 1 + (size % 32);
            (void)cinder::AntiEntropyManager::decodeDigest(payload, buckets);
            break;
        }
        case 1: {
            // Fuzz decodeBucketIds.
            (void)cinder::AntiEntropyManager::decodeBucketIds(payload);
            break;
        }
        case 2: {
            // Fuzz applyEntries against a real store.
            cinder::LruStore store(1'024);
            FuzzTransport transport;
            cinder::ConsistentHashRing ring;
            cinder::RealClock clock;
            cinder::AntiEntropyManager mgr(store, ring, "fuzz-self", clock, transport, 8);
            (void)mgr.applyEntries(payload);
            break;
        }
        case 3: {
            // Fuzz onSyncRequest: construct a fake Request with the payload
            // as the value, then call the handler.
            cinder::LruStore store(1'024);
            FuzzTransport transport;
            cinder::ConsistentHashRing ring;
            cinder::RealClock clock;
            cinder::AntiEntropyManager mgr(store, ring, "fuzz-self", clock, transport, 8);
            cinder::net::Request req;
            req.opcode = cinder::net::Opcode::AntiEntropySync;
            req.value = std::string(payload);
            mgr.onSyncRequest("fuzz-peer", req, [](cinder::net::Response) {});
            break;
        }
        case 4: {
            // Fuzz onDigestRequest: construct a fake digest request.
            cinder::LruStore store(1'024);
            FuzzTransport transport;
            cinder::ConsistentHashRing ring;
            cinder::RealClock clock;
            cinder::AntiEntropyManager mgr(store, ring, "fuzz-self", clock, transport, 8);
            cinder::net::Request req;
            req.opcode = cinder::net::Opcode::AntiEntropyDigest;
            req.value = std::string(payload);
            mgr.onDigestRequest("fuzz-peer", req, [](cinder::net::Response) {});
            break;
        }
        default:
            break;
    }
    return 0;
}
