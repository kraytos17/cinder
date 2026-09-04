#include <cinder/cluster/gossip.hpp>
#include <cinder/cluster/membership.hpp>
#include <cstddef>
#include <cstdint>
#include <string_view>

// Stub transport for fuzzing decodeView via handleMessage.
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
    std::string_view input(reinterpret_cast<const char*>(data), size);
    // Selector byte: 0 = parseEntry, 1 = handleMessage (full decodeView path)
    if (size < 1) {
        return 0;
    }

    uint8_t selector = data[0];
    std::string_view payload(input.data() + 1, input.size() - 1);
    if (selector % 2 == 0) {
        // Fuzz parseEntry directly — must not crash on any input.
        cinder::NodeInfo info;
        (void)cinder::gossip::parseEntry(payload, info);
    } else {
        // Fuzz the full gossip path: handleMessage → decodeView → parseEntry
        // → applyRumor. Exercises the semicolon-delimited multi-entry parsing.
        cinder::MembershipTable table("fuzz-self");
        FuzzTransport transport;
        cinder::RealClock clock;
        cinder::GossipManager gm(
            clock, transport, table, "fuzz-self", std::chrono::milliseconds(1'000));

        cinder::net::Request req;
        req.opcode = cinder::net::Opcode::Gossip;
        req.value = std::string(payload);
        gm.handleMessage("fuzz-peer", req);
    }
    return 0;
}
