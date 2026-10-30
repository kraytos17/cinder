#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fuzzer/FuzzedDataProvider.h>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "cinder/cluster/clock.hpp"
#include "cinder/cluster/gossip.hpp"
#include "cinder/cluster/membership.hpp"
#include "cinder/cluster/transport.hpp"
#include "cinder/common/status.hpp"

namespace {

    // Deterministic clock: gossip tick seeds its RNG from the clock, so a fixed
    // clock keeps the whole path reproducible (no RealClock in fuzz).
    class FuzzClock final : public cinder::Clock {
    public:

        auto now() const -> std::chrono::steady_clock::time_point override {
            return std::chrono::steady_clock::time_point(std::chrono::milliseconds(1'000'000));
        }
    };

    class FuzzTransport final : public cinder::Transport {
    public:

        void sendAsync(
            const cinder::NodeId& /*to*/,
            const cinder::net::Request& /*req*/,
            SendCallback on_done
        ) override {
            on_done(cinder::ok());
        }

        void sendRequestAsync(
            const cinder::NodeId& /*to*/,
            const cinder::net::Request& /*req*/,
            RequestCallback on_done
        ) override {
            on_done(
                cinder::err<cinder::net::Response>(
                    cinder::Error(cinder::Errc::NotReady, "fuzz stub")
                )
            );
        }

        void onMessage(MessageHandler /*handler*/) override {}
    };

    // Incarnations stay clear of u64max so the refutation bump (max+1) cannot
    // wrap and the model stays exact.
    constexpr uint64_t K_MAX_INCARNATION = uint64_t{1} << 62U;

    struct ExpectedNode {
        std::string host;
        uint16_t port = 0;
        cinder::NodeState state = cinder::NodeState::Alive;
        uint64_t incarnation = 0;
        bool known = false;
    };

    static auto makeRumor(FuzzedDataProvider& fdp, const std::string& id, uint64_t known_inc)
        -> cinder::NodeInfo {
        cinder::NodeInfo rumor;
        rumor.id = id;
        rumor.host = fdp.ConsumeRandomLengthString(16);
        rumor.port = fdp.ConsumeIntegral<uint16_t>();
        rumor.state = fdp.PickValueInArray(
            {cinder::NodeState::Alive, cinder::NodeState::Suspect, cinder::NodeState::Dead}
        );
        // Bias toward guard boundaries (equal, ±1) with occasional jumps.
        switch (fdp.ConsumeIntegralInRange<uint8_t>(0, 3)) {
            case 0:
                rumor.incarnation = known_inc;
                break;
            case 1:
                rumor.incarnation = known_inc + 1;
                break;
            case 2:
                rumor.incarnation = known_inc == 0 ? 0 : known_inc - 1;
                break;
            default:
                rumor.incarnation = fdp.ConsumeIntegralInRange<uint64_t>(0, K_MAX_INCARNATION);
                break;
        }
        return rumor;
    }

    static void checkMatches(
        const cinder::MembershipTable& table,
        const std::string& id,
        const ExpectedNode& exp
    ) {
        auto got = table.get(id);
        if (!exp.known) {
            assert(!got.has_value());
            return;
        }
        assert(got.has_value());
        assert(got->host == exp.host);
        assert(got->port == exp.port);
        assert(got->state == exp.state);
        assert(got->incarnation == exp.incarnation);
    }

    // Incarnation-guard model: the table applies a rumor iff its incarnation is
    // not stale, with sticky host/port (filled only when empty) and Alive-only
    // insert for unknown ids. Self-rumors refute by bumping past the rumor.
    // Any deviation is a membership bug, not fuzzer noise.
    static void guardModel(FuzzedDataProvider& fdp) {
        cinder::MembershipTable table("fuzz-self");
        // Self exists from construction (Alive/0).
        std::vector<std::string> ids = {"fuzz-self"};
        // Unique non-empty pool: the model is slot-keyed while the table is
        // id-keyed, so duplicate (or empty) ids would desync them — a harness
        // artifact, not a membership bug. Raw empty-id strings are still
        // covered by the quiescence and parse modes below.
        const size_t extra = fdp.ConsumeIntegralInRange<size_t>(1, 3);
        for (size_t i = 0; i < extra; ++i) {
            auto candidate = fdp.ConsumeRandomLengthString(8);
            if (candidate.empty() || candidate == "fuzz-self") {
                candidate = "node" + std::to_string(ids.size());
            }
            if (std::find(ids.begin(), ids.end(), candidate) == ids.end()) {
                ids.push_back(std::move(candidate));
            }
        }
        // Model mirrors table state for every known id.
        std::vector<ExpectedNode> model(ids.size());
        model[0].known = true; // self: Alive/0/""/0

        const size_t n = fdp.ConsumeIntegralInRange<size_t>(1, 32);
        for (size_t i = 0; i < n; ++i) {
            const size_t idx = fdp.ConsumeIntegralInRange<size_t>(0, ids.size() - 1);
            auto rumor = makeRumor(fdp, ids[idx], model[idx].incarnation);
            table.applyRumor("fuzz-from", rumor);

            ExpectedNode& exp = model[idx];
            const bool is_self = ids[idx] == "fuzz-self";
            if (!exp.known) {
                if (rumor.state == cinder::NodeState::Alive) {
                    exp = {rumor.host, rumor.port, rumor.state, rumor.incarnation, true};
                }
            } else if (rumor.incarnation < exp.incarnation) {
                // Stale — ignored.
            } else if (rumor.incarnation == exp.incarnation && rumor.state == exp.state) {
                // No change.
            } else if (is_self && rumor.state != cinder::NodeState::Alive) {
                // Refutation: bump past the rumor, force Alive. Caller only
                // reaches here with rumor.inc >= local.inc (stale returns early).
                if (exp.incarnation <= rumor.incarnation) {
                    exp.incarnation = std::max(exp.incarnation, rumor.incarnation) + 1;
                }
                exp.state = cinder::NodeState::Alive;
            } else {
                exp.state = rumor.state;
                exp.incarnation = rumor.incarnation;
                if (exp.host.empty()) {
                    exp.host = rumor.host;
                }
                if (exp.port == 0) {
                    exp.port = rumor.port;
                }
            }
            checkMatches(table, ids[idx], exp);
        }
    }

    static auto snapKey(const cinder::NodeInfo& info) -> std::string {
        return info.id
            + '\x00'
            + info.host
            + '\x00'
            + std::to_string(info.port)
            + '\x00'
            + std::to_string(std::to_underlying(info.state))
            + '\x00'
            + std::to_string(info.incarnation);
    }

    // Quiescence: applying the same gossip view repeatedly must converge —
    // snapshot after the 2nd apply equals snapshot after the 3rd (re-apply of
    // known-or-stale rumors changes nothing; self-refutation already bumped
    // past). joined_at is wall-observed, so it is excluded from the comparison.
    static void quiescence(FuzzedDataProvider& fdp) {
        cinder::MembershipTable table("fuzz-self");
        FuzzTransport transport;
        FuzzClock clock;
        cinder::GossipManager
            gm(clock, transport, table, "fuzz-self", std::chrono::milliseconds(1000));

        cinder::net::Request req;
        req.opcode = cinder::net::Opcode::Gossip;
        const std::string from = fdp.ConsumeRandomLengthString(8);
        req.value = fdp.ConsumeRemainingBytesAsString();
        // Drive past convergence, then compare two consecutive applies.
        gm.handleMessage(from, req);
        gm.handleMessage(from, req);
        std::vector<std::string> second;
        for (const auto& info : table.snapshot()) {
            second.push_back(snapKey(info));
        }
        gm.handleMessage(from, req);
        std::vector<std::string> third;
        for (const auto& info : table.snapshot()) {
            third.push_back(snapKey(info));
        }
        assert(second == third);
    }

    static void parseRaw(FuzzedDataProvider& fdp) {
        // Arbitrary bytes: never crash, deterministic, and a successful parse
        // carries a fully-formed entry (port/state/incarnation all validated).
        auto bytes = fdp.ConsumeRemainingBytesAsString();
        std::string_view input(bytes);
        cinder::NodeInfo first;
        cinder::NodeInfo second;
        const bool ok1 = cinder::gossip::parseEntry(input, first);
        const bool ok2 = cinder::gossip::parseEntry(input, second);
        assert(ok1 == ok2);
        if (ok1) {
            assert(snapKey(first) == snapKey(second));
        }
    }

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 4) {
        return 0;
    }
    FuzzedDataProvider fdp(data, size);
    switch (fdp.ConsumeIntegralInRange<uint8_t>(0, 2)) {
        case 0:
            guardModel(fdp);
            break;
        case 1:
            quiescence(fdp);
            break;
        default:
            parseRaw(fdp);
            break;
    }
    return 0;
}
