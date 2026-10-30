#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <fuzzer/FuzzedDataProvider.h>
#include <string>
#include <utility>
#include <vector>

#include "cinder/node/hint_queue.hpp"

namespace {

    using steady_clock = std::chrono::steady_clock;

    static auto makeHint(FuzzedDataProvider& fdp, steady_clock::time_point now)
        -> cinder::HintQueue::Hint {
        cinder::HintQueue::Hint hint;
        hint.target = fdp.ConsumeRandomLengthString(8);
        hint.req.opcode = cinder::net::Opcode::Replicate;
        hint.req.key = fdp.ConsumeRandomLengthString(16);
        hint.req.value = fdp.ConsumeRandomLengthString(64);
        hint.req.version = fdp.ConsumeIntegral<cinder::Version>();
        // Expiry around now: already-expired, imminent, and far-future.
        const int64_t delta = fdp.PickValueInArray(
            {int64_t{-1}, int64_t{0}, int64_t{1}, fdp.ConsumeIntegral<int64_t>() % 1'000'000}
        );
        hint.expires_at = now + std::chrono::milliseconds(delta);
        return hint;
    }

    static void reapModel(
        std::deque<cinder::HintQueue::Hint>& model,
        steady_clock::time_point now
    ) {
        std::deque<cinder::HintQueue::Hint> live;
        for (auto& h : model) {
            if (h.expires_at > now) {
                live.push_back(h);
            }
        }
        model = std::move(live);
    }

    // Snapshot the queue, check FIFO order + content against the model, and
    // return the snapshot for handle-driven follow-ups.
    static auto checkSnapshot(
        cinder::HintQueue& queue,
        std::deque<cinder::HintQueue::Hint>& model,
        steady_clock::time_point now
    ) -> std::vector<cinder::HintQueue::SnapshotEntry> {
        reapModel(model, now);
        auto snap = queue.snapshot(now);
        assert(snap.size() == model.size());
        assert(queue.size() == model.size());
        for (size_t k = 0; k < snap.size(); ++k) {
            assert(snap[k].hint.target == model[k].target);
            assert(snap[k].hint.req.key == model[k].req.key);
            assert(snap[k].hint.req.version == model[k].req.version);
        }
        return snap;
    }

    static void sequence(FuzzedDataProvider& fdp) {
        cinder::HintQueue queue;
        std::deque<cinder::HintQueue::Hint> model;
        auto now = steady_clock::time_point(std::chrono::milliseconds(1'000'000));

        const size_t n_ops = fdp.ConsumeIntegralInRange<size_t>(0, 128);
        for (size_t i = 0; i < n_ops; ++i) {
            const uint8_t op = fdp.ConsumeIntegralInRange<uint8_t>(0, 4);
            if (op == 0) {
                // Push: bounded at K_CAPACITY with drop-oldest FIFO eviction.
                auto hint = makeHint(fdp, now);
                (void)queue.push(hint); // copy: model keeps the original
                model.push_back(std::move(hint));
                if (model.size() > cinder::HintQueue::K_CAPACITY) {
                    model.pop_front();
                }
                assert(queue.size() == model.size());
            } else if (op == 1) {
                // Snapshot + variable drain (all / prefix / none) so handles
                // stay live across snapshots and slot reuse is exercised.
                auto snap = checkSnapshot(queue, model, now);
                const uint8_t drain = fdp.ConsumeIntegralInRange<uint8_t>(0, 2);
                size_t drop = 0;
                if (drain == 0) {
                    drop = snap.size();
                } else if (drain == 1) {
                    drop = snap.size() / 2;
                }
                for (size_t k = 0; k < drop; ++k) {
                    queue.remove(snap[k].handle);
                }
                model.erase(model.begin(), model.begin() + static_cast<ptrdiff_t>(drop));
                assert(queue.size() == model.size());
                // Re-removing a drained handle is a stale no-op.
                for (size_t k = 0; k < drop; ++k) {
                    queue.remove(snap[k].handle);
                }
                assert(queue.size() == model.size());
            } else if (op == 2) {
                // Provably-stale handle: snapshot first to learn live handles,
                // then remove with a bumped generation (no slot reuse happens
                // in between, so it cannot match) or an out-of-range index.
                // Both must be ignored — unlike a random generation, which can
                // legitimately match a low-numbered live generation.
                auto snap = checkSnapshot(queue, model, now);
                const size_t before = queue.size();
                if (!snap.empty() && fdp.ConsumeBool()) {
                    auto stale =
                        snap[fdp.ConsumeIntegralInRange<size_t>(0, snap.size() - 1)].handle;
                    ++stale.generation;
                    queue.remove(stale);
                } else {
                    queue.remove(
                        {cinder::HintQueue::K_CAPACITY
                             + fdp.ConsumeIntegralInRange<size_t>(0, 1024),
                         fdp.ConsumeIntegral<uint64_t>()}
                    );
                }
                assert(queue.size() == before);
            } else if (op == 3) {
                // Advance time: entries expire only when a snapshot reaps them;
                // size() alone must not drop anything.
                now += std::chrono::milliseconds(fdp.ConsumeIntegralInRange<int64_t>(0, 2'000'000));
                assert(queue.size() == model.size());
            } else {
                // Legacy tuple removal: removes the first matching
                // (target, key, version) hint, if any.
                cinder::net::Request req;
                req.opcode = cinder::net::Opcode::Replicate;
                req.key = fdp.ConsumeRandomLengthString(16);
                req.version = fdp.ConsumeIntegral<cinder::Version>();
                const std::string target = fdp.ConsumeRandomLengthString(8);
                queue.remove(target, req);
                // Mirror in the model: drop the first match.
                for (auto it = model.begin(); it != model.end(); ++it) {
                    if (it->target
                        == target
                        && it->req.key
                        == req.key
                        && it->req.version
                        == req.version) {
                        model.erase(it);
                        break;
                    }
                }
                assert(queue.size() == model.size());
            }
        }
    }

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 4) {
        return 0;
    }
    FuzzedDataProvider fdp(data, size);
    sequence(fdp);
    return 0;
}
