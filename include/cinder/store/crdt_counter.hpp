#pragma once

// State-based CRDT counters (pure, header-only: no I/O, no locks, no clocks).
//
// GCounter: grow-only counter. Each actor owns one monotonically increasing
// component; the value is the sum over actors. Merge is the pointwise
// maximum, so replicas converge regardless of delivery order or duplication
// (commutative, associative, idempotent).
//
// PNCounter: increment/decrement counter built from two GCounters, one for
// increments (P) and one for decrements (N). The value is sum(P) - sum(N).
// The internal state only grows even though the exposed value can go down,
// which is what makes concurrent increments and decrements both survive a
// merge — Cinder's LWW (version, writer) order would drop one of them.
//
// Actor identity is the caller's responsibility: pass a stable per-replica
// NodeId (the same string a node always uses). A restart that mints a fresh
// identity orphans its old components, growing state without reusing it.
//
// These types are deliberately storage- and transport-agnostic: merging here
// is in-memory only. Replication, anti-entropy, WAL, and snapshots must call
// merge() explicitly — routing counter bytes through the LWW putVersioned
// path would reintroduce last-writer-wins and lose concurrent updates.
//
// Overflow policy: component arithmetic is checked. An increment that would
// overflow the actor's component fails with CapacityExceeded and leaves the
// state unchanged. value() saturates instead of wrapping: GCounter clamps at
// uint64 max, PNCounter clamps at int64 min/max.

#include <cstdint>
#include <limits>
#include <map>
#include <string>

#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"

namespace cinder::crdt {

    class GCounter {
    public:

        GCounter() = default;

        // Add delta to this actor's component. Empty actor ids are rejected
        // (identity must be stable and attributable); delta 0 is a no-op.
        auto increment(const NodeId& actor, uint64_t delta = 1) -> Result<void> {
            if (actor.empty()) {
                return err(Error(Errc::InvalidArgument, "counter actor must not be empty"));
            }
            if (delta == 0) {
                return ok();
            }
            const auto it = components_.find(actor);
            const uint64_t current = it == components_.end() ? 0 : it->second;
            if (current > std::numeric_limits<uint64_t>::max() - delta) {
                return err(Error(Errc::CapacityExceeded, "counter component overflow"));
            }
            components_[actor] = current + delta;
            return ok();
        }

        // Pointwise maximum: every component keeps the highest value seen.
        void merge(const GCounter& other) {
            for (const auto& [actor, count] : other.components_) {
                auto& slot = components_[actor];
                if (count > slot) {
                    slot = count;
                }
            }
        }

        // Sum over actors, saturating at uint64 max.
        [[nodiscard]]
        auto value() const -> uint64_t {
            uint64_t total = 0;
            for (const auto& entry : components_) {
                const uint64_t count = entry.second;
                if (count > std::numeric_limits<uint64_t>::max() - total) {
                    return std::numeric_limits<uint64_t>::max();
                }
                total += count;
            }
            return total;
        }

        // This actor's component, 0 when the actor has never incremented.
        [[nodiscard]]
        auto component(const NodeId& actor) const -> uint64_t {
            if (auto it = components_.find(actor); it != components_.end()) {
                return it->second;
            }
            return 0;
        }

        [[nodiscard]]
        auto componentCount() const -> size_t {
            return components_.size();
        }

        [[nodiscard]]
        auto empty() const -> bool {
            return components_.empty();
        }

        [[nodiscard]]
        auto operator==(const GCounter& other) const -> bool {
            return components_ == other.components_;
        }

    private:

        // std::map (not unordered): deterministic iteration order keeps
        // future serialization byte-identical across replicas and runs.
        std::map<NodeId, uint64_t> components_;
    };

    class PNCounter {
    public:

        PNCounter() = default;

        auto increment(const NodeId& actor, uint64_t delta = 1) -> Result<void> {
            return positive_.increment(actor, delta);
        }

        auto decrement(const NodeId& actor, uint64_t delta = 1) -> Result<void> {
            return negative_.increment(actor, delta);
        }

        void merge(const PNCounter& other) {
            positive_.merge(other.positive_);
            negative_.merge(other.negative_);
        }

        // sum(P) - sum(N), clamped to the int64 range. Both sums saturate at
        // uint64 max first, so a saturated side still yields a bounded value
        // instead of wrapping.
        [[nodiscard]]
        auto value() const -> int64_t {
            const uint64_t pos = positive_.value();
            const uint64_t neg = negative_.value();
            if (pos >= neg) {
                const uint64_t diff = pos - neg;
                if (diff > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                    return std::numeric_limits<int64_t>::max();
                }
                return static_cast<int64_t>(diff);
            }
            const uint64_t diff = neg - pos;
            const uint64_t limit = static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1U;
            if (diff >= limit) {
                return std::numeric_limits<int64_t>::min();
            }
            return -static_cast<int64_t>(diff);
        }

        [[nodiscard]]
        auto positive() const -> const GCounter& {
            return positive_;
        }

        [[nodiscard]]
        auto negative() const -> const GCounter& {
            return negative_;
        }

        [[nodiscard]]
        auto empty() const -> bool {
            return positive_.empty() && negative_.empty();
        }

        [[nodiscard]]
        auto operator==(const PNCounter& other) const -> bool {
            return positive_ == other.positive_ && negative_ == other.negative_;
        }

    private:

        GCounter positive_;
        GCounter negative_;
    };

} // namespace cinder::crdt
