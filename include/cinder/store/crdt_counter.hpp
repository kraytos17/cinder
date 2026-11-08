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

#include <algorithm>
#include <btree/map.h>
#include <cstdint>
#include <limits>
#include <optional>
#include <string_view>
#include <vector>

#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"
#include "cinder/store/detail/io_utils.hpp"

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
                slot = std::max(count, slot);
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

        // Sorted component view, backing the deterministic wire encoding
        [[nodiscard]]
        auto components() const -> const btree::map<NodeId, uint64_t>& {
            return components_;
        }

        [[nodiscard]]
        auto componentCount() const -> long {
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

        btree::map<NodeId, uint64_t> components_;
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

        // Side-level merges: apply one decoded or replicated side without
        // touching the other. Used by the wire decoder below; later by
        // replication and anti-entropy when only one side arrives.
        void mergePositive(const GCounter& side) {
            positive_.merge(side);
        }

        void mergeNegative(const GCounter& side) {
            negative_.merge(side);
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

    // Deterministic wire encoding (native-endian, like the WAL/snapshot
    // layout; the cluster is homogeneous). btree::map iterates in key
    // order, so encoding is byte-identical across replicas and runs:
    //
    //   magic u32 ("CRDC") | kind u8 (1 = G, 2 = PN)
    //   GCounter: count u32 | count × (actor_len u32, actor bytes, value u64)
    //   PNCounter: the GCounter body for P, then the GCounter body for N
    //   (each with its own count; no second magic/kind).
    //
    // Decode is strict: wrong magic/kind, empty actor ids, a count that
    // cannot fit in the remaining bytes, truncation, or trailing garbage
    // all fail. An empty string return from encode signals an unencodable
    // counter (component count or actor length past u32); a valid encoding
    // is never empty.
    inline constexpr uint32_t K_CRDT_COUNTER_MAGIC = 0x43524443; // "CRDC"
    inline constexpr uint8_t K_CRDT_GCOUNTER_KIND = 1;
    inline constexpr uint8_t K_CRDT_PNCOUNTER_KIND = 2;

    namespace wire {
        // Minimum bytes per component entry: actor_len u32 + value u64
        // (actor itself may be any non-empty length).
        inline constexpr size_t K_MIN_COMPONENT_BYTES = sizeof(uint32_t) + sizeof(uint64_t);

        [[nodiscard]]
        inline auto encodeComponents(detail::BufferWriter& out, const GCounter& counter, long count)
            -> bool {
            if (count < 0 || static_cast<uint64_t>(count) > std::numeric_limits<uint32_t>::max()) {
                return false;
            }

            out.write(static_cast<uint32_t>(count));
            // components() exposes the map in key order; keep the loop here
            // (not a GCounter method) so the value type stays map-agnostic.
            for (const auto& entry : counter.components()) {
                const std::string& actor = entry.first;
                if (actor.empty() || actor.size() > std::numeric_limits<uint32_t>::max()) {
                    return false;
                }

                out.write(static_cast<uint32_t>(actor.size()));
                out.writeString(actor);
                out.write(entry.second);
            }
            return true;
        }

        [[nodiscard]]
        inline auto decodeComponents(detail::BufferReader& in, GCounter& counter) -> bool {
            uint32_t count = 0;
            if (!in.read(count)) {
                return false;
            }
            // Bound the loop by the remaining bytes before allocating
            // anything: every entry costs at least K_MIN_COMPONENT_BYTES, so
            // a lying count fails here instead of spinning.
            if (static_cast<uint64_t>(count) > in.remaining() / K_MIN_COMPONENT_BYTES) {
                return false;
            }
            for (uint32_t i = 0; i < count; ++i) {
                uint32_t len = 0;
                std::string actor;
                uint64_t value = 0;
                if (!in.read(len)
                    || !in.readString(len, actor)
                    || actor.empty()
                    || !in.read(value)) {
                    return false;
                }
                // Increment-from-empty reconstructs the component exactly
                // (0 + value always fits) and reuses the empty-actor guard.
                if (!counter.increment(actor, value).has_value()) {
                    return false;
                }
            }
            return true;
        }
    } // namespace wire

    [[nodiscard]]
    inline auto encodeCounter(const GCounter& counter) -> std::string {
        const long count = counter.componentCount();
        // Header + count + per-entry worst case; BufferWriter asserts fits,
        // so pre-size exactly: entries are variable-length (actor ids).
        std::vector<std::byte> buf;
        buf.resize(sizeof(uint32_t) + sizeof(uint8_t) + sizeof(uint32_t));
        size_t need = buf.size();
        for (const auto& entry : counter.components()) {
            need += sizeof(uint32_t) + entry.first.size() + sizeof(uint64_t);
        }

        buf.resize(need);
        detail::BufferWriter out(buf);
        out.write(K_CRDT_COUNTER_MAGIC);
        out.write(K_CRDT_GCOUNTER_KIND);
        if (!wire::encodeComponents(out, counter, count)) {
            return {};
        }
        return detail::toWireString(buf);
    }

    [[nodiscard]]
    inline auto decodeGCounter(std::string_view data) -> std::optional<GCounter> {
        detail::BufferReader in(data);
        uint32_t magic = 0;
        uint8_t kind = 0;
        GCounter counter;
        if (!in.read(magic)
            || magic
            != K_CRDT_COUNTER_MAGIC
            || !in.read(kind)
            || kind
            != K_CRDT_GCOUNTER_KIND
            || !wire::decodeComponents(in, counter)
            || in.remaining()
            != 0) {
            return std::nullopt;
        }
        return counter;
    }

    [[nodiscard]]
    inline auto encodeCounter(const PNCounter& counter) -> std::string {
        const long pos_count = counter.positive().componentCount();
        const long neg_count = counter.negative().componentCount();
        std::vector<std::byte> buf;

        buf.resize(sizeof(uint32_t) + sizeof(uint8_t));
        size_t need = buf.size();
        for (const GCounter* side : {&counter.positive(), &counter.negative()}) {
            need += sizeof(uint32_t);
            for (const auto& entry : side->components()) {
                need += sizeof(uint32_t) + entry.first.size() + sizeof(uint64_t);
            }
        }

        buf.resize(need);
        detail::BufferWriter out(buf);
        out.write(K_CRDT_COUNTER_MAGIC);
        out.write(K_CRDT_PNCOUNTER_KIND);
        if (!wire::encodeComponents(out, counter.positive(), pos_count)
            || !wire::encodeComponents(out, counter.negative(), neg_count)) {
            return {};
        }
        return detail::toWireString(buf);
    }

    [[nodiscard]]
    inline auto decodePNCounter(std::string_view data) -> std::optional<PNCounter> {
        detail::BufferReader in(data);
        uint32_t magic = 0;
        uint8_t kind = 0;
        if (!in.read(magic)
            || magic
            != K_CRDT_COUNTER_MAGIC
            || !in.read(kind)
            || kind
            != K_CRDT_PNCOUNTER_KIND) {
            return std::nullopt;
        }

        // Decode into temporaries so a corrupt N body cannot leave a
        // half-merged P side behind.
        GCounter pos;
        GCounter neg;
        if (!wire::decodeComponents(in, pos)
            || !wire::decodeComponents(in, neg)
            || in.remaining()
            != 0) {
            return std::nullopt;
        }

        PNCounter result;
        result.mergePositive(pos);
        result.mergeNegative(neg);
        return result;
    }
} // namespace cinder::crdt
