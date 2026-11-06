#pragma once

// Invertible Bloom lookup table.
// One self-contained unit: item/cell/sketch types, wire constants, the
// hash/map/peel core, and the BufferWriter/BufferReader framing. Sketches
// are per-bucket, so the bucket tag lives outside the item and is applied
// by the caller when converting a difference back to metadata rows.
//
// Wire layout is frozen (K_IBLT_MAGIC "AEI4", 6 x u64 per cell); the
// anti-entropy exchange relies on byte-identical encode output.

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace cinder::iblt {

    // One IBLT item: the full LWW identity tuple, so two versions of one
    // key stay distinct until peeling.
    struct Item {
        uint64_t key_hash = 0;
        uint64_t version = 0;
        uint64_t writer = 0;
        bool tombstone = false;
    };

    struct Cell {
        int64_t count = 0;
        uint64_t key_hash_xor = 0;
        uint64_t version_xor = 0;
        uint64_t writer_xor = 0;
        uint64_t tombstone_xor = 0;
        uint64_t checksum_xor = 0;
    };

    struct Sketch {
        std::vector<Cell> cells;
    };

    struct Diff {
        std::vector<Item> remote_only;
        std::vector<Item> local_only;
    };

    inline constexpr uint32_t K_MAGIC = 0x41454934; // "AEI4"
    inline constexpr size_t K_HASH_COUNT = 3;
    inline constexpr size_t K_MAX_CELLS = 4096;
    inline constexpr size_t K_DEFAULT_CELLS = 64;
    // Serialized cell width: count u64 + 5 x u64 sums.
    inline constexpr size_t K_CELL_BYTES = 6 * sizeof(uint64_t);

    [[nodiscard]]
    auto build(const std::vector<Item>& rows, size_t cell_count) -> std::optional<Sketch>;

    // Single-item insert into an existing sketch (streaming builds, e.g. the
    // bucket-refinement responder, which switches from row sampling to a
    // sketch mid-scan).
    void insert(
        Sketch& sketch,
        uint64_t key_hash,
        uint64_t version,
        uint64_t writer,
        bool tombstone
    );

    [[nodiscard]]
    auto encode(const Sketch& sketch) -> std::string;

    [[nodiscard]]
    auto decode(std::string_view data) -> std::optional<Sketch>;

    [[nodiscard]]
    auto difference(const Sketch& remote, const Sketch& local) -> std::optional<Diff>;
} // namespace cinder::iblt
