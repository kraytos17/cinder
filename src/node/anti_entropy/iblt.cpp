#include "cinder/node/anti_entropy/iblt.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <vector>
#include <xxhash.h>

#include "cinder/store/detail/io_utils.hpp"

namespace cinder {
    namespace {

        using detail::BufferReader;
        using detail::BufferWriter;
        using detail::toWireString;

        auto itemChecksum(uint64_t key_hash, uint64_t version, uint64_t writer, bool tombstone)
            -> uint64_t {
            std::array<uint64_t, 4> words{key_hash, version, writer, tombstone ? 1U : 0U};
            return XXH3_64bits(words.data(), sizeof(words));
        }

        auto positions(uint64_t hash, size_t count) -> std::array<size_t, iblt::K_HASH_COUNT> {
            std::array<size_t, iblt::K_HASH_COUNT> result{};
            for (size_t i = 0; i < result.size(); ++i) {
                uint64_t candidate = hash + (i + 1) * 0x9E3779B97F4A7C15ULL;
                candidate ^= candidate >> 30U;
                candidate *= 0xBF58476D1CE4E5B9ULL;
                candidate ^= candidate >> 27U;
                candidate *= 0x94D049BB133111EBULL;
                candidate ^= candidate >> 31U;
                result[i] = static_cast<size_t>(candidate % count);
                while (std::ranges::find(
                           result.begin(),
                           result.begin() + static_cast<ptrdiff_t>(i),
                           result[i]
                       )
                       != result.begin()
                       + static_cast<ptrdiff_t>(i)) {
                    ++result[i];
                    if (result[i] == count) {
                        result[i] = 0;
                    }
                }
            }
            return result;
        }

        void apply(iblt::Cell& cell, int64_t sign, const iblt::Item& row, uint64_t checksum) {
            cell.count += sign;
            cell.key_hash_xor ^= row.key_hash;
            cell.version_xor ^= row.version;
            cell.writer_xor ^= row.writer;
            cell.tombstone_xor ^= row.tombstone ? 1U : 0U;
            cell.checksum_xor ^= checksum;
        }

        auto pure(const iblt::Cell& cell) -> bool {
            if (cell.count != 1 && cell.count != -1) {
                return false;
            }
            if (cell.tombstone_xor != 0 && cell.tombstone_xor != 1) {
                return false;
            }
            return cell.checksum_xor
                == itemChecksum(
                       cell.key_hash_xor,
                       cell.version_xor,
                       cell.writer_xor,
                       cell.tombstone_xor != 0
                );
        }
    } // namespace

    namespace iblt {

        auto build(const std::vector<Item>& rows, size_t cell_count) -> std::optional<Sketch> {
            if (cell_count < K_HASH_COUNT || cell_count > K_MAX_CELLS) {
                return std::nullopt;
            }

            Sketch sketch;
            sketch.cells.resize(cell_count);
            for (const auto& row : rows) {
                insert(sketch, row.key_hash, row.version, row.writer, row.tombstone);
            }
            return sketch;
        }

        void insert(
            Sketch& sketch,
            uint64_t key_hash,
            uint64_t version,
            uint64_t writer,
            bool tombstone
        ) {
            const Item row{
                .key_hash = key_hash,
                .version = version,
                .writer = writer,
                .tombstone = tombstone,
            };

            const auto checksum = itemChecksum(key_hash, version, writer, tombstone);
            const auto map = positions(checksum, sketch.cells.size());
            for (size_t pos : map) {
                apply(sketch.cells[pos], 1, row, checksum);
            }
        }

        auto encode(const Sketch& sketch) -> std::string {
            if (sketch.cells.size() < K_HASH_COUNT || sketch.cells.size() > K_MAX_CELLS) {
                return {};
            }

            std::vector<std::byte> buf(2 * sizeof(uint32_t) + sketch.cells.size() * K_CELL_BYTES);
            BufferWriter out(buf);
            out.write(K_MAGIC);
            out.write(static_cast<uint32_t>(sketch.cells.size()));
            for (const auto& cell : sketch.cells) {
                out.write(std::bit_cast<uint64_t>(cell.count));
                out.write(cell.key_hash_xor);
                out.write(cell.version_xor);
                out.write(cell.writer_xor);
                out.write(cell.tombstone_xor);
                out.write(cell.checksum_xor);
            }
            return toWireString(buf);
        }

        auto decode(std::string_view data) -> std::optional<Sketch> {
            BufferReader in(data);
            uint32_t magic = 0;
            uint32_t count = 0;
            if (!in.read(magic)
                || magic
                != K_MAGIC
                || !in.read(count)
                || count
                < K_HASH_COUNT
                || count
                > K_MAX_CELLS
                || in.remaining()
                != static_cast<size_t>(count)
                * K_CELL_BYTES) {
                return std::nullopt;
            }

            Sketch sketch;
            sketch.cells.reserve(count);
            for (uint32_t i = 0; i < count; ++i) {
                uint64_t count_bits = 0;
                Cell cell;
                if (!in.read(count_bits)
                    || !in.read(cell.key_hash_xor)
                    || !in.read(cell.version_xor)
                    || !in.read(cell.writer_xor)
                    || !in.read(cell.tombstone_xor)
                    || !in.read(cell.checksum_xor)) {
                    return std::nullopt;
                }
                if (count_bits < (uint64_t{1} << 63U) && count_bits > (uint64_t{1} << 40U)) {
                    return std::nullopt;
                }
                if (count_bits
                    >= (uint64_t{1} << 63U)
                    && (std::numeric_limits<uint64_t>::max() - count_bits)
                    >= (uint64_t{1} << 40U)) {
                    return std::nullopt;
                }

                cell.count = std::bit_cast<int64_t>(count_bits);
                sketch.cells.push_back(cell);
            }
            return sketch;
        }

        auto difference(const Sketch& remote, const Sketch& local) -> std::optional<Diff> {
            if (remote.cells.size()
                != local.cells.size()
                || remote.cells.size()
                < K_HASH_COUNT
                || remote.cells.size()
                > K_MAX_CELLS) {
                return std::nullopt;
            }

            Sketch delta = remote;
            for (size_t i = 0; i < delta.cells.size(); ++i) {
                delta.cells[i].count -= local.cells[i].count;
                delta.cells[i].key_hash_xor ^= local.cells[i].key_hash_xor;
                delta.cells[i].version_xor ^= local.cells[i].version_xor;
                delta.cells[i].writer_xor ^= local.cells[i].writer_xor;
                delta.cells[i].tombstone_xor ^= local.cells[i].tombstone_xor;
                delta.cells[i].checksum_xor ^= local.cells[i].checksum_xor;
            }

            Diff decoded;
            std::vector<size_t> queue;
            std::vector<char> queued(delta.cells.size(), 0);
            queue.reserve(delta.cells.size());
            for (size_t i = 0; i < delta.cells.size(); ++i) {
                if (pure(delta.cells[i])) {
                    queue.push_back(i);
                    queued[i] = 1;
                }
            }
            // Each peel removes one item's remaining copies, so a decodable
            // sketch finishes in at most (#items) steps — far below cells ×
            // kHashCount in every regime that can succeed. The bound only
            // trips on checksum-forgery loops from hostile blobs, which the
            // final all-zero check would reject anyway after unbounded work.
            size_t peeled = 0;
            const size_t peel_budget = delta.cells.size() * K_HASH_COUNT;
            for (size_t cursor = 0; cursor < queue.size(); ++cursor) {
                const size_t pos = queue[cursor];
                const Cell cell = delta.cells[pos];
                if (!pure(cell)) {
                    queued[pos] = 0;
                    continue;
                }
                if (++peeled > peel_budget) {
                    return std::nullopt;
                }

                queued[pos] = 0;
                Item row{
                    .key_hash = cell.key_hash_xor,
                    .version = cell.version_xor,
                    .writer = cell.writer_xor,
                    .tombstone = cell.tombstone_xor != 0,
                };

                const bool is_remote = cell.count == 1;
                (is_remote ? decoded.remote_only : decoded.local_only).push_back(row);
                const auto checksum =
                    itemChecksum(row.key_hash, row.version, row.writer, row.tombstone);
                const auto map = positions(checksum, delta.cells.size());
                for (size_t target : map) {
                    apply(delta.cells[target], is_remote ? -1 : 1, row, checksum);
                    if (!queued[target] && pure(delta.cells[target])) {
                        queue.push_back(target);
                        queued[target] = 1;
                    }
                }
            }
            for (const auto& cell : delta.cells) {
                if (cell.count
                    != 0
                    || cell.key_hash_xor
                    != 0
                    || cell.version_xor
                    != 0
                    || cell.writer_xor
                    != 0
                    || cell.tombstone_xor
                    != 0
                    || cell.checksum_xor
                    != 0) {
                    return std::nullopt;
                }
            }
            return decoded;
        }
    } // namespace iblt
} // namespace cinder
