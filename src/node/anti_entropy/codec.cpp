#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <concepts>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <xxhash.h>

#include "cinder/node/anti_entropy_codec.hpp"

namespace cinder {
    namespace {

        // Little-endian append helper for the digest/sync blobs. The
        // cluster is homogeneous, but a fixed endianness keeps the wire
        // format well-defined.
        template <typename T>
            requires std::unsigned_integral<T>
        void appendLE(std::string& out, T v) {
            std::array<char, sizeof(v)> buf{};
            std::memcpy(buf.data(), &v, buf.size());
            out.append(buf.data(), buf.size());
        }

        template <typename T>
            requires std::unsigned_integral<T>
        auto readLE(const char*& p, const char* end, T& out) -> bool {
            if (static_cast<size_t>(end - p) < sizeof(T)) {
                return false;
            }

            std::memcpy(&out, p, sizeof(out));
            p += sizeof(out);
            return true;
        }

        auto readBytes(const char*& p, const char* end, size_t n, std::string& out) -> bool {
            if (n > static_cast<size_t>(end - p)) {
                return false;
            }

            out.assign(p, n);
            p += n;
            return true;
        }

        auto ibltItemChecksum(uint64_t key_hash, uint64_t version, uint64_t writer, bool tombstone)
            -> uint64_t {
            std::array<uint64_t, 4> words{key_hash, version, writer, tombstone ? 1U : 0U};
            return XXH3_64bits(words.data(), sizeof(words));
        }

        auto ibltPositions(
            uint64_t key_hash,
            uint64_t version,
            uint64_t writer,
            bool tombstone,
            size_t count
        ) -> std::array<size_t, AntiEntropyManager::K_IBLT_HASH_COUNT> {
            std::array<size_t, AntiEntropyManager::K_IBLT_HASH_COUNT> positions{};
            std::array<uint64_t, 4> words{key_hash, version, writer, tombstone ? 1U : 0U};
            uint64_t hash = XXH3_64bits(words.data(), sizeof(words));
            for (size_t i = 0; i < positions.size(); ++i) {
                uint64_t candidate = hash + (i + 1) * 0x9E3779B97F4A7C15ULL;
                candidate ^= candidate >> 30U;
                candidate *= 0xBF58476D1CE4E5B9ULL;
                candidate ^= candidate >> 27U;
                candidate *= 0x94D049BB133111EBULL;
                candidate ^= candidate >> 31U;
                positions[i] = static_cast<size_t>(candidate % count);
                while (std::ranges::find(
                           positions.begin(),
                           positions.begin() + static_cast<ptrdiff_t>(i),
                           positions[i]
                       )
                       != positions.begin()
                       + static_cast<ptrdiff_t>(i)) {
                    ++positions[i];
                    if (positions[i] == count) {
                        positions[i] = 0;
                    }
                }
            }
            return positions;
        }

        void ibltApply(
            AntiEntropyManager::IbltCell& cell,
            int64_t sign,
            uint64_t key_hash,
            uint64_t version,
            uint64_t writer,
            bool tombstone
        ) {
            cell.count += sign;
            cell.key_hash_xor ^= key_hash;
            cell.version_xor ^= version;
            cell.writer_xor ^= writer;
            cell.tombstone_xor ^= tombstone ? 1U : 0U;
            cell.checksum_xor ^= ibltItemChecksum(key_hash, version, writer, tombstone);
        }

        auto ibltPure(const AntiEntropyManager::IbltCell& cell) -> bool {
            if (cell.count != 1 && cell.count != -1) {
                return false;
            }
            if (cell.tombstone_xor != 0 && cell.tombstone_xor != 1) {
                return false;
            }
            return cell.checksum_xor
                == ibltItemChecksum(
                       cell.key_hash_xor,
                       cell.version_xor,
                       cell.writer_xor,
                       cell.tombstone_xor != 0
                );
        }
    } // namespace

    namespace ae_codec {

        auto encodeDigest(const std::vector<uint64_t>& digest) -> std::string {
            using M = AntiEntropyManager;
            std::string out;

            out.reserve(sizeof(uint32_t) * 2 + digest.size() * sizeof(uint64_t));
            appendLE(out, M::K_DIGEST_MAGIC);
            appendLE(out, static_cast<uint32_t>(digest.size()));
            for (auto h : digest) {
                appendLE(out, h);
            }
            return out;
        }

        auto decodeDigest(std::string_view data, size_t expected_buckets)
            -> std::optional<std::vector<uint64_t>> {
            using M = AntiEntropyManager;
            const char* p = data.data();
            const char* end = p + data.size();
            uint32_t magic = 0;
            uint32_t n = 0;
            if (!readLE(p, end, magic) || magic != M::K_DIGEST_MAGIC) {
                return std::nullopt;
            }
            if (!readLE(p, end, n) || n != expected_buckets) {
                return std::nullopt;
            }
            if (static_cast<size_t>(end - p) != static_cast<size_t>(n) * sizeof(uint64_t)) {
                return std::nullopt;
            }

            std::vector<uint64_t> digest;
            digest.reserve(n);
            for (uint32_t i = 0; i < n; ++i) {
                uint64_t h = 0;
                if (!readLE(p, end, h)) {
                    return std::nullopt;
                }
                digest.push_back(h);
            }
            return digest;
        }

        auto groupDigests(const std::vector<uint64_t>& bucket_hashes) -> std::vector<uint64_t> {
            using M = AntiEntropyManager;
            const size_t group_count =
                (bucket_hashes.size() + M::K_BUCKETS_PER_GROUP - 1) / M::K_BUCKETS_PER_GROUP;
            std::vector<uint64_t> groups(group_count, 0);
            for (size_t group = 0; group < group_count; ++group) {
                // Include each absolute bucket index to make the fold sensitive to
                // placement, not only the multiset of bucket hashes.
                std::string material;
                const size_t begin = group * M::K_BUCKETS_PER_GROUP;
                const size_t end = std::min(begin + M::K_BUCKETS_PER_GROUP, bucket_hashes.size());
                material.reserve((end - begin) * (sizeof(uint32_t) + sizeof(uint64_t)));
                for (size_t bucket = begin; bucket < end; ++bucket) {
                    appendLE(material, static_cast<uint32_t>(bucket));
                    appendLE(material, bucket_hashes[bucket]);
                }
                groups[group] = XXH3_64bits(material.data(), material.size());
            }
            return groups;
        }

        auto encodeGroupDigest(const std::vector<uint64_t>& groups) -> std::string {
            using M = AntiEntropyManager;
            std::string out;
            out.reserve(2 * sizeof(uint32_t) + groups.size() * sizeof(uint64_t));
            appendLE(out, M::K_GROUP_DIGEST_MAGIC);
            appendLE(out, static_cast<uint32_t>(groups.size()));
            for (uint64_t hash : groups) {
                appendLE(out, hash);
            }
            return out;
        }

        auto decodeGroupDigest(std::string_view data, size_t expected_groups)
            -> std::optional<std::vector<uint64_t>> {
            using M = AntiEntropyManager;
            const char* p = data.data();
            const char* end = p + data.size();
            uint32_t magic = 0;
            uint32_t count = 0;
            if (!readLE(p, end, magic)
                || magic
                != M::K_GROUP_DIGEST_MAGIC
                || !readLE(p, end, count)
                || count
                != expected_groups
                || static_cast<size_t>(end - p)
                != static_cast<size_t>(count)
                * sizeof(uint64_t)) {
                return std::nullopt;
            }

            std::vector<uint64_t> result;
            result.reserve(count);
            for (uint32_t i = 0; i < count; ++i) {
                uint64_t hash = 0;
                if (!readLE(p, end, hash)) {
                    return std::nullopt;
                }
                result.push_back(hash);
            }
            return result;
        }

        auto encodeBucketRefinement(const BucketRefinement& request) -> std::string {
            using M = AntiEntropyManager;
            std::string out;
            appendLE(out, M::K_BUCKET_REFINEMENT_MAGIC);
            appendLE(out, request.force_metadata ? 1U : 0U);
            appendLE(out, static_cast<uint32_t>(request.bucket_hashes.size()));
            for (const auto& [bucket, hash] : request.bucket_hashes) {
                appendLE(out, bucket);
                appendLE(out, hash);
            }
            return out;
        }

        auto decodeBucketRefinement(std::string_view data, size_t expected_groups)
            -> std::optional<BucketRefinement> {
            using M = AntiEntropyManager;
            const char* p = data.data();
            const char* end = p + data.size();
            uint32_t magic = 0;
            uint32_t flags = 0;
            uint32_t bucket_count = 0;
            if (!readLE(p, end, magic)
                || magic
                != M::K_BUCKET_REFINEMENT_MAGIC
                || !readLE(p, end, flags)
                || (flags & ~1U)
                != 0
                || !readLE(p, end, bucket_count)
                || static_cast<size_t>(end - p)
                != static_cast<size_t>(bucket_count)
                * (sizeof(uint32_t) + sizeof(uint64_t))) {
                return std::nullopt;
            }

            BucketRefinement request;
            request.force_metadata = (flags & 1U) != 0;
            request.bucket_hashes.reserve(bucket_count);
            for (uint32_t i = 0; i < bucket_count; ++i) {
                uint32_t bucket = 0;
                uint64_t hash = 0;
                if (!readLE(p, end, bucket) || bucket >= expected_groups || !readLE(p, end, hash)) {
                    return std::nullopt;
                }
                request.bucket_hashes.emplace_back(bucket, hash);
            }
            return request;
        }

        auto encodeDigestResponse(const DigestResponse& response) -> std::string {
            using M = AntiEntropyManager;
            std::string rows = encodeMetaRows(response.rows);
            std::string out;
            out.reserve(
                3
                * sizeof(uint32_t)
                + response.group_hashes.size()
                * sizeof(uint64_t)
                + response.bucket_hashes.size()
                * (sizeof(uint32_t) + sizeof(uint64_t))
                + response.covered_buckets.size()
                * sizeof(uint32_t)
                + sizeof(uint32_t)
                + rows.size()
                + sizeof(uint32_t)
            );

            appendLE(out, M::K_DIGEST_RESPONSE_MAGIC);
            appendLE(out, static_cast<uint32_t>(response.group_hashes.size()));
            for (uint64_t hash : response.group_hashes) {
                appendLE(out, hash);
            }

            appendLE(out, static_cast<uint32_t>(response.bucket_hashes.size()));
            for (const auto& [bucket, hash] : response.bucket_hashes) {
                appendLE(out, bucket);
                appendLE(out, hash);
            }

            appendLE(out, static_cast<uint32_t>(response.covered_buckets.size()));
            for (uint32_t bucket : response.covered_buckets) {
                appendLE(out, bucket);
            }

            appendLE(out, static_cast<uint32_t>(rows.size()));
            out += rows;
            appendLE(out, static_cast<uint32_t>(response.iblt_sketches.size()));
            for (const auto& [bucket, sketch] : response.iblt_sketches) {
                appendLE(out, bucket);
                appendLE(out, static_cast<uint32_t>(sketch.size()));
                out += sketch;
            }
            return out;
        }

        auto decodeDigestResponse(std::string_view data, size_t expected_groups)
            -> std::optional<DigestResponse> {
            using M = AntiEntropyManager;
            const char* p = data.data();
            const char* end = p + data.size();
            uint32_t magic = 0;
            uint32_t group_count = 0;
            if (!readLE(p, end, magic)
                || magic
                != M::K_DIGEST_RESPONSE_MAGIC
                || !readLE(p, end, group_count)
                || group_count
                != expected_groups) {
                return std::nullopt;
            }

            DigestResponse response;
            response.group_hashes.reserve(group_count);
            for (uint32_t i = 0; i < group_count; ++i) {
                uint64_t hash = 0;
                if (!readLE(p, end, hash)) {
                    return std::nullopt;
                }
                response.group_hashes.push_back(hash);
            }

            uint32_t bucket_hash_count = 0;
            if (!readLE(p, end, bucket_hash_count)
                || static_cast<size_t>(end - p)
                < static_cast<size_t>(bucket_hash_count)
                * (sizeof(uint32_t) + sizeof(uint64_t))) {
                return std::nullopt;
            }

            response.bucket_hashes.reserve(bucket_hash_count);
            for (uint32_t i = 0; i < bucket_hash_count; ++i) {
                uint32_t bucket = 0;
                uint64_t hash = 0;
                if (!readLE(p, end, bucket) || !readLE(p, end, hash)) {
                    return std::nullopt;
                }
                response.bucket_hashes.emplace_back(bucket, hash);
            }

            uint32_t covered_count = 0;
            if (!readLE(p, end, covered_count)
                || static_cast<size_t>(end - p)
                < static_cast<size_t>(covered_count)
                * sizeof(uint32_t)
                + sizeof(uint32_t)) {
                return std::nullopt;
            }

            response.covered_buckets.reserve(covered_count);
            for (uint32_t i = 0; i < covered_count; ++i) {
                uint32_t bucket = 0;
                if (!readLE(p, end, bucket)) {
                    return std::nullopt;
                }
                response.covered_buckets.push_back(bucket);
            }

            uint32_t rows_size = 0;
            if (!readLE(p, end, rows_size)
                || static_cast<size_t>(end - p)
                < rows_size
                + sizeof(uint32_t)) {
                return std::nullopt;
            }

            auto rows = decodeMetaRows(std::string_view(p, rows_size));
            if (!rows.has_value()) {
                return std::nullopt;
            }

            response.rows = std::move(*rows);
            p += rows_size;
            uint32_t sketch_count = 0;
            if (!readLE(p, end, sketch_count)
                || static_cast<size_t>(sketch_count)
                > static_cast<size_t>(end - p)
                / (2 * sizeof(uint32_t))) {
                return std::nullopt;
            }

            response.iblt_sketches.reserve(sketch_count);
            for (uint32_t i = 0; i < sketch_count; ++i) {
                uint32_t bucket = 0;
                uint32_t sketch_size = 0;
                std::string sketch_blob;
                if (!readLE(p, end, bucket)
                    || !readLE(p, end, sketch_size)
                    || !readBytes(p, end, sketch_size, sketch_blob)
                    || !decodeIblt(sketch_blob).has_value()) {
                    return std::nullopt;
                }
                response.iblt_sketches.emplace_back(bucket, std::move(sketch_blob));
            }
            if (p != end) {
                return std::nullopt;
            }
            for (const auto& row : response.rows) {
                if (!std::ranges::binary_search(response.covered_buckets, row.bucket)) {
                    return std::nullopt;
                }
            }

            std::vector<char> sketch_bucket_seen(
                response.covered_buckets.empty()
                    ? 0
                    : static_cast<size_t>(response.covered_buckets.back()) + 1,
                0
            );

            std::vector<uint32_t> sorted_covered = response.covered_buckets;
            std::ranges::sort(sorted_covered);
            std::vector<uint32_t> sketch_buckets;
            sketch_buckets.reserve(response.iblt_sketches.size());
            for (const auto& [bucket, _] : response.iblt_sketches) {
                if (!std::ranges::binary_search(sorted_covered, bucket)) {
                    return std::nullopt;
                }
                if (bucket >= sketch_bucket_seen.size() || sketch_bucket_seen[bucket]) {
                    return std::nullopt;
                }

                sketch_bucket_seen[bucket] = 1;
                sketch_buckets.push_back(bucket);
            }

            std::ranges::sort(sketch_buckets);
            if (std::ranges::adjacent_find(sketch_buckets) != sketch_buckets.end()) {
                return std::nullopt;
            }
            for (const auto& row : response.rows) {
                if (row.bucket < sketch_bucket_seen.size() && sketch_bucket_seen[row.bucket]) {
                    return std::nullopt;
                }
            }
            return response;
        }

        auto buildIblt(const std::vector<MetaRow>& rows, size_t cell_count)
            -> std::optional<IbltSketch> {
            using M = AntiEntropyManager;
            if (cell_count < M::K_IBLT_HASH_COUNT || cell_count > M::K_MAX_IBLT_CELLS) {
                return std::nullopt;
            }

            IbltSketch sketch;
            sketch.cells.resize(cell_count);
            for (const auto& row : rows) {
                ibltInsert(sketch, row.key_hash, row.version, row.writer, row.tombstone);
            }
            return sketch;
        }

        void ibltInsert(
            IbltSketch& sketch,
            uint64_t key_hash,
            uint64_t version,
            uint64_t writer,
            bool tombstone
        ) {
            const auto positions =
                ibltPositions(key_hash, version, writer, tombstone, sketch.cells.size());
            for (size_t pos : positions) {
                ibltApply(sketch.cells[pos], 1, key_hash, version, writer, tombstone);
            }
        }

        auto encodeIblt(const IbltSketch& sketch) -> std::string {
            using M = AntiEntropyManager;
            if (sketch.cells.size()
                < M::K_IBLT_HASH_COUNT
                || sketch.cells.size()
                > M::K_MAX_IBLT_CELLS) {
                return {};
            }

            std::string out;
            out.reserve(2 * sizeof(uint32_t) + sketch.cells.size() * 48U);
            appendLE(out, M::K_IBLT_MAGIC);
            appendLE(out, static_cast<uint32_t>(sketch.cells.size()));
            for (const auto& cell : sketch.cells) {
                appendLE(out, std::bit_cast<uint64_t>(cell.count));
                appendLE(out, cell.key_hash_xor);
                appendLE(out, cell.version_xor);
                appendLE(out, cell.writer_xor);
                appendLE(out, cell.tombstone_xor);
                appendLE(out, cell.checksum_xor);
            }
            return out;
        }

        auto decodeIblt(std::string_view data) -> std::optional<IbltSketch> {
            using M = AntiEntropyManager;
            const char* p = data.data();
            const char* end = p + data.size();
            uint32_t magic = 0;
            uint32_t count = 0;
            constexpr size_t K_CELL_BYTES = sizeof(uint64_t) + 5 * sizeof(uint64_t);
            if (!readLE(p, end, magic)
                || magic
                != M::K_IBLT_MAGIC
                || !readLE(p, end, count)
                || count
                < M::K_IBLT_HASH_COUNT
                || count
                > M::K_MAX_IBLT_CELLS
                || static_cast<size_t>(end - p)
                != static_cast<size_t>(count)
                * K_CELL_BYTES) {
                return std::nullopt;
            }

            IbltSketch sketch;
            sketch.cells.reserve(count);
            for (uint32_t i = 0; i < count; ++i) {
                uint64_t count_bits = 0;
                IbltCell cell;
                if (!readLE(p, end, count_bits)
                    || !readLE(p, end, cell.key_hash_xor)
                    || !readLE(p, end, cell.version_xor)
                    || !readLE(p, end, cell.writer_xor)
                    || !readLE(p, end, cell.tombstone_xor)
                    || !readLE(p, end, cell.checksum_xor)) {
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

        auto decodeIbltDifference(
            const IbltSketch& remote,
            const IbltSketch& local,
            uint32_t bucket
        ) -> std::optional<IbltDiff> {
            using M = AntiEntropyManager;
            if (remote.cells.size()
                != local.cells.size()
                || remote.cells.size()
                < M::K_IBLT_HASH_COUNT
                || remote.cells.size()
                > M::K_MAX_IBLT_CELLS) {
                return std::nullopt;
            }

            IbltSketch difference = remote;
            for (size_t i = 0; i < difference.cells.size(); ++i) {
                difference.cells[i].count -= local.cells[i].count;
                difference.cells[i].key_hash_xor ^= local.cells[i].key_hash_xor;
                difference.cells[i].version_xor ^= local.cells[i].version_xor;
                difference.cells[i].writer_xor ^= local.cells[i].writer_xor;
                difference.cells[i].tombstone_xor ^= local.cells[i].tombstone_xor;
                difference.cells[i].checksum_xor ^= local.cells[i].checksum_xor;
            }

            IbltDiff decoded;
            std::vector<size_t> queue;
            std::vector<char> queued(difference.cells.size(), 0);
            queue.reserve(difference.cells.size());
            for (size_t i = 0; i < difference.cells.size(); ++i) {
                if (ibltPure(difference.cells[i])) {
                    queue.push_back(i);
                    queued[i] = 1;
                }
            }
            for (size_t cursor = 0; cursor < queue.size(); ++cursor) {
                const size_t pos = queue[cursor];
                const IbltCell cell = difference.cells[pos];
                if (!ibltPure(cell)) {
                    queued[pos] = 0;
                    continue;
                }

                queued[pos] = 0;
                MetaRow row{
                    .bucket = bucket,
                    .tombstone = cell.tombstone_xor != 0,
                    .key_hash = cell.key_hash_xor,
                    .version = cell.version_xor,
                    .writer = cell.writer_xor,
                };

                const bool is_remote = cell.count == 1;
                (is_remote ? decoded.remote_only : decoded.local_only).push_back(row);
                const auto positions = ibltPositions(
                    row.key_hash,
                    row.version,
                    row.writer,
                    row.tombstone,
                    difference.cells.size()
                );
                for (size_t target : positions) {
                    ibltApply(
                        difference.cells[target],
                        is_remote ? -1 : 1,
                        row.key_hash,
                        row.version,
                        row.writer,
                        row.tombstone
                    );
                    if (!queued[target] && ibltPure(difference.cells[target])) {
                        queue.push_back(target);
                        queued[target] = 1;
                    }
                }
            }
            for (const auto& cell : difference.cells) {
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

        auto encodeMetaRows(const std::vector<MetaRow>& rows) -> std::string {
            std::string out;
            out.reserve(sizeof(uint32_t) + rows.size() * K_META_ROW_BYTES);
            appendLE(out, static_cast<uint32_t>(rows.size()));
            for (const auto& r : rows) {
                appendLE(out, r.bucket);
                appendLE(out, r.key_hash);
                appendLE(out, r.version);
                appendLE(out, r.writer);
                out.push_back(r.tombstone ? static_cast<char>(1) : static_cast<char>(0));
            }
            return out;
        }

        auto decodeMetaRows(std::string_view data) -> std::optional<std::vector<MetaRow>> {
            const char* p = data.data();
            const char* end = p + data.size();
            uint32_t n = 0;
            if (!readLE(p, end, n)) {
                return std::nullopt;
            }
            if (static_cast<size_t>(end - p) != static_cast<size_t>(n) * K_META_ROW_BYTES) {
                return std::nullopt;
            }

            std::vector<MetaRow> rows;
            rows.reserve(n);
            for (uint32_t i = 0; i < n; ++i) {
                MetaRow r;
                if (!readLE(p, end, r.bucket)
                    || !readLE(p, end, r.key_hash)
                    || !readLE(p, end, r.version)
                    || !readLE(p, end, r.writer)
                    || p
                    >= end) {
                    return std::nullopt;
                }

                // Exact-size validation above bounds p: one flag byte remains.
                const auto tombstone = static_cast<uint8_t>(*p++);
                if (tombstone > 1) {
                    return std::nullopt;
                }

                r.tombstone = tombstone != 0;
                rows.push_back(r);
            }
            return rows;
        }

        auto encodeSync(const SyncMessage& msg, const Clock& clock) -> std::string {
            using M = AntiEntropyManager;
            std::string out;
            appendLE(out, M::K_SYNC_MAGIC);
            appendLE(out, static_cast<uint32_t>(msg.entries.size()));
            for (const auto& item : msg.entries) {
                appendLE(out, static_cast<uint32_t>(item.key.size()));
                out.append(item.key);
                appendLE(out, item.entry.version());
                appendLE(out, item.entry.writer_node_hash);
                // Entry flags bitfield: bit 0 = has_ttl, bit 1 = tombstone.
                // Unknown bits are rejected (clean fail on mixed versions).
                uint8_t entry_flags = 0;
                if (item.entry.hasTtl()) {
                    entry_flags |= 1U;
                }
                if (item.entry.isTombstone()) {
                    entry_flags |= 2U;
                }

                out.push_back(static_cast<char>(entry_flags));
                if (item.entry.hasTtl()) {
                    appendLE(out, toSystemMs(clock, item.entry.expires_at));
                }

                appendLE(out, static_cast<uint32_t>(item.entry.value.size()));
                out.append(item.entry.value);
            }

            appendLE(out, static_cast<uint32_t>(msg.needs.size()));
            for (auto h : msg.needs) {
                appendLE(out, h);
            }
            return out;
        }

        auto decodeSync(std::string_view data, const Clock& clock) -> std::optional<SyncMessage> {
            using M = AntiEntropyManager;
            const char* p = data.data();
            const char* end = p + data.size();
            uint32_t magic = 0;
            if (!readLE(p, end, magic) || magic != M::K_SYNC_MAGIC) {
                return std::nullopt;
            }

            SyncMessage msg;
            uint32_t entry_count = 0;
            if (!readLE(p, end, entry_count)) {
                return std::nullopt;
            }

            msg.entries.reserve(entry_count);
            for (uint32_t i = 0; i < entry_count; ++i) {
                SyncMessage::Item item;
                std::string key;
                VersionedEntry entry;
                if (auto reason = parseSingleEntry(p, end, key, entry, clock); reason.has_value()) {
                    return std::nullopt;
                }

                item.key = std::move(key);
                item.entry = std::move(entry);
                msg.entries.push_back(std::move(item));
            }

            uint32_t need_count = 0;
            if (!readLE(p, end, need_count)) {
                return std::nullopt;
            }
            if (static_cast<size_t>(end - p)
                != static_cast<size_t>(need_count)
                * sizeof(uint64_t)) {
                return std::nullopt;
            }

            msg.needs.reserve(need_count);
            for (uint32_t i = 0; i < need_count; ++i) {
                uint64_t h = 0;
                if (!readLE(p, end, h)) {
                    return std::nullopt;
                }
                msg.needs.push_back(h);
            }
            return msg;
        }

        auto parseSingleEntry(
            const char*& p,
            const char* end,
            std::string& key,
            VersionedEntry& entry,
            const Clock& clock
        ) -> std::optional<std::string_view> {
            uint32_t key_len = 0;
            uint64_t version = 0;
            uint64_t writer_hash = 0;
            uint32_t val_len = 0;
            std::string value;
            if (!readLE(p, end, key_len)
                || !readBytes(p, end, key_len, key)
                || !readLE(p, end, version)
                || !readLE(p, end, writer_hash)
                || static_cast<size_t>(end - p)
                < 1) {
                return "truncated entry";
            }

            uint8_t entry_flags = static_cast<uint8_t>(*p);
            ++p;
            if ((entry_flags & uint8_t{3}) != entry_flags) {
                return "bad entry flags";
            }

            const bool has_ttl = (entry_flags & 1U) != 0;
            uint64_t expires_ms = 0;
            if (has_ttl && !readLE(p, end, expires_ms)) {
                return "truncated expiry";
            }
            if (!readLE(p, end, val_len) || !readBytes(p, end, val_len, value)) {
                return "truncated value";
            }

            entry.setVersion(version);
            entry.writer_node_hash = writer_hash;
            entry.value = std::move(value);
            entry.setTombstone((entry_flags & 2U) != 0);
            if (has_ttl) {
                entry.setHasTtl(true);
                entry.expires_at = toSteadyExpiry(
                    clock,
                    std::chrono::system_clock::time_point(std::chrono::milliseconds(expires_ms))
                );
            }
            return std::nullopt;
        }

        auto syncEntryWireSize(const std::string& key, const VersionedEntry& entry) -> size_t {
            size_t n = sizeof(uint32_t)
                + key.size()
                + sizeof(uint64_t)
                + sizeof(uint64_t)
                + sizeof(uint8_t)
                + sizeof(uint32_t)
                + entry.value.size();
            if (entry.hasTtl()) {
                n += sizeof(uint64_t);
            }
            return n;
        }
    } // namespace ae_codec
} // namespace cinder
