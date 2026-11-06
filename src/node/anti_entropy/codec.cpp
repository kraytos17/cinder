#include <algorithm>
#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <xxhash.h>

#include "cinder/node/anti_entropy_codec.hpp"
#include "cinder/node/iblt.hpp"

namespace cinder {
    namespace {

        using detail::BufferReader;
        using detail::BufferWriter;
        using detail::toWireString;
    } // namespace

    namespace ae_codec {

        auto encodeDigest(const std::vector<uint64_t>& digest) -> std::string {
            using M = AntiEntropyManager;
            std::vector<std::byte> buf(sizeof(uint32_t) * 2 + digest.size() * sizeof(uint64_t));
            BufferWriter out(buf);
            out.write(M::K_DIGEST_MAGIC);
            out.write(static_cast<uint32_t>(digest.size()));
            for (auto h : digest) {
                out.write(h);
            }
            return toWireString(buf);
        }

        auto decodeDigest(std::string_view data, size_t expected_buckets)
            -> std::optional<std::vector<uint64_t>> {
            using M = AntiEntropyManager;
            BufferReader in(data);
            uint32_t magic = 0;
            uint32_t n = 0;
            if (!in.read(magic) || magic != M::K_DIGEST_MAGIC) {
                return std::nullopt;
            }
            if (!in.read(n) || n != expected_buckets) {
                return std::nullopt;
            }
            if (in.remaining() != static_cast<size_t>(n) * sizeof(uint64_t)) {
                return std::nullopt;
            }

            std::vector<uint64_t> digest;
            digest.reserve(n);
            for (uint32_t i = 0; i < n; ++i) {
                uint64_t h = 0;
                if (!in.read(h)) {
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
                const size_t begin = group * M::K_BUCKETS_PER_GROUP;
                const size_t end = std::min(begin + M::K_BUCKETS_PER_GROUP, bucket_hashes.size());
                std::vector<std::byte> material(
                    (end - begin) * (sizeof(uint32_t) + sizeof(uint64_t))
                );

                BufferWriter mat(material);
                for (size_t bucket = begin; bucket < end; ++bucket) {
                    mat.write(static_cast<uint32_t>(bucket));
                    mat.write(bucket_hashes[bucket]);
                }
                groups[group] = XXH3_64bits(mat.bytes().data(), mat.bytes().size());
            }
            return groups;
        }

        auto encodeGroupDigest(const std::vector<uint64_t>& groups) -> std::string {
            using M = AntiEntropyManager;
            std::vector<std::byte> buf(2 * sizeof(uint32_t) + groups.size() * sizeof(uint64_t));
            BufferWriter out(buf);
            out.write(M::K_GROUP_DIGEST_MAGIC);
            out.write(static_cast<uint32_t>(groups.size()));
            for (uint64_t hash : groups) {
                out.write(hash);
            }
            return toWireString(buf);
        }

        auto decodeGroupDigest(std::string_view data, size_t expected_groups)
            -> std::optional<std::vector<uint64_t>> {
            using M = AntiEntropyManager;
            BufferReader in(data);
            uint32_t magic = 0;
            uint32_t count = 0;
            if (!in.read(magic)
                || magic
                != M::K_GROUP_DIGEST_MAGIC
                || !in.read(count)
                || count
                != expected_groups
                || in.remaining()
                != static_cast<size_t>(count)
                * sizeof(uint64_t)) {
                return std::nullopt;
            }

            std::vector<uint64_t> result;
            result.reserve(count);
            for (uint32_t i = 0; i < count; ++i) {
                uint64_t hash = 0;
                if (!in.read(hash)) {
                    return std::nullopt;
                }
                result.push_back(hash);
            }
            return result;
        }

        auto encodeBucketRefinement(const BucketRefinement& request) -> std::string {
            using M = AntiEntropyManager;
            std::vector<std::byte> buf(
                3
                * sizeof(uint32_t)
                + request.bucket_hashes.size()
                * (sizeof(uint32_t) + sizeof(uint64_t))
            );

            BufferWriter out(buf);
            out.write(M::K_BUCKET_REFINEMENT_MAGIC);
            out.write(request.force_metadata ? 1U : 0U);
            out.write(static_cast<uint32_t>(request.bucket_hashes.size()));
            for (const auto& [bucket, hash] : request.bucket_hashes) {
                out.write(bucket);
                out.write(hash);
            }
            return toWireString(buf);
        }

        auto decodeBucketRefinement(std::string_view data, size_t expected_groups)
            -> std::optional<BucketRefinement> {
            using M = AntiEntropyManager;
            BufferReader in(data);
            uint32_t magic = 0;
            uint32_t flags = 0;
            uint32_t bucket_count = 0;
            if (!in.read(magic)
                || magic
                != M::K_BUCKET_REFINEMENT_MAGIC
                || !in.read(flags)
                || (flags & ~1U)
                != 0
                || !in.read(bucket_count)
                || in.remaining()
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
                if (!in.read(bucket) || bucket >= expected_groups || !in.read(hash)) {
                    return std::nullopt;
                }
                request.bucket_hashes.emplace_back(bucket, hash);
            }
            return request;
        }

        auto encodeDigestResponse(const DigestResponse& response) -> std::string {
            using M = AntiEntropyManager;
            std::string rows = encodeMetaRows(response.rows);
            size_t sketches_size = 0;
            for (const auto& [bucket, sketch] : response.iblt_sketches) {
                (void)bucket;
                sketches_size += 2 * sizeof(uint32_t) + sketch.size();
            }

            std::vector<std::byte> buf(
                4
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
                + sketches_size
            );

            BufferWriter out(buf);
            out.write(M::K_DIGEST_RESPONSE_MAGIC);
            out.write(static_cast<uint32_t>(response.group_hashes.size()));
            for (uint64_t hash : response.group_hashes) {
                out.write(hash);
            }

            out.write(static_cast<uint32_t>(response.bucket_hashes.size()));
            for (const auto& [bucket, hash] : response.bucket_hashes) {
                out.write(bucket);
                out.write(hash);
            }

            out.write(static_cast<uint32_t>(response.covered_buckets.size()));
            for (uint32_t bucket : response.covered_buckets) {
                out.write(bucket);
            }

            out.write(static_cast<uint32_t>(rows.size()));
            out.writeString(rows);
            out.write(static_cast<uint32_t>(response.iblt_sketches.size()));
            for (const auto& [bucket, sketch] : response.iblt_sketches) {
                out.write(bucket);
                out.write(static_cast<uint32_t>(sketch.size()));
                out.writeString(sketch);
            }
            return toWireString(buf);
        }

        auto decodeDigestResponse(std::string_view data, size_t expected_groups)
            -> std::optional<DigestResponse> {
            using M = AntiEntropyManager;
            BufferReader in(data);
            uint32_t magic = 0;
            uint32_t group_count = 0;
            if (!in.read(magic)
                || magic
                != M::K_DIGEST_RESPONSE_MAGIC
                || !in.read(group_count)
                || group_count
                != expected_groups) {
                return std::nullopt;
            }

            DigestResponse response;
            response.group_hashes.reserve(group_count);
            for (uint32_t i = 0; i < group_count; ++i) {
                uint64_t hash = 0;
                if (!in.read(hash)) {
                    return std::nullopt;
                }
                response.group_hashes.push_back(hash);
            }

            uint32_t bucket_hash_count = 0;
            if (!in.read(bucket_hash_count)
                || in.remaining()
                < static_cast<size_t>(bucket_hash_count)
                * (sizeof(uint32_t) + sizeof(uint64_t))) {
                return std::nullopt;
            }

            response.bucket_hashes.reserve(bucket_hash_count);
            for (uint32_t i = 0; i < bucket_hash_count; ++i) {
                uint32_t bucket = 0;
                uint64_t hash = 0;
                if (!in.read(bucket) || !in.read(hash)) {
                    return std::nullopt;
                }
                response.bucket_hashes.emplace_back(bucket, hash);
            }

            uint32_t covered_count = 0;
            if (!in.read(covered_count)
                || in.remaining()
                < static_cast<size_t>(covered_count)
                * sizeof(uint32_t)
                + sizeof(uint32_t)) {
                return std::nullopt;
            }

            response.covered_buckets.reserve(covered_count);
            for (uint32_t i = 0; i < covered_count; ++i) {
                uint32_t bucket = 0;
                if (!in.read(bucket)) {
                    return std::nullopt;
                }
                response.covered_buckets.push_back(bucket);
            }

            uint32_t rows_size = 0;
            if (!in.read(rows_size) || in.remaining() < rows_size + sizeof(uint32_t)) {
                return std::nullopt;
            }

            auto rows_view = in.tryView(rows_size);
            if (!rows_view.has_value()) {
                return std::nullopt;
            }
            auto rows = decodeMetaRows(*rows_view);
            if (!rows.has_value()) {
                return std::nullopt;
            }

            response.rows = std::move(*rows);
            uint32_t sketch_count = 0;
            if (!in.read(sketch_count)
                || static_cast<size_t>(sketch_count)
                > in.remaining()
                / (2 * sizeof(uint32_t))) {
                return std::nullopt;
            }

            response.iblt_sketches.reserve(sketch_count);
            for (uint32_t i = 0; i < sketch_count; ++i) {
                uint32_t bucket = 0;
                uint32_t sketch_size = 0;
                std::string sketch_blob;
                if (!in.read(bucket)
                    || !in.read(sketch_size)
                    || !in.readString(sketch_size, sketch_blob)
                    || !iblt::decode(sketch_blob).has_value()) {
                    return std::nullopt;
                }
                response.iblt_sketches.emplace_back(bucket, std::move(sketch_blob));
            }
            if (in.remaining() != 0) {
                return std::nullopt;
            }
            // covered_buckets arrives in encode order; sort one copy so the
            // membership searches below validate by content, not by order.
            std::vector<uint32_t> sorted_covered = response.covered_buckets;
            std::ranges::sort(sorted_covered);
            for (const auto& row : response.rows) {
                if (!std::ranges::binary_search(sorted_covered, row.bucket)) {
                    return std::nullopt;
                }
            }

            std::vector<char> sketch_bucket_seen(
                sorted_covered.empty() ? 0 : static_cast<size_t>(sorted_covered.back()) + 1,
                0
            );

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

        auto encodeMetaRows(const std::vector<MetaRow>& rows) -> std::string {
            std::vector<std::byte> buf(sizeof(uint32_t) + rows.size() * K_META_ROW_BYTES);
            BufferWriter out(buf);
            out.write(static_cast<uint32_t>(rows.size()));
            for (const auto& r : rows) {
                out.write(r.bucket);
                out.write(r.key_hash);
                out.write(r.version);
                out.write(r.writer);
                out.write(r.tombstone ? uint8_t{1} : uint8_t{0});
            }
            return toWireString(buf);
        }

        auto decodeMetaRows(std::string_view data) -> std::optional<std::vector<MetaRow>> {
            BufferReader in(data);
            uint32_t n = 0;
            if (!in.read(n)) {
                return std::nullopt;
            }
            if (in.remaining() != static_cast<size_t>(n) * K_META_ROW_BYTES) {
                return std::nullopt;
            }

            std::vector<MetaRow> rows;
            rows.reserve(n);
            for (uint32_t i = 0; i < n; ++i) {
                MetaRow r;
                uint8_t tombstone = 0;
                if (!in.read(r.bucket)
                    || !in.read(r.key_hash)
                    || !in.read(r.version)
                    || !in.read(r.writer)
                    || !in.read(tombstone)) {
                    return std::nullopt;
                }
                // Exact-size validation above bounds the reader: one flag
                // byte remains here.
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
            size_t entries_size = 0;
            for (const auto& item : msg.entries) {
                entries_size += syncEntryWireSize(item.key, item.entry);
            }

            std::vector<std::byte> buf(
                2
                * sizeof(uint32_t)
                + entries_size
                + sizeof(uint32_t)
                + msg.needs.size()
                * sizeof(uint64_t)
            );

            BufferWriter out(buf);
            out.write(M::K_SYNC_MAGIC);
            out.write(static_cast<uint32_t>(msg.entries.size()));
            for (const auto& item : msg.entries) {
                out.write(static_cast<uint32_t>(item.key.size()));
                out.writeString(item.key);
                out.write(item.entry.version());
                out.write(item.entry.writer_node_hash);
                // Entry flags bitfield: bit 0 = has_ttl, bit 1 = tombstone.
                // Unknown bits are rejected (clean fail on mixed versions).
                uint8_t entry_flags = 0;
                if (item.entry.hasTtl()) {
                    entry_flags |= 1U;
                }
                if (item.entry.isTombstone()) {
                    entry_flags |= 2U;
                }

                out.write(entry_flags);
                if (item.entry.hasTtl()) {
                    out.write(toSystemMs(clock, item.entry.expires_at));
                }

                out.write(static_cast<uint32_t>(item.entry.value.size()));
                out.writeString(item.entry.value);
            }

            out.write(static_cast<uint32_t>(msg.needs.size()));
            for (auto h : msg.needs) {
                out.write(h);
            }
            return toWireString(buf);
        }

        auto decodeSync(std::string_view data, const Clock& clock) -> std::optional<SyncMessage> {
            using M = AntiEntropyManager;
            BufferReader in(data);
            uint32_t magic = 0;
            if (!in.read(magic) || magic != M::K_SYNC_MAGIC) {
                return std::nullopt;
            }

            SyncMessage msg;
            uint32_t entry_count = 0;
            if (!in.read(entry_count)) {
                return std::nullopt;
            }

            msg.entries.reserve(entry_count);
            for (uint32_t i = 0; i < entry_count; ++i) {
                SyncMessage::Item item;
                std::string key;
                VersionedEntry entry;
                if (auto reason = parseSingleEntry(in, key, entry, clock); reason.has_value()) {
                    return std::nullopt;
                }

                item.key = std::move(key);
                item.entry = std::move(entry);
                msg.entries.push_back(std::move(item));
            }

            uint32_t need_count = 0;
            if (!in.read(need_count)) {
                return std::nullopt;
            }
            if (in.remaining() != static_cast<size_t>(need_count) * sizeof(uint64_t)) {
                return std::nullopt;
            }

            msg.needs.reserve(need_count);
            for (uint32_t i = 0; i < need_count; ++i) {
                uint64_t h = 0;
                if (!in.read(h)) {
                    return std::nullopt;
                }
                msg.needs.push_back(h);
            }
            return msg;
        }

        auto parseSingleEntry(
            BufferReader& in,
            std::string& key,
            VersionedEntry& entry,
            const Clock& clock
        ) -> std::optional<std::string_view> {
            uint32_t key_len = 0;
            uint64_t version = 0;
            uint64_t writer_hash = 0;
            uint32_t val_len = 0;
            std::string value;
            if (!in.read(key_len)
                || !in.readString(key_len, key)
                || !in.read(version)
                || !in.read(writer_hash)
                || in.remaining()
                < 1) {
                return "truncated entry";
            }

            uint8_t entry_flags = 0;
            if (!in.read(entry_flags)) {
                return "truncated entry";
            }
            if ((entry_flags & uint8_t{3}) != entry_flags) {
                return "bad entry flags";
            }

            const bool has_ttl = (entry_flags & 1U) != 0;
            uint64_t expires_ms = 0;
            if (has_ttl && !in.read(expires_ms)) {
                return "truncated expiry";
            }
            if (!in.read(val_len) || !in.readString(val_len, value)) {
                return "truncated value";
            }

            entry.setVersion(version);
            entry.writer_node_hash = writer_hash;
            entry.value = std::move(value);
            entry.setTombstone((entry_flags & 2U) != 0);
            if (has_ttl) {
                entry.setHasTtl(true);
                entry.expires_at =
                    toSteadyExpiry(clock, system_clock::time_point(milliseconds(expires_ms)));
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
