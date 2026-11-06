#pragma once

// Pure wire codec for anti-entropy payloads, extracted from
// AntiEntropyManager. Everything here is deterministic byte handling with no
// store/clock/transport access — except the sync pair, which takes an explicit
// Clock for TTL conversion. AntiEntropyManager keeps thin forwarders so the
// existing test/fuzz API is unchanged.

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "cinder/cluster/clock.hpp"
#include "cinder/node/anti_entropy.hpp"
#include "cinder/store/detail/io_utils.hpp"

namespace cinder::ae_codec {

    // One serialized metadata row: bucket u32 + key_hash/version/writer u64 +
    // tombstone u8.
    inline constexpr size_t K_META_ROW_BYTES =
        sizeof(uint32_t) + 3 * sizeof(uint64_t) + sizeof(uint8_t);

    using MetaRow = AntiEntropyManager::MetaRow;
    using SyncMessage = AntiEntropyManager::SyncMessage;
    using DigestResponse = AntiEntropyManager::DigestResponse;
    using BucketRefinement = AntiEntropyManager::BucketRefinement;
    using IbltSketch = AntiEntropyManager::IbltSketch;
    using IbltCell = AntiEntropyManager::IbltCell;
    using IbltDiff = AntiEntropyManager::IbltDiff;

    [[nodiscard]]
    auto encodeDigest(const std::vector<uint64_t>& digest) -> std::string;

    [[nodiscard]]
    auto decodeDigest(std::string_view data, size_t expected_buckets)
        -> std::optional<std::vector<uint64_t>>;

    [[nodiscard]]
    auto groupDigests(const std::vector<uint64_t>& bucket_hashes) -> std::vector<uint64_t>;

    [[nodiscard]]
    auto encodeGroupDigest(const std::vector<uint64_t>& groups) -> std::string;

    [[nodiscard]]
    auto decodeGroupDigest(std::string_view data, size_t expected_groups)
        -> std::optional<std::vector<uint64_t>>;

    [[nodiscard]]
    auto encodeBucketRefinement(const BucketRefinement& request) -> std::string;

    [[nodiscard]]
    auto decodeBucketRefinement(std::string_view data, size_t expected_groups)
        -> std::optional<BucketRefinement>;

    [[nodiscard]]
    auto encodeDigestResponse(const DigestResponse& response) -> std::string;

    [[nodiscard]]
    auto decodeDigestResponse(std::string_view data, size_t expected_groups)
        -> std::optional<DigestResponse>;

    [[nodiscard]]
    auto buildIblt(const std::vector<MetaRow>& rows, size_t cell_count)
        -> std::optional<IbltSketch>;

    // Single-item insert into an existing sketch (streaming builds, e.g. the
    // bucket-refinement responder, which switches from row sampling to a
    // sketch mid-scan).
    void ibltInsert(
        IbltSketch& sketch,
        uint64_t key_hash,
        uint64_t version,
        uint64_t writer,
        bool tombstone
    );

    [[nodiscard]]
    auto encodeIblt(const IbltSketch& sketch) -> std::string;

    [[nodiscard]]
    auto decodeIblt(std::string_view data) -> std::optional<IbltSketch>;

    [[nodiscard]]
    auto decodeIbltDifference(const IbltSketch& remote, const IbltSketch& local, uint32_t bucket)
        -> std::optional<IbltDiff>;

    [[nodiscard]]
    auto encodeMetaRows(const std::vector<MetaRow>& rows) -> std::string;

    [[nodiscard]]
    auto decodeMetaRows(std::string_view data) -> std::optional<std::vector<MetaRow>>;

    [[nodiscard]]
    auto encodeSync(const SyncMessage& msg, const Clock& clock) -> std::string;

    [[nodiscard]]
    auto decodeSync(std::string_view data, const Clock& clock) -> std::optional<SyncMessage>;

    // Parse one entry from the reader, advancing past it. Returns failure
    // reason or nullopt on success; key/entry are valid only on success.
    auto parseSingleEntry(
        detail::BufferReader& in,
        std::string& key,
        VersionedEntry& entry,
        const Clock& clock
    ) -> std::optional<std::string_view>;

    // Wire size of one serialized sync entry (values are never hashed).
    [[nodiscard]]
    auto syncEntryWireSize(const std::string& key, const VersionedEntry& entry) -> size_t;
} // namespace cinder::ae_codec
