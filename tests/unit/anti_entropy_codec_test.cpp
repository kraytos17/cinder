#include <algorithm>
#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <xxhash.h>

#include "cinder/node/anti_entropy.hpp"
#include "cinder/store/lru_store.hpp"
#include "unit/anti_entropy_fixtures.hpp"

namespace cinder {
    namespace {

        TEST(AntiEntropyDigestTest, EmptyStoresMatch) {
            Node a;
            Node b;
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
            AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);
            EXPECT_EQ(ma.computeDigest(), mb.computeDigest());
        }

        TEST(AntiEntropyDigestTest, IdenticalStoresSameDigest) {
            Node a;
            Node b;
            for (auto* n : {&a, &b}) {
                ASSERT_TRUE(n->store.putVersioned("k1", makeEntry("v1", 1)).has_value());
                ASSERT_TRUE(n->store.putVersioned("k2", makeEntry("v2", 2)).has_value());
                ASSERT_TRUE(n->store.putVersioned("k3", makeEntry("v3", 3)).has_value());
            }
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
            AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);
            EXPECT_EQ(ma.computeDigest(), mb.computeDigest());
        }

        TEST(AntiEntropyDigestTest, SameWriteSameDigestDespiteValue) {
            // (writer, version) uniquely identifies a write, so values are
            // excluded from the digest: same key/version/writer converges.
            Node a;
            Node b;
            ASSERT_TRUE(a.store.putVersioned("k1", makeEntry("v1", 1)).has_value());
            ASSERT_TRUE(b.store.putVersioned("k1", makeEntry("DIFFERENT", 1)).has_value());
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
            AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);
            EXPECT_EQ(ma.computeDigest(), mb.computeDigest());
        }

        TEST(AntiEntropyDigestTest, NewerVersionDifferentDigest) {
            Node a;
            Node b;
            ASSERT_TRUE(a.store.putVersioned("k1", makeEntry("v1", 1)).has_value());
            ASSERT_TRUE(b.store.putVersioned("k1", makeEntry("v1", 2)).has_value());
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
            AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);
            EXPECT_NE(ma.computeDigest(), mb.computeDigest());
        }

        TEST(AntiEntropyDigestTest, InsertionOrderIndependent) {
            Node a;
            Node b;
            ASSERT_TRUE(a.store.putVersioned("k1", makeEntry("v1", 1)).has_value());
            ASSERT_TRUE(a.store.putVersioned("k2", makeEntry("v2", 2)).has_value());
            // Same data, reverse insertion order (different LRU recency order).
            ASSERT_TRUE(b.store.putVersioned("k2", makeEntry("v2", 2)).has_value());
            ASSERT_TRUE(b.store.putVersioned("k1", makeEntry("v1", 1)).has_value());
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
            AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);
            EXPECT_EQ(ma.computeDigest(), mb.computeDigest());
        }

        TEST(AntiEntropyEncodeTest, DigestRoundTrip) {
            std::vector<uint64_t> digest = {1, 2, 3, 0xFFFFFFFFFFFFFFFFULL};
            auto blob = AntiEntropyManager::encodeDigest(digest);
            auto back = AntiEntropyManager::decodeDigest(blob, digest.size());
            ASSERT_TRUE(back.has_value());
            EXPECT_EQ(*back, digest);
        }

        TEST(AntiEntropyEncodeTest, DigestRejectsBadInput) {
            std::vector<uint64_t> digest = {1, 2, 3};
            auto blob = AntiEntropyManager::encodeDigest(digest);
            EXPECT_FALSE(AntiEntropyManager::decodeDigest(blob, digest.size() + 1).has_value());
            EXPECT_FALSE(
                AntiEntropyManager::decodeDigest(blob.substr(0, blob.size() - 1), 3).has_value()
            );
            EXPECT_FALSE(AntiEntropyManager::decodeDigest("", 3).has_value());
            EXPECT_FALSE(AntiEntropyManager::decodeDigest(blob + "x", 3).has_value());
        }

        TEST(AntiEntropyEncodeTest, LegacySchemaRejected) {
            // v1 layout: bare count-first blob with no magic. Must fail
            // cleanly so mixed-version peers skip the round.
            std::string legacy;
            uint32_t n = 3;
            legacy.append(reinterpret_cast<const char*>(&n), sizeof(n));
            for (uint64_t h : {1ULL, 2ULL, 3ULL}) {
                legacy.append(reinterpret_cast<const char*>(&h), sizeof(h));
            }
            EXPECT_FALSE(AntiEntropyManager::decodeDigest(legacy, 3).has_value());
        }

        TEST(AntiEntropyEncodeTest, MetaRowsRoundTrip) {
            Node a;
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
            std::vector<AntiEntropyManager::MetaRow> rows = {
                {0, false, 11, 1, 7},
                {0, false, 22, 2, 7},
                {5, false, 33, 1, 9},
            };
            auto blob = AntiEntropyManager::encodeMetaRows(rows);
            auto back = AntiEntropyManager::decodeMetaRows(blob);
            ASSERT_TRUE(back.has_value());
            ASSERT_EQ(back->size(), 3U);
            for (size_t i = 0; i < 3; ++i) {
                EXPECT_EQ((*back)[i].bucket, rows[i].bucket);
                EXPECT_EQ((*back)[i].key_hash, rows[i].key_hash);
                EXPECT_EQ((*back)[i].version, rows[i].version);
                EXPECT_EQ((*back)[i].writer, rows[i].writer);
            }

            auto empty = AntiEntropyManager::encodeMetaRows({});
            auto back_empty = AntiEntropyManager::decodeMetaRows(empty);
            ASSERT_TRUE(back_empty.has_value());
            EXPECT_TRUE(back_empty->empty());

            EXPECT_FALSE(
                AntiEntropyManager::decodeMetaRows(blob.substr(0, blob.size() - 1)).has_value()
            );
            EXPECT_FALSE(AntiEntropyManager::decodeMetaRows("").has_value());
            EXPECT_FALSE(AntiEntropyManager::decodeMetaRows("garbage-blob").has_value());
        }

        TEST(AntiEntropyEncodeTest, SyncRoundTrip) {
            Node a;
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
            AntiEntropyManager::SyncMessage msg;
            msg.entries.push_back({"k1", makeEntry("v1", 3)});
            msg.entries.push_back({"k2", makeEntry("v2", 4)});
            msg.needs = {11, 22};
            auto blob = ma.encodeSync(msg);
            auto back = ma.decodeSync(blob);
            ASSERT_TRUE(back.has_value());
            ASSERT_EQ(back->entries.size(), 2U);
            EXPECT_EQ(back->entries[0].key, "k1");
            EXPECT_EQ(back->entries[0].entry.value, "v1");
            EXPECT_EQ(back->entries[0].entry.version(), 3U);
            EXPECT_EQ(back->entries[1].key, "k2");
            EXPECT_EQ(back->needs, msg.needs);

            EXPECT_FALSE(ma.decodeSync("").has_value());
            EXPECT_FALSE(ma.decodeSync("garbage").has_value());
            EXPECT_FALSE(ma.decodeSync(blob.substr(0, blob.size() - 1)).has_value());
        }

        TEST(AntiEntropyIbltTest, TombstoneRowsPeelWithIdentity) {
            using Manager = AntiEntropyManager;
            constexpr uint32_t BUCKET = 7;
            // Same key, live v1 vs tombstone v2: distinct IBLT items.
            std::vector<Manager::MetaRow> local_rows{
                {BUCKET, false, 10, 1, 100},
                {BUCKET, false, 20, 1, 100},
            };
            std::vector<Manager::MetaRow> remote_rows{
                {BUCKET, false, 10, 1, 100},
                {BUCKET, true, 20, 2, 100},
            };
            auto local = Manager::buildIblt(local_rows, 64);
            auto remote = Manager::buildIblt(remote_rows, 64);
            ASSERT_TRUE(local.has_value());
            ASSERT_TRUE(remote.has_value());
            auto decoded = Manager::decodeIbltDifference(*remote, *local, BUCKET);
            ASSERT_TRUE(decoded.has_value());
            ASSERT_EQ(decoded->remote_only.size(), 1U);
            ASSERT_EQ(decoded->local_only.size(), 1U);
            EXPECT_TRUE(decoded->remote_only[0].tombstone);
            EXPECT_EQ(decoded->remote_only[0].version, 2U);
            EXPECT_FALSE(decoded->local_only[0].tombstone);
        }

        TEST(AntiEntropyEncodeTest, HierarchicalDigestRoundTripAndValidation) {
            const std::vector<uint64_t> buckets = {1, 2, 3, 4, 5, 6, 7, 8, 9};
            auto groups = AntiEntropyManager::groupDigests(buckets);
            ASSERT_EQ(groups.size(), 1U);

            AntiEntropyManager::DigestResponse response{
                .group_hashes = groups,
                .bucket_hashes = {{0, 1}, {1, 2}, {8, 9}},
                .covered_buckets = {0, 1, 8},
                .rows = {{0, false, 11, 1, 7}, {1, false, 22, 2, 7}},
            };

            auto blob = AntiEntropyManager::encodeDigestResponse(response);
            auto decoded = AntiEntropyManager::decodeDigestResponse(blob, groups.size());
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded->group_hashes, groups);
            EXPECT_EQ(decoded->bucket_hashes, response.bucket_hashes);
            EXPECT_EQ(decoded->covered_buckets, response.covered_buckets);
            ASSERT_EQ(decoded->rows.size(), 2U);
            EXPECT_EQ(decoded->rows[0].key_hash, 11U);

            auto sketch = AntiEntropyManager::buildIblt(response.rows, 64);
            ASSERT_TRUE(sketch.has_value());
            response.iblt_sketches.emplace_back(8, AntiEntropyManager::encodeIblt(*sketch));
            blob = AntiEntropyManager::encodeDigestResponse(response);
            decoded = AntiEntropyManager::decodeDigestResponse(blob, groups.size());
            ASSERT_TRUE(decoded.has_value());
            ASSERT_EQ(decoded->iblt_sketches.size(), 1U);
            EXPECT_EQ(decoded->iblt_sketches[0].first, 8U);
            EXPECT_TRUE(AntiEntropyManager::decodeIblt(decoded->iblt_sketches[0].second));

            EXPECT_FALSE(AntiEntropyManager::decodeGroupDigest(blob, groups.size()).has_value());
            EXPECT_FALSE(
                AntiEntropyManager::decodeDigestResponse(
                    std::string_view(blob).substr(0, blob.size() - 1),
                    groups.size()
                )
                    .has_value()
            );
        }

        TEST(AntiEntropyIbltTest, PeelsSymmetricDifferenceIncludingVersionUpdates) {
            using Manager = AntiEntropyManager;
            constexpr uint32_t BUCKET = 7;
            std::vector<Manager::MetaRow> local_rows{
                {BUCKET, false, 10, 1, 100},
                {BUCKET, false, 20, 1, 100},
                {BUCKET, false, 30, 4, 100},
            };
            std::vector<Manager::MetaRow> remote_rows{
                {BUCKET, false, 10, 1, 100},
                {BUCKET, false, 20, 2, 100}, // same key, newer LWW identity
                {BUCKET, false, 40, 1, 200},
            };
            auto local = Manager::buildIblt(local_rows, 64);
            auto remote = Manager::buildIblt(remote_rows, 64);
            ASSERT_TRUE(local.has_value());
            ASSERT_TRUE(remote.has_value());
            auto decoded = Manager::decodeIbltDifference(*remote, *local, BUCKET);
            ASSERT_TRUE(decoded.has_value());
            ASSERT_EQ(decoded->remote_only.size(), 2U);
            ASSERT_EQ(decoded->local_only.size(), 2U);
            EXPECT_TRUE(std::ranges::any_of(decoded->remote_only, [](const auto& row) {
                return row.key_hash == 20 && row.version == 2;
            }));
            EXPECT_TRUE(std::ranges::any_of(decoded->local_only, [](const auto& row) {
                return row.key_hash == 20 && row.version == 1;
            }));
        }

        TEST(AntiEntropyIbltTest, CodecAndInvalidSketchValidation) {
            using Manager = AntiEntropyManager;
            std::vector<Manager::MetaRow> rows;
            for (uint64_t i = 0; i < 40; ++i) {
                rows.push_back({3, false, i + 1, i + 2, i + 3});
            }
            auto sketch = Manager::buildIblt(rows, 64);
            ASSERT_TRUE(sketch.has_value());
            auto blob = Manager::encodeIblt(*sketch);
            auto decoded = Manager::decodeIblt(blob);
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded->cells.size(), 64U);
            EXPECT_FALSE(Manager::decodeIblt(std::string_view(blob).substr(0, blob.size() - 1)));
            EXPECT_FALSE(Manager::buildIblt(rows, Manager::K_MAX_IBLT_CELLS + 1));
        }

        TEST(AntiEntropyIbltTest, UnpeelableDifferenceRequestsFallback) {
            using Manager = AntiEntropyManager;
            std::vector<Manager::MetaRow> local_rows;
            std::vector<Manager::MetaRow> remote_rows;
            for (uint64_t i = 0; i < 80; ++i) {
                local_rows.push_back({9, false, i + 1, 1, 5});
                remote_rows.push_back({9, false, i + 1001, 1, 5});
            }
            auto local = Manager::buildIblt(local_rows, 64);
            auto remote = Manager::buildIblt(remote_rows, 64);
            ASSERT_TRUE(local.has_value());
            ASSERT_TRUE(remote.has_value());
            EXPECT_FALSE(Manager::decodeIbltDifference(*remote, *local, 9).has_value());
        }

        TEST(AntiEntropyIbltTest, EmptySketchesPeelToEmptyDifference) {
            using Manager = AntiEntropyManager;
            auto local = Manager::buildIblt({}, 64);
            auto remote = Manager::buildIblt({}, 64);
            ASSERT_TRUE(local.has_value());
            ASSERT_TRUE(remote.has_value());
            auto diff = Manager::decodeIbltDifference(*remote, *local, 0);
            ASSERT_TRUE(diff.has_value());
            EXPECT_TRUE(diff->remote_only.empty());
            EXPECT_TRUE(diff->local_only.empty());
        }

        TEST(AntiEntropyIbltTest, RequestCodecValidatesFallbackFlag) {
            AntiEntropyManager::BucketRefinement request{
                .bucket_hashes = {{3, 0xfeedU}},
                .force_metadata = true,
            };
            auto blob = AntiEntropyManager::encodeBucketRefinement(request);
            auto decoded = AntiEntropyManager::decodeBucketRefinement(blob, 16);
            ASSERT_TRUE(decoded.has_value());
            EXPECT_TRUE(decoded->force_metadata);
            EXPECT_EQ(decoded->bucket_hashes, request.bucket_hashes);
            blob[4] = static_cast<char>(0x02); // unknown flag bit
            EXPECT_FALSE(AntiEntropyManager::decodeBucketRefinement(blob, 16));
        }

        TEST(AntiEntropyDigestTest, GroupDigestChangesOnlyContainingGroup) {
            Node a;
            Node b;
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 32);
            AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 32);
            auto before_a = AntiEntropyManager::groupDigests(ma.computeDigest());
            auto before_b = AntiEntropyManager::groupDigests(mb.computeDigest());
            EXPECT_EQ(before_a, before_b);

            ASSERT_TRUE(b.store.putVersioned("one-key", makeEntry("v", 1)).has_value());
            auto after_b = AntiEntropyManager::groupDigests(mb.computeDigest());
            size_t changed = 0;
            for (size_t i = 0; i < before_a.size(); ++i) {
                changed += before_a[i] != after_b[i] ? 1U : 0U;
            }
            EXPECT_EQ(changed, 1U);
        }

    } // namespace
} // namespace cinder
