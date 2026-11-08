#include <algorithm>
#include <cstdint>
#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "cinder/node/anti_entropy/iblt.hpp"

namespace cinder {
    namespace {

        auto makeItems(size_t n, uint64_t base = 1) -> std::vector<iblt::Item> {
            std::vector<iblt::Item> rows;
            rows.reserve(n);
            for (uint64_t i = 0; i < n; ++i) {
                rows.push_back({
                    .key_hash = base + i,
                    .version = base + i,
                    .writer = 7,
                    .tombstone = false,
                });
            }
            return rows;
        }

        auto keySet(const std::vector<iblt::Item>& items) -> std::vector<uint64_t> {
            std::vector<uint64_t> keys;
            keys.reserve(items.size());
            for (const auto& item : items) {
                keys.push_back(item.key_hash);
            }
            std::ranges::sort(keys);
            return keys;
        }

        TEST(IbltTest, BuildDecodeRoundTrip) {
            auto rows = makeItems(40);
            auto sketch = iblt::build(rows, iblt::K_DEFAULT_CELLS);
            ASSERT_TRUE(sketch.has_value());
            ASSERT_EQ(sketch->cells.size(), iblt::K_DEFAULT_CELLS);
            auto blob = iblt::encode(*sketch);
            // Magic u32 + count u32 + fixed cells.
            EXPECT_EQ(
                blob.size(),
                2 * sizeof(uint32_t) + iblt::K_DEFAULT_CELLS * iblt::K_CELL_BYTES
            );
            auto back = iblt::decode(blob);
            ASSERT_TRUE(back.has_value());
            ASSERT_EQ(back->cells.size(), sketch->cells.size());
            for (size_t i = 0; i < sketch->cells.size(); ++i) {
                EXPECT_EQ(back->cells[i].count, sketch->cells[i].count);
                EXPECT_EQ(back->cells[i].key_hash_xor, sketch->cells[i].key_hash_xor);
                EXPECT_EQ(back->cells[i].checksum_xor, sketch->cells[i].checksum_xor);
            }
        }

        TEST(IbltTest, EmptySketchRoundTrips) {
            auto sketch = iblt::build({}, iblt::K_DEFAULT_CELLS);
            ASSERT_TRUE(sketch.has_value());
            auto back = iblt::decode(iblt::encode(*sketch));
            ASSERT_TRUE(back.has_value());
            EXPECT_TRUE(back->cells.empty() == false);
            for (const auto& cell : back->cells) {
                EXPECT_EQ(cell.count, 0);
            }
        }

        TEST(IbltTest, RejectsCellCountBounds) {
            EXPECT_FALSE(iblt::build(makeItems(4), iblt::K_HASH_COUNT - 1).has_value());
            EXPECT_FALSE(iblt::build(makeItems(4), iblt::K_MAX_CELLS + 1).has_value());
            auto good = iblt::build(makeItems(4), iblt::K_DEFAULT_CELLS);
            ASSERT_TRUE(good.has_value());
            auto blob = iblt::encode(*good);
            EXPECT_FALSE(iblt::decode("").has_value());
            EXPECT_FALSE(iblt::decode("garbage").has_value());
            EXPECT_FALSE(iblt::decode(blob.substr(0, blob.size() - 1)).has_value());
            std::string bad_magic = blob;
            bad_magic[0] = static_cast<char>(bad_magic[0] == 0 ? 1 : 0);
            EXPECT_FALSE(iblt::decode(bad_magic).has_value());
        }

        TEST(IbltTest, StreamingInsertMatchesBatchBuild) {
            auto rows = makeItems(40);
            auto batched = iblt::build(rows, iblt::K_DEFAULT_CELLS);
            ASSERT_TRUE(batched.has_value());
            auto streamed = iblt::build({}, iblt::K_DEFAULT_CELLS);
            ASSERT_TRUE(streamed.has_value());
            for (const auto& row : rows) {
                iblt::insert(*streamed, row.key_hash, row.version, row.writer, row.tombstone);
            }
            EXPECT_EQ(iblt::encode(*streamed), iblt::encode(*batched));
        }

        TEST(IbltTest, DifferenceSplitsSymmetricDifference) {
            // A = keys 1..10, B = keys 6..15: overlap peels away.
            auto a = iblt::build(makeItems(10, 1), iblt::K_DEFAULT_CELLS);
            auto b = iblt::build(makeItems(10, 6), iblt::K_DEFAULT_CELLS);
            ASSERT_TRUE(a.has_value() && b.has_value());
            auto diff = iblt::difference(*b, *a);
            ASSERT_TRUE(diff.has_value());
            EXPECT_EQ(keySet(diff->remote_only), (std::vector<uint64_t>{11, 12, 13, 14, 15}));
            EXPECT_EQ(keySet(diff->local_only), (std::vector<uint64_t>{1, 2, 3, 4, 5}));
        }

        TEST(IbltTest, IdenticalSketchesPeelEmpty) {
            auto a = iblt::build(makeItems(40), iblt::K_DEFAULT_CELLS);
            auto b = iblt::build(makeItems(40), iblt::K_DEFAULT_CELLS);
            ASSERT_TRUE(a.has_value() && b.has_value());
            auto diff = iblt::difference(*a, *b);
            ASSERT_TRUE(diff.has_value());
            EXPECT_TRUE(diff->remote_only.empty());
            EXPECT_TRUE(diff->local_only.empty());
        }

        TEST(IbltTest, OverloadedDifferenceFailsCleanly) {
            // 80 disjoint items per side in 64 cells: far past the peeling
            // threshold, must report failure rather than stall.
            auto local = iblt::build(makeItems(80, 1), iblt::K_DEFAULT_CELLS);
            auto remote = iblt::build(makeItems(80, 1001), iblt::K_DEFAULT_CELLS);
            ASSERT_TRUE(local.has_value() && remote.has_value());
            EXPECT_FALSE(iblt::difference(*remote, *local).has_value());
        }

        TEST(IbltTest, MismatchedCellCountsFail) {
            auto a = iblt::build(makeItems(10), 64);
            auto b = iblt::build(makeItems(10), 128);
            ASSERT_TRUE(a.has_value() && b.has_value());
            EXPECT_FALSE(iblt::difference(*a, *b).has_value());
        }

    } // namespace
} // namespace cinder
