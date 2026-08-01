#include <gtest/gtest.h>

#include "cinder/store/lfu_store.hpp"
#include "cinder/store/lru_store.hpp"

namespace cinder {
namespace {

// used to supress clang -Wunneeded-internal-declaration
[[maybe_unused]] auto
makeVersionedEntry(const std::string& value, Version version, uint64_t writer = 0)
    -> VersionedEntry {
    VersionedEntry e;
    e.value = value;
    e.version = version;
    e.writer_node_hash = writer;
    return e;
}

using StoreTypes = ::testing::Types<LruStore, LfuStore>;

template <typename T> class VersionedStoreTest : public ::testing::Test {};

TYPED_TEST_SUITE(VersionedStoreTest, StoreTypes);

TYPED_TEST(VersionedStoreTest, NewerVersionWins) {
    TypeParam store(1'024);
    ASSERT_TRUE(store.putVersioned("k", makeVersionedEntry("v1", 1, 10)).has_value());
    ASSERT_TRUE(store.putVersioned("k", makeVersionedEntry("v2", 2, 10)).has_value());

    auto e = store.getVersioned("k");
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->value, "v2");
    EXPECT_EQ(e->version, 2);
}

TYPED_TEST(VersionedStoreTest, StaleVersionIgnored) {
    TypeParam store(1'024);
    ASSERT_TRUE(store.putVersioned("k", makeVersionedEntry("v2", 2, 10)).has_value());
    // Lower version replayed out of order → rejected, no-op success.
    auto res = store.putVersioned("k", makeVersionedEntry("v1", 1, 10));
    ASSERT_TRUE(res.has_value());
    auto e = store.getVersioned("k");
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(e->value, "v2");
}

TYPED_TEST(VersionedStoreTest, SameVersionTiebreakWriterHash) {
    TypeParam store(1'024);
    ASSERT_TRUE(store.putVersioned("k", makeVersionedEntry("w1", 1, 10)).has_value());
    // Same version, lower writer hash → rejected.
    ASSERT_TRUE(store.putVersioned("k", makeVersionedEntry("w0", 1, 5)).has_value());
    EXPECT_EQ(store.getVersioned("k")->value, "w1");
    // Same version, higher writer hash → accepted.
    ASSERT_TRUE(store.putVersioned("k", makeVersionedEntry("w2", 1, 20)).has_value());
    EXPECT_EQ(store.getVersioned("k")->value, "w2");
}

TYPED_TEST(VersionedStoreTest, PutAssignsMonotonicVersion) {
    TypeParam store(1'024);
    ASSERT_TRUE(store.put("k1", "a").has_value());
    ASSERT_TRUE(store.put("k1", "b").has_value());
    ASSERT_TRUE(store.put("k2", "c").has_value());
    // Overwrite of existing key must carry a higher version than the prior write.
    EXPECT_GT(store.getVersioned("k1")->version, 1);
    EXPECT_NE(store.getVersioned("k1")->version, store.getVersioned("k2")->version);
}

TYPED_TEST(VersionedStoreTest, TtlExpiresVersioned) {
    TypeParam store(1'024);
    auto entry = makeVersionedEntry("v", 1, 10);
    entry.has_ttl = true;
    entry.expires_at = std::chrono::steady_clock::now() - std::chrono::seconds(1);
    ASSERT_TRUE(store.putVersioned("k", entry).has_value());
    EXPECT_FALSE(store.getVersioned("k").has_value());
}

TYPED_TEST(VersionedStoreTest, GetVersionedMissing) {
    TypeParam store(1'024);
    EXPECT_FALSE(store.getVersioned("missing").has_value());
}
} // namespace
} // namespace cinder
