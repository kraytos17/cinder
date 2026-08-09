#include <chrono>
#include <gtest/gtest.h>
#include <map>

#include "cinder/store/lfu_store.hpp"
#include "cinder/store/lru_store.hpp"

using std::chrono::milliseconds;
using std::chrono::seconds;
using std::chrono::steady_clock;

namespace cinder {
namespace {

// Deterministic clock for the restart-seed test.
class TestClock final : public Clock {
  public:

    auto now() const -> steady_clock::time_point override { return now_; }

    void advance(milliseconds d) { now_ += d; }

  private:

    steady_clock::time_point now_{};
};

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
    entry.expires_at = steady_clock::now() - seconds(1);
    ASSERT_TRUE(store.putVersioned("k", entry).has_value());
    EXPECT_FALSE(store.getVersioned("k").has_value());
}

TYPED_TEST(VersionedStoreTest, GetVersionedMissing) {
    TypeParam store(1'024);
    EXPECT_FALSE(store.getVersioned("missing").has_value());
}

TYPED_TEST(VersionedStoreTest, MintVersionIsMonotonic) {
    TypeParam store(1'024);
    Version a = store.mintVersion();
    Version b = store.mintVersion();
    EXPECT_GT(b, a);

    // Interleaved with put(): versions keep increasing.
    ASSERT_TRUE(store.put("k", "v").has_value());
    Version c = store.mintVersion();
    EXPECT_GT(c, b);
    EXPECT_GT(store.getVersioned("k")->version, a);
}

TYPED_TEST(VersionedStoreTest, LamportBumpOnAcceptedWrite) {
    TypeParam store(1'024);
    // Accept a replicated write with a high version.
    ASSERT_TRUE(store.putVersioned("k", makeVersionedEntry("v", 5, 10)).has_value());

    // Next minted version advances past the observed version.
    EXPECT_GE(store.mintVersion(), 6);

    // A stale write (version 2) must NOT advance the counter.
    TypeParam store2(1'024);
    ASSERT_TRUE(store2.putVersioned("k", makeVersionedEntry("high", 100, 10)).has_value());
    ASSERT_TRUE(store2.putVersioned("k", makeVersionedEntry("stale", 2, 10)).has_value());
    EXPECT_GE(store2.mintVersion(), 101);
}

TYPED_TEST(VersionedStoreTest, RestartSeedWins) {
    TestClock clock;
    // Store A "runs" at a low time seed.
    TypeParam store_a(1'024, &clock);
    auto old = store_a.mintVersion();

    // A restart gap passes; a fresh store seeds from the advanced clock.
    clock.advance(seconds(5));
    TypeParam store_b(1'024, &clock);
    EXPECT_GT(store_b.mintVersion(), old);

    // The restarted node's write wins LWW when both versions reach one store.
    TypeParam store_c(1'024, &clock);
    ASSERT_TRUE(store_c.putVersioned("k", makeVersionedEntry("old", old, 10)).has_value());
    ASSERT_TRUE(store_c.putVersioned("k", makeVersionedEntry("new", store_b.mintVersion(), 10))
            .has_value());
    EXPECT_EQ(store_c.getVersioned("k")->value, "new");
}

TYPED_TEST(VersionedStoreTest, ForEachVisitsAllKeys) {
    TypeParam store(1'048'576); // ample capacity — no eviction
    std::map<std::string, std::string> expected;
    for (int i = 0; i < 10; i++) {
        auto key = "key" + std::to_string(i);
        auto value = "v" + std::to_string(i);
        ASSERT_TRUE(store.put(key, value).has_value());
        expected[key] = value;
    }

    std::map<std::string, std::string> visited;
    store.forEach(
        [&](const std::string& key, const VersionedEntry& entry) { visited[key] = entry.value; });
    EXPECT_EQ(visited, expected);
}

TYPED_TEST(VersionedStoreTest, ForEachSkipsExpired) {
    TestClock clock;
    TypeParam store(1'048'576, &clock);

    auto entry = makeVersionedEntry("gone", 1, 0);
    entry.has_ttl = true;
    entry.expires_at = clock.now() - seconds(1); // relative to injected clock
    ASSERT_TRUE(store.putVersioned("gone", entry).has_value());

    size_t count = 0;
    store.forEach([&](const std::string&, const VersionedEntry&) { ++count; });
    EXPECT_EQ(count, 0);
}

TYPED_TEST(VersionedStoreTest, EvictExpiredPurgesAcrossSlots) {
    TestClock clock;
    TypeParam store(1'024, &clock);

    ASSERT_TRUE(store.put("a", "1", seconds(3)).has_value());
    ASSERT_TRUE(store.put("b", "2", seconds(1)).has_value());

    clock.advance(seconds(3) + milliseconds(500));
    EXPECT_EQ(store.evictExpired(), 2);
    EXPECT_EQ(store.size(), 0);
}

TYPED_TEST(VersionedStoreTest, EvictExpiredReSchedulesNotYetExpired) {
    TestClock clock;
    TypeParam store(1'024, &clock);

    ASSERT_TRUE(store.put("a", "1", seconds(5)).has_value());
    clock.advance(seconds(2));
    EXPECT_EQ(store.evictExpired(), 0); // slot not fired yet
    EXPECT_EQ(store.size(), 1);

    clock.advance(seconds(4)); // now past expiry
    EXPECT_EQ(store.evictExpired(), 1);
    EXPECT_EQ(store.size(), 0);
}

TYPED_TEST(VersionedStoreTest, EvictExpiredWrapLongTtl) {
    TestClock clock;
    TypeParam store(1'024, &clock);

    // 300s > wheel wrap (256 slots); catch-up must still reap it.
    ASSERT_TRUE(store.put("a", "1", seconds(300)).has_value());
    clock.advance(seconds(300));
    EXPECT_EQ(store.evictExpired(), 1);
    EXPECT_EQ(store.size(), 0);
}

TYPED_TEST(VersionedStoreTest, OverwriteMovesWheelSlot) {
    TestClock clock;
    TypeParam store(1'024, &clock);

    ASSERT_TRUE(store.put("a", "1", seconds(2)).has_value());
    clock.advance(seconds(1) + milliseconds(500));
    // Overwrite with a shorter TTL: the old wheel slot must not leak.
    ASSERT_TRUE(store.put("a", "2", seconds(1)).has_value());

    clock.advance(milliseconds(600));
    EXPECT_EQ(store.evictExpired(), 0); // not expired yet, re-scheduled
    EXPECT_EQ(store.size(), 1);

    clock.advance(milliseconds(500));
    EXPECT_EQ(store.evictExpired(), 1);
    EXPECT_EQ(store.size(), 0);
}

TYPED_TEST(VersionedStoreTest, RemoveClearsWheelSlot) {
    TestClock clock;
    TypeParam store(1'024, &clock);

    ASSERT_TRUE(store.put("a", "1", seconds(1)).has_value());
    EXPECT_TRUE(store.remove("a"));

    clock.advance(seconds(2));
    EXPECT_EQ(store.evictExpired(), 0); // wheel slot was cleared on remove
    EXPECT_EQ(store.size(), 0);
}

TYPED_TEST(VersionedStoreTest, NonTtlOverwriteClearsWheelSlot) {
    TestClock clock;
    TypeParam store(1'024, &clock);

    ASSERT_TRUE(store.put("a", "1", seconds(1)).has_value());
    ASSERT_TRUE(store.put("a", "2").has_value()); // TTL dropped

    clock.advance(seconds(2));
    EXPECT_EQ(store.evictExpired(), 0);
    EXPECT_EQ(store.size(), 1);
    auto result = store.get("a");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "2");
}
} // namespace
} // namespace cinder
