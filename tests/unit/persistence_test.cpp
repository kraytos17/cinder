#include <filesystem>
#include <string>
#include <vector>

#include "cinder/store/lru_store.hpp"
#include "cinder/store/persistence.hpp"
#include "cinder/store/snapshot.hpp"
#include "cinder/store/wal.hpp"
#include "gtest/gtest.h"

namespace cinder {
namespace {

class PersistenceTest : public ::testing::Test {
  protected:

    void SetUp() override {
        test_dir_ =
            std::filesystem::temp_directory_path()
            / ("cinder_persistence_test_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::create_directories(test_dir_);
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(test_dir_, ec);
    }

    std::filesystem::path test_dir_;
};

TEST_F(PersistenceTest, WalAppendAndRead) {
    auto wal_path = (test_dir_ / "wal.log").string();
    {
        WalWriter writer(wal_path);
        ASSERT_TRUE(writer
                .append(WalEntry{
                    .op = WalEntry::Op::Set,
                    .key = "k1",
                    .value = "v1",
                    .version = 1,
                    .writer_node_hash = 100,
                    .expires_at_ms = 0,
                    .has_ttl = false,
                })
                .has_value());
        ASSERT_TRUE(writer
                .append(WalEntry{
                    .op = WalEntry::Op::Set,
                    .key = "k2",
                    .value = "v2",
                    .version = 2,
                    .writer_node_hash = 200,
                    .expires_at_ms = 1'234'567'890,
                    .has_ttl = true,
                })
                .has_value());
        ASSERT_TRUE(writer
                .append(WalEntry{
                    .op = WalEntry::Op::Del,
                    .key = "k3",
                    .value = "",
                    .version = 3,
                    .writer_node_hash = 300,
                    .expires_at_ms = 0,
                    .has_ttl = false,
                })
                .has_value());
    }

    WalReader reader(wal_path);
    ASSERT_TRUE(reader.hasMore());

    auto e1 = reader.next();
    ASSERT_TRUE(e1.has_value());
    EXPECT_EQ(e1->op, WalEntry::Op::Set);
    EXPECT_EQ(e1->key, "k1");
    EXPECT_EQ(e1->value, "v1");
    EXPECT_EQ(e1->version, 1);
    EXPECT_EQ(e1->writer_node_hash, 100);
    EXPECT_FALSE(e1->has_ttl);

    auto e2 = reader.next();
    ASSERT_TRUE(e2.has_value());
    EXPECT_EQ(e2->op, WalEntry::Op::Set);
    EXPECT_EQ(e2->key, "k2");
    EXPECT_EQ(e2->expires_at_ms, 1'234'567'890);
    EXPECT_TRUE(e2->has_ttl);

    auto e3 = reader.next();
    ASSERT_TRUE(e3.has_value());
    EXPECT_EQ(e3->op, WalEntry::Op::Del);
    EXPECT_EQ(e3->key, "k3");

    EXPECT_FALSE(reader.hasMore());
}

TEST_F(PersistenceTest, WalTruncate) {
    auto wal_path = (test_dir_ / "wal.log").string();
    {
        WalWriter writer(wal_path);
        ASSERT_TRUE(writer
                .append(WalEntry{
                    .op = WalEntry::Op::Set,
                    .key = "k",
                    .value = "v",
                    .version = 1,
                    .writer_node_hash = 0,
                    .expires_at_ms = 0,
                    .has_ttl = false,
                })
                .has_value());
    }

    // Truncate by re-opening with trunc
    {
        WalWriter writer(wal_path);
        writer.flush();
    }

    WalReader reader(wal_path);
    EXPECT_FALSE(reader.hasMore());
}

TEST_F(PersistenceTest, WalEmptyFile) {
    auto wal_path = (test_dir_ / "wal.log").string();
    {
        std::ofstream(wal_path, std::ios::binary | std::ios::trunc);
    }

    WalReader reader(wal_path);
    EXPECT_FALSE(reader.hasMore());
}

// --- Snapshot Tests ---

TEST_F(PersistenceTest, SnapshotWriteAndRead) {
    auto snap_path = (test_dir_ / "snapshot.dat").string();
    std::vector<SnapshotEntry> entries;
    entries.push_back(SnapshotEntry{
        .key = "a",
        .value = "alpha",
        .version = 10,
        .writer_node_hash = 100,
        .expires_at_ms = 9'999,
        .has_ttl = true,
        .freq = 5,
    });
    entries.push_back(SnapshotEntry{
        .key = "b",
        .value = "beta",
        .version = 20,
        .writer_node_hash = 200,
        .expires_at_ms = 0,
        .has_ttl = false,
        .freq = 0,
    });

    {
        SnapshotWriter writer(snap_path);
        ASSERT_TRUE(writer.write(42, entries).has_value());
    }

    SnapshotReader reader(snap_path);
    auto result = reader.readAll();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->next_version, 42);
    ASSERT_EQ(result->entries.size(), 2U);

    EXPECT_EQ(result->entries[0].key, "a");
    EXPECT_EQ(result->entries[0].value, "alpha");
    EXPECT_EQ(result->entries[0].version, 10);
    EXPECT_TRUE(result->entries[0].has_ttl);
    EXPECT_EQ(result->entries[0].expires_at_ms, 9'999);

    EXPECT_EQ(result->entries[1].key, "b");
    EXPECT_FALSE(result->entries[1].has_ttl);
}

// --- PersistenceManager Tests ---

TEST_F(PersistenceTest, RecoverFromSnapshotPlusWal) {
    LruStore store(1'000'000);
    PersistenceManager pm(
        PersistenceManager::Options{
            .data_dir = test_dir_.string(),
            .enabled = true,
            .snapshot_interval_s = 60,
            .max_wal_entries = 100,
        },
        store);

    // Seed the store
    ASSERT_TRUE(store.put("k1", "v1").has_value());
    ASSERT_TRUE(store.put("k2", "v2").has_value());
    ASSERT_TRUE(store.put("k3", "v3").has_value());

    // Compact: creates snapshot, truncates WAL
    ASSERT_TRUE(pm.compact().has_value());
    EXPECT_EQ(store.size(), 3U);

    // Write more data (goes to WAL)
    ASSERT_TRUE(store.put("k4", "v4").has_value());
    pm.onWrite(WalEntry{
        .op = WalEntry::Op::Set,
        .key = "k4",
        .value = "v4",
        .version = 4,
        .writer_node_hash = 0,
        .expires_at_ms = 0,
        .has_ttl = false,
    });
    pm.flush();

    // Create a fresh store and recover
    LruStore store2(1'000'000);
    PersistenceManager pm2(
        PersistenceManager::Options{
            .data_dir = test_dir_.string(),
            .enabled = true,
            .snapshot_interval_s = 60,
            .max_wal_entries = 100,
        },
        store2);

    ASSERT_TRUE(pm2.recover().has_value());

    // All 4 keys should be present
    EXPECT_EQ(store2.size(), 4U);
    EXPECT_TRUE(store2.get("k1").has_value());
    EXPECT_TRUE(store2.get("k2").has_value());
    EXPECT_TRUE(store2.get("k3").has_value());
    EXPECT_TRUE(store2.get("k4").has_value());
}

TEST_F(PersistenceTest, RecoverSkipsExpiredEntries) {
    // Pre-populate a snapshot with an expired entry
    auto snap_path = test_dir_ / "snapshot_1000.dat";
    std::vector<SnapshotEntry> entries;
    entries.push_back(SnapshotEntry{
        .key = "alive",
        .value = "yes",
        .version = 1,
        .writer_node_hash = 0,
        .expires_at_ms = 0,
        .has_ttl = false,
        .freq = 0,
    });
    entries.push_back(SnapshotEntry{
        .key = "expired",
        .value = "no",
        .version = 2,
        .writer_node_hash = 0,
        .expires_at_ms = 1, // way in the past
        .has_ttl = true,
        .freq = 0,
    });
    {
        SnapshotWriter writer(snap_path.string());
        ASSERT_TRUE(writer.write(3, entries).has_value());
    }

    LruStore store(1'000'000);
    PersistenceManager pm(
        PersistenceManager::Options{
            .data_dir = test_dir_.string(),
            .enabled = true,
            .snapshot_interval_s = 60,
            .max_wal_entries = 100,
        },
        store);

    ASSERT_TRUE(pm.recover().has_value());
    EXPECT_EQ(store.size(), 1U);
    EXPECT_TRUE(store.get("alive").has_value());
}

TEST_F(PersistenceTest, CompactTruncatesWal) {
    LruStore store(1'000'000);
    PersistenceManager pm(
        PersistenceManager::Options{
            .data_dir = test_dir_.string(),
            .enabled = true,
            .snapshot_interval_s = 60,
            .max_wal_entries = 100,
        },
        store);

    ASSERT_TRUE(store.put("k1", "v1").has_value());
    pm.onWrite(WalEntry{
        .op = WalEntry::Op::Set,
        .key = "k1",
        .value = "v1",
        .version = 1,
        .writer_node_hash = 0,
        .expires_at_ms = 0,
        .has_ttl = false,
    });

    ASSERT_TRUE(pm.compact().has_value());

    // WAL should be empty after compact
    auto wal_path = test_dir_ / "wal.log";
    WalReader reader(wal_path.string());
    EXPECT_FALSE(reader.hasMore());

    // Snapshot should exist
    bool found_snapshot = false;
    for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
        auto name = entry.path().filename().string();
        if (name.starts_with("snapshot_") && name.substr(name.size() - 4) == ".dat") {
            found_snapshot = true;
            break;
        }
    }
    EXPECT_TRUE(found_snapshot);
}

TEST_F(PersistenceTest, PersistenceDisabled) {
    LruStore store(1'000'000);
    PersistenceManager pm(
        PersistenceManager::Options{
            .data_dir = test_dir_.string(),
            .enabled = false,
        },
        store);

    EXPECT_FALSE(pm.enabled());
    EXPECT_TRUE(pm.recover().has_value());
    pm.onWrite(WalEntry{
        .op = WalEntry::Op::Set,
        .key = "k",
        .value = "v",
        .version = 1,
        .writer_node_hash = 0,
        .expires_at_ms = 0,
        .has_ttl = false,
    });
    EXPECT_TRUE(pm.compact().has_value());
}

} // namespace
} // namespace cinder
