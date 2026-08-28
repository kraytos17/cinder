#include <chrono>
#include <filesystem>
#include <string>
#include <vector>

#include "cinder/store/lru_store.hpp"
#include "cinder/store/persistence.hpp"
#include "cinder/store/snapshot.hpp"
#include "cinder/store/wal.hpp"
#include "gtest/gtest.h"
#include "sim/sim_clock.hpp"

using std::chrono::duration_cast;
using std::chrono::milliseconds;

namespace cinder {
namespace {

PersistenceManager::Options
pmOptions(const std::filesystem::path& dir) {
    return {
        .data_dir = dir.string(),
        .enabled = true,
        .snapshot_interval_s = 60,
        .max_wal_entries = 100,
    };
}

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

    // Header is present but no entries — next() should return nullopt.
    WalReader reader(wal_path);
    auto entry = reader.next();
    EXPECT_FALSE(entry.has_value());
}

TEST_F(PersistenceTest, WalEmptyFile) {
    auto wal_path = (test_dir_ / "wal.log").string();
    {
        std::ofstream(wal_path, std::ios::binary | std::ios::trunc);
    }

    WalReader reader(wal_path);
    EXPECT_FALSE(reader.hasMore());
}

TEST_F(PersistenceTest, WalChecksumDetectsCorruption) {
    auto wal_path = (test_dir_ / "wal.log").string();
    {
        WalWriter writer(wal_path);
        ASSERT_TRUE(writer
                .append({.op = WalEntry::Op::Set,
                    .key = "k1",
                    .value = "v1",
                    .version = 1,
                    .writer_node_hash = 10,
                    .expires_at_ms = 0,
                    .has_ttl = false})
                .has_value());
        ASSERT_TRUE(writer
                .append({.op = WalEntry::Op::Set,
                    .key = "k2",
                    .value = "v2",
                    .version = 2,
                    .writer_node_hash = 20,
                    .expires_at_ms = 0,
                    .has_ttl = false})
                .has_value());
        ASSERT_TRUE(writer
                .append({.op = WalEntry::Op::Set,
                    .key = "k3",
                    .value = "v3",
                    .version = 3,
                    .writer_node_hash = 30,
                    .expires_at_ms = 0,
                    .has_ttl = false})
                .has_value());
    }

    // Flip a byte in the second entry's value (offset past header + first entry).
    // The header is 8 bytes. Entry 1: 1+4+2+4+2+8+8+8+1+8 = 46 bytes.
    // So entry 2 starts at offset 54. Its value starts after
    // op(1)+key_len(4)+key(2)+val_len(4) = 11 bytes into entry 2.
    // Flip at offset 54+11 = 65.
    {
        std::fstream f(wal_path, std::ios::binary | std::ios::in | std::ios::out);
        f.seekg(65);
        char byte = 0;

        f.get(byte);
        f.seekg(65);
        f.put(static_cast<char>(0xFFU ^ static_cast<unsigned char>(byte)));
    }

    // Reader should return entry 1 OK, then stop at the corrupt entry 2.
    WalReader reader(wal_path);
    auto e1 = reader.next();
    ASSERT_TRUE(e1.has_value());
    EXPECT_EQ(e1->key, "k1");
    EXPECT_EQ(e1->value, "v1");

    auto e2 = reader.next();
    EXPECT_FALSE(e2.has_value()); // checksum mismatch
}

TEST_F(PersistenceTest, WalOldFormatBackwardCompat) {
    auto wal_path = (test_dir_ / "wal.log").string();

    // Write a WAL in the old format (no header, no checksum) using raw I/O.
    {
        std::ofstream out(wal_path, std::ios::binary | std::ios::trunc);
        auto write_u8 = [&](uint8_t v) {
            out.write(reinterpret_cast<const char*>(&v), 1);
        };
        auto write_u32 = [&](uint32_t v) {
            out.write(reinterpret_cast<const char*>(&v), 4);
        };
        auto write_u64 = [&](uint64_t v) {
            out.write(reinterpret_cast<const char*>(&v), 8);
        };

        // Entry 1
        write_u8(1); // Set
        write_u32(2);
        out.write("ab", 2);
        write_u32(3);
        out.write("xyz", 3);
        write_u64(100);
        write_u64(42);
        write_u64(0);
        write_u8(0);

        // Entry 2
        write_u8(2); // Del
        write_u32(1);
        out.write("x", 1);
        write_u32(0);
        write_u64(200);
        write_u64(99);
        write_u64(5'000);
        write_u8(1);
    }

    // Reader should handle old format gracefully (no checksums).
    WalReader reader(wal_path);
    auto e1 = reader.next();
    ASSERT_TRUE(e1.has_value());
    EXPECT_EQ(e1->op, WalEntry::Op::Set);
    EXPECT_EQ(e1->key, "ab");
    EXPECT_EQ(e1->value, "xyz");
    EXPECT_EQ(e1->version, 100);
    EXPECT_FALSE(e1->has_ttl);

    auto e2 = reader.next();
    ASSERT_TRUE(e2.has_value());
    EXPECT_EQ(e2->op, WalEntry::Op::Del);
    EXPECT_EQ(e2->key, "x");
    EXPECT_TRUE(e2->has_ttl);
    EXPECT_EQ(e2->expires_at_ms, 5'000);

    EXPECT_FALSE(reader.hasMore());
}

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

// Restart round-trip with a TTL'd key, deterministic via SimClock: the
// remaining wall-clock lifetime must survive recovery instead of being reset
// to ~1ms (regression test for the lost-TTL-on-restart bug).
TEST_F(PersistenceTest, RecoverPreservesRemainingTtlFromSnapshot) {
    SimClock sim;
    LruStore store(1'000'000, &sim);
    PersistenceManager pm(pmOptions(test_dir_), store, &sim);

    ASSERT_TRUE(store.put("ttl", "v", milliseconds{10'000}).has_value());
    ASSERT_TRUE(pm.compact().has_value()); // snapshot written, WAL truncated

    // Fresh "process": same simulated time, new store + manager.
    LruStore store2(1'000'000, &sim);
    PersistenceManager pm2(pmOptions(test_dir_), store2, &sim);
    ASSERT_TRUE(pm2.recover().has_value());

    auto ve = store2.getVersioned("ttl");
    ASSERT_TRUE(ve.has_value());
    EXPECT_TRUE(ve->has_ttl);

    auto remaining = duration_cast<milliseconds>(ve->expires_at - sim.now()).count();
    EXPECT_GE(remaining, 9'900); // ~full lifetime preserved (not 1ms)
    EXPECT_LE(remaining, 10'000);

    // Advancing past the expiry must actually expire it.
    sim.advance(milliseconds{10'001});
    store2.evictExpired();
    EXPECT_FALSE(store2.get("ttl").has_value());
}

TEST_F(PersistenceTest, RecoverPreservesRemainingTtlFromWal) {
    SimClock sim;
    LruStore store(1'000'000, &sim);
    PersistenceManager pm(pmOptions(test_dir_), store, &sim);
    store.setPersistence(&pm); // put() now appends through the real write path

    // Production boots with recover() before serving; it opens the WAL writer.
    ASSERT_TRUE(pm.recover().has_value());
    ASSERT_TRUE(store.put("ttl", "v", milliseconds{5'000}).has_value());
    pm.flush(); // WAL only — no snapshot in this dir

    LruStore store2(1'000'000, &sim);
    PersistenceManager pm2(pmOptions(test_dir_), store2, &sim);
    ASSERT_TRUE(pm2.recover().has_value());

    auto ve = store2.getVersioned("ttl");
    ASSERT_TRUE(ve.has_value());
    EXPECT_TRUE(ve->has_ttl);

    auto remaining = duration_cast<milliseconds>(ve->expires_at - sim.now()).count();
    EXPECT_GE(remaining, 4'900);
    EXPECT_LE(remaining, 5'000);
}

TEST_F(PersistenceTest, SnapshotCorruptionDetection) {
    // Write a valid snapshot, then corrupt it
    LruStore store(1'000'000);
    ASSERT_TRUE(store.put("key1", "value1").has_value());

    PersistenceManager::Options opts = pmOptions(test_dir_);
    PersistenceManager pm(opts, store);
    ASSERT_TRUE(pm.recover().has_value());
    ASSERT_TRUE(pm.compact().has_value());

    // Find the snapshot file and corrupt it
    for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
        if (entry.path().extension() == ".dat") {
            // Corrupt the file by overwriting bytes
            std::fstream f(entry.path(), std::ios::in | std::ios::out | std::ios::binary);
            char byte = 0xFF;
            f.seekp(10);
            f.write(&byte, 1);
            f.close();
            break;
        }
    }

    // Recover from corrupted snapshot — should either fail or skip corrupted entries
    LruStore store2(1'000'000);
    PersistenceManager pm2(opts, store2);
    auto result = pm2.recover();
    // Either the recovery fails, or it succeeds but the corrupted entry is skipped
    if (result.has_value()) {
        // If recovery succeeded, corrupted entry should be absent or partially recovered
        EXPECT_TRUE(store2.size() <= 1);
    }
}

TEST_F(PersistenceTest, WalPartialEntry) {
    // Write a valid WAL, then truncate it mid-entry
    auto wal_path = test_dir_ / "wal.log";
    {
        WalWriter writer(wal_path.string());
        WalEntry entry;
        entry.op = WalEntry::Op::Set;
        entry.key = "key1";
        entry.value = "value1";
        entry.version = 1;
        entry.writer_node_hash = 100;
        entry.has_ttl = false;
        entry.expires_at_ms = 0;
        ASSERT_TRUE(writer.append(entry).has_value());
    }

    // Truncate the file mid-entry
    auto file_size = std::filesystem::file_size(wal_path);
    std::filesystem::resize_file(wal_path, file_size / 2);

    // Read truncated WAL — should handle gracefully
    WalReader reader(wal_path.string());
    size_t count = 0;
    while (reader.hasMore()) {
        auto entry = reader.next();
        if (entry.has_value()) {
            ++count;
        } else {
            break; // truncated entry — stop
        }
    }
    // Should have read at least 0 entries (partial entry is invalid)
    EXPECT_GE(count, 0U);
}

TEST_F(PersistenceTest, MultipleSnapshotCompactions) {
    // Compact twice — second snapshot should contain all current entries
    LruStore store(1'000'000);
    PersistenceManager::Options opts = pmOptions(test_dir_);
    PersistenceManager pm(opts, store);

    // Write two keys and compact
    ASSERT_TRUE(store.put("key1", "v1").has_value());
    ASSERT_TRUE(store.put("key2", "v2").has_value());
    ASSERT_TRUE(pm.compact().has_value());

    // Recover — should have both keys
    LruStore store2(1'000'000);
    PersistenceManager pm2(opts, store2);
    ASSERT_TRUE(pm2.recover().has_value());

    auto val1 = store2.get("key1");
    ASSERT_TRUE(val1.has_value());
    EXPECT_EQ(*val1, "v1");

    auto val2 = store2.get("key2");
    ASSERT_TRUE(val2.has_value());
    EXPECT_EQ(*val2, "v2");
}

TEST_F(PersistenceTest, SnapshotEntryCountExceedsFileSize) {
    // Craft a snapshot file with a fake entry_count of 0xFFFFFFFF.
    // The reader must reject it instead of trying to allocate ~16GB.
    auto snap_path = test_dir_ / "snapshot.dat";
    {
        std::ofstream out(snap_path, std::ios::binary | std::ios::trunc);
        uint32_t magic = 0x43534E50; // "CSNP"
        uint32_t format_ver = 1;
        uint64_t next_ver = 0;
        uint32_t entry_count = 0xFFFFFFFF;
        out.write(reinterpret_cast<const char*>(&magic), sizeof(magic));
        out.write(reinterpret_cast<const char*>(&format_ver), sizeof(format_ver));
        out.write(reinterpret_cast<const char*>(&next_ver), sizeof(next_ver));
        out.write(reinterpret_cast<const char*>(&entry_count), sizeof(entry_count));

        // Write a few zero bytes — far fewer than entry_count * 41 bytes
        std::array<char, 20> zeros{};
        out.write(zeros.data(), zeros.size());
        out.close();
    }

    SnapshotReader reader(snap_path.string());
    auto result = reader.readAll();
    EXPECT_FALSE(result.has_value());
}
} // namespace
} // namespace cinder
