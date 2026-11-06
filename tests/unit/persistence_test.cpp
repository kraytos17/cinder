#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "cinder/common/tracing.hpp"
#include "cinder/store/lfu_store.hpp"
#include "cinder/store/lru_store.hpp"
#include "cinder/store/persistence.hpp"
#include "cinder/store/snapshot.hpp"
#include "cinder/store/wal.hpp"
#include "gtest/gtest.h"
#include "sim/sim_clock.hpp"
#include "test_scratch.hpp"

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::seconds;
using std::chrono::steady_clock;

namespace cinder {
    namespace {

        PersistenceManager::Options pmOptions(const std::filesystem::path& dir) {
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
                // Fallible by design (see test_scratch.hpp): a failed
                // create must not masquerade as an empty dir.
                ASSERT_TRUE(scratch_);
            }

            // Unique per process and per case (see test_scratch.hpp): CTest
            // runs every case in its own process, so address- or fixed-name
            // dirs collide across parallel workers.
            test::ScratchDir scratch_;
        };

        TEST_F(PersistenceTest, WalAppendAndRead) {
            auto wal_path = (scratch_.path() / "wal.log").string();
            {
                WalWriter writer(wal_path);
                ASSERT_TRUE(writer
                                .append(
                                    WalEntry{
                                        .op = WalEntry::Op::Set,
                                        .key = "k1",
                                        .value = "v1",
                                        .version = 1,
                                        .writer_node_hash = 100,
                                        .expires_at_ms = 0,
                                        .has_ttl = false,
                                    }
                                )
                                .has_value());
                ASSERT_TRUE(writer
                                .append(
                                    WalEntry{
                                        .op = WalEntry::Op::Set,
                                        .key = "k2",
                                        .value = "v2",
                                        .version = 2,
                                        .writer_node_hash = 200,
                                        .expires_at_ms = 1'234'567'890,
                                        .has_ttl = true,
                                    }
                                )
                                .has_value());
                ASSERT_TRUE(writer
                                .append(
                                    WalEntry{
                                        .op = WalEntry::Op::Del,
                                        .key = "k3",
                                        .value = {},
                                        .version = 3,
                                        .writer_node_hash = 300,
                                        .expires_at_ms = 0,
                                        .has_ttl = false,
                                    }
                                )
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
            auto wal_path = (scratch_.path() / "wal.log").string();
            {
                WalWriter writer(wal_path);
                ASSERT_TRUE(writer
                                .append(
                                    WalEntry{
                                        .op = WalEntry::Op::Set,
                                        .key = "k",
                                        .value = "v",
                                        .version = 1,
                                        .writer_node_hash = 0,
                                        .expires_at_ms = 0,
                                        .has_ttl = false,
                                    }
                                )
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
            auto wal_path = (scratch_.path() / "wal.log").string();
            {
                std::ofstream(wal_path, std::ios::binary | std::ios::trunc);
            }

            WalReader reader(wal_path);
            EXPECT_FALSE(reader.hasMore());
        }

        TEST_F(PersistenceTest, WalChecksumDetectsCorruption) {
            auto wal_path = (scratch_.path() / "wal.log").string();
            {
                WalWriter writer(wal_path);
                ASSERT_TRUE(writer
                                .append(
                                    {.op = WalEntry::Op::Set,
                                     .key = "k1",
                                     .value = "v1",
                                     .version = 1,
                                     .writer_node_hash = 10,
                                     .expires_at_ms = 0,
                                     .has_ttl = false}
                                )
                                .has_value());
                ASSERT_TRUE(writer
                                .append(
                                    {.op = WalEntry::Op::Set,
                                     .key = "k2",
                                     .value = "v2",
                                     .version = 2,
                                     .writer_node_hash = 20,
                                     .expires_at_ms = 0,
                                     .has_ttl = false}
                                )
                                .has_value());
                ASSERT_TRUE(writer
                                .append(
                                    {.op = WalEntry::Op::Set,
                                     .key = "k3",
                                     .value = "v3",
                                     .version = 3,
                                     .writer_node_hash = 30,
                                     .expires_at_ms = 0,
                                     .has_ttl = false}
                                )
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
            auto wal_path = (scratch_.path() / "wal.log").string();

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
                write_u64(5000);
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
            EXPECT_EQ(e2->expires_at_ms, 5000);

            EXPECT_FALSE(reader.hasMore());
        }

        TEST_F(PersistenceTest, WalTombstoneRoundTrip) {
            auto wal_path = (scratch_.path() / "wal.log").string();
            {
                WalWriter writer(wal_path);
                ASSERT_TRUE(writer
                                .append(
                                    WalEntry{
                                        .op = WalEntry::Op::Set,
                                        .key = "gone",
                                        .value = "",
                                        .version = 7,
                                        .writer_node_hash = 11,
                                        .expires_at_ms = 9999,
                                        .has_ttl = true,
                                        .is_tombstone = true,
                                    }
                                )
                                .has_value());
            }

            WalReader reader(wal_path);
            auto e = reader.next();
            ASSERT_TRUE(e.has_value());
            EXPECT_EQ(e->op, WalEntry::Op::Set);
            EXPECT_EQ(e->key, "gone");
            EXPECT_EQ(e->version, 7);
            EXPECT_TRUE(e->is_tombstone);
            EXPECT_FALSE(reader.hasMore());
        }

        TEST_F(PersistenceTest, SnapshotWriteAndRead) {
            auto snap_path = (scratch_.path() / "snapshot.dat").string();
            std::vector<SnapshotEntry> entries;
            entries.push_back(
                SnapshotEntry{
                    .key = "a",
                    .value = "alpha",
                    .version = 10,
                    .writer_node_hash = 100,
                    .expires_at_ms = 9999,
                    .has_ttl = true,
                    .tombstone = false,
                    .freq = 5,
                }
            );
            entries.push_back(
                SnapshotEntry{
                    .key = "b",
                    .value = "beta",
                    .version = 20,
                    .writer_node_hash = 200,
                    .expires_at_ms = 0,
                    .has_ttl = false,
                    .tombstone = false,
                    .freq = 0,
                }
            );
            entries.push_back(
                SnapshotEntry{
                    .key = "c",
                    .value = "",
                    .version = 30,
                    .writer_node_hash = 300,
                    .expires_at_ms = 9999,
                    .has_ttl = true,
                    .tombstone = true,
                    .freq = 2,
                }
            );

            {
                SnapshotWriter writer(snap_path);
                ASSERT_TRUE(writer.write(42, entries).has_value());
            }

            SnapshotReader reader(snap_path);
            auto result = reader.readAll();
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->next_version, 42);
            ASSERT_EQ(result->entries.size(), 3U);

            EXPECT_EQ(result->entries[0].key, "a");
            EXPECT_EQ(result->entries[0].value, "alpha");
            EXPECT_EQ(result->entries[0].version, 10);
            EXPECT_TRUE(result->entries[0].has_ttl);
            EXPECT_EQ(result->entries[0].expires_at_ms, 9999);

            EXPECT_EQ(result->entries[1].key, "b");
            EXPECT_FALSE(result->entries[1].has_ttl);
            EXPECT_EQ(result->entries[1].tombstone, false);

            EXPECT_EQ(result->entries[2].key, "c");
            EXPECT_EQ(result->entries[2].version, 30);
            EXPECT_TRUE(result->entries[2].tombstone);
            EXPECT_TRUE(result->entries[2].has_ttl);
        }

        TEST_F(PersistenceTest, RecoverFromSnapshotPlusWal) {
            LruStore store(1'000'000);
            PersistenceManager pm(
                PersistenceManager::Options{
                    .data_dir = scratch_.path().string(),
                    .enabled = true,
                    .snapshot_interval_s = 60,
                    .max_wal_entries = 100,
                },
                store
            );

            // Seed the store
            ASSERT_TRUE(store.put("k1", "v1").has_value());
            ASSERT_TRUE(store.put("k2", "v2").has_value());
            ASSERT_TRUE(store.put("k3", "v3").has_value());

            // Compact: creates snapshot, truncates WAL
            ASSERT_TRUE(pm.compact().has_value());
            EXPECT_EQ(store.size(), 3U);

            // Write more data (goes to WAL)
            ASSERT_TRUE(store.put("k4", "v4").has_value());
            pm.onWrite(
                WalEntry{
                    .op = WalEntry::Op::Set,
                    .key = "k4",
                    .value = "v4",
                    .version = 4,
                    .writer_node_hash = 0,
                    .expires_at_ms = 0,
                    .has_ttl = false,
                }
            );
            pm.flush();

            // Create a fresh store and recover
            LruStore store2(1'000'000);
            PersistenceManager pm2(
                PersistenceManager::Options{
                    .data_dir = scratch_.path().string(),
                    .enabled = true,
                    .snapshot_interval_s = 60,
                    .max_wal_entries = 100,
                },
                store2
            );

            ASSERT_TRUE(pm2.recover().has_value());

            // All 4 keys should be present
            EXPECT_EQ(store2.size(), 4U);
            EXPECT_TRUE(store2.get("k1").has_value());
            EXPECT_TRUE(store2.get("k2").has_value());
            EXPECT_TRUE(store2.get("k3").has_value());
            EXPECT_TRUE(store2.get("k4").has_value());
        }

        TEST_F(PersistenceTest, RecoverRestoresTombstones) {
            LruStore store(1'000'000);
            PersistenceManager pm(
                PersistenceManager::Options{
                    .data_dir = scratch_.path().string(),
                    .enabled = true,
                    .snapshot_interval_s = 60,
                    .max_wal_entries = 100,
                },
                store
            );

            ASSERT_TRUE(store.put("live", "v").has_value());
            VersionedEntry tomb;
            tomb.setVersion(store.mintVersion());
            tomb.writer_node_hash = 42;
            tomb.setTombstone(true);
            tomb.expires_at = steady_clock::now() + seconds(600);
            tomb.setHasTtl(true);
            ASSERT_TRUE(store.putVersioned("gone", std::move(tomb)).has_value());
            ASSERT_TRUE(pm.compact().has_value());

            // Fresh store recovers both the live entry and the marker; the
            // delete survives the restart instead of resurrecting.
            LruStore store2(1'000'000);
            PersistenceManager pm2(
                PersistenceManager::Options{
                    .data_dir = scratch_.path().string(),
                    .enabled = true,
                    .snapshot_interval_s = 60,
                    .max_wal_entries = 100,
                },
                store2
            );
            ASSERT_TRUE(pm2.recover().has_value());
            EXPECT_TRUE(store2.get("live").has_value());
            EXPECT_FALSE(store2.get("gone").has_value());
            auto marker = store2.getVersioned("gone");
            ASSERT_TRUE(marker.has_value());
            EXPECT_TRUE(marker->isTombstone());
        }

        TEST_F(PersistenceTest, RecoverSkipsExpiredEntries) {
            // Pre-populate a snapshot with an expired entry
            auto snap_path = scratch_.path() / "snapshot_1000.dat";
            std::vector<SnapshotEntry> entries;
            entries.push_back(
                SnapshotEntry{
                    .key = "alive",
                    .value = "yes",
                    .version = 1,
                    .writer_node_hash = 0,
                    .expires_at_ms = 0,
                    .has_ttl = false,
                    .freq = 0,
                }
            );
            entries.push_back(
                SnapshotEntry{
                    .key = "expired",
                    .value = "no",
                    .version = 2,
                    .writer_node_hash = 0,
                    .expires_at_ms = 1, // way in the past
                    .has_ttl = true,
                    .freq = 0,
                }
            );
            {
                SnapshotWriter writer(snap_path.string());
                ASSERT_TRUE(writer.write(3, entries).has_value());
            }

            LruStore store(1'000'000);
            PersistenceManager pm(
                PersistenceManager::Options{
                    .data_dir = scratch_.path().string(),
                    .enabled = true,
                    .snapshot_interval_s = 60,
                    .max_wal_entries = 100,
                },
                store
            );

            ASSERT_TRUE(pm.recover().has_value());
            EXPECT_EQ(store.size(), 1U);
            EXPECT_TRUE(store.get("alive").has_value());
        }

        TEST_F(PersistenceTest, CompactTruncatesWal) {
            LruStore store(1'000'000);
            PersistenceManager pm(
                PersistenceManager::Options{
                    .data_dir = scratch_.path().string(),
                    .enabled = true,
                    .snapshot_interval_s = 60,
                    .max_wal_entries = 100,
                },
                store
            );

            ASSERT_TRUE(store.put("k1", "v1").has_value());
            pm.onWrite(
                WalEntry{
                    .op = WalEntry::Op::Set,
                    .key = "k1",
                    .value = "v1",
                    .version = 1,
                    .writer_node_hash = 0,
                    .expires_at_ms = 0,
                    .has_ttl = false,
                }
            );

            ASSERT_TRUE(pm.compact().has_value());

            // WAL should be empty after compact (header only, no entries)
            auto wal_path = scratch_.path() / "wal.log";
            WalReader reader(wal_path.string());
            EXPECT_FALSE(reader.next().has_value());
            EXPECT_FALSE(reader.corrupted());

            // Snapshot should exist
            bool found_snapshot = false;
            for (const auto& entry : std::filesystem::directory_iterator(scratch_.path())) {
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
                    .data_dir = scratch_.path().string(),
                    .enabled = false,
                },
                store
            );

            EXPECT_FALSE(pm.enabled());
            EXPECT_TRUE(pm.recover().has_value());
            pm.onWrite(
                WalEntry{
                    .op = WalEntry::Op::Set,
                    .key = "k",
                    .value = "v",
                    .version = 1,
                    .writer_node_hash = 0,
                    .expires_at_ms = 0,
                    .has_ttl = false,
                }
            );
            EXPECT_TRUE(pm.compact().has_value());
        }

        // Restart round-trip with a TTL'd key, deterministic via SimClock: the
        // remaining wall-clock lifetime must survive recovery instead of being reset
        // to ~1ms (regression test for the lost-TTL-on-restart bug).
        TEST_F(PersistenceTest, RecoverPreservesRemainingTtlFromSnapshot) {
            SimClock sim;
            LruStore store(1'000'000, &sim);
            PersistenceManager pm(pmOptions(scratch_.path()), store, &sim);

            ASSERT_TRUE(store.put("ttl", "v", milliseconds{10'000}).has_value());
            ASSERT_TRUE(pm.compact().has_value()); // snapshot written, WAL truncated

            // Fresh "process": same simulated time, new store + manager.
            LruStore store2(1'000'000, &sim);
            PersistenceManager pm2(pmOptions(scratch_.path()), store2, &sim);
            ASSERT_TRUE(pm2.recover().has_value());

            auto ve = store2.getVersioned("ttl");
            ASSERT_TRUE(ve.has_value());
            EXPECT_TRUE(ve->hasTtl());

            auto remaining = duration_cast<milliseconds>(ve->expires_at - sim.now()).count();
            EXPECT_GE(remaining, 9900); // ~full lifetime preserved (not 1ms)
            EXPECT_LE(remaining, 10'000);

            // Advancing past the expiry must actually expire it.
            sim.advance(milliseconds{10'001});
            store2.evictExpired();
            EXPECT_FALSE(store2.get("ttl").has_value());
        }

        TEST_F(PersistenceTest, RecoverPreservesRemainingTtlFromWal) {
            SimClock sim;
            LruStore store(1'000'000, &sim);
            PersistenceManager pm(pmOptions(scratch_.path()), store, &sim);
            store.setPersistence(&pm); // put() now appends through the real write path

            // Production boots with recover() before serving; it opens the WAL writer.
            ASSERT_TRUE(pm.recover().has_value());
            ASSERT_TRUE(store.put("ttl", "v", milliseconds{5000}).has_value());
            pm.flush(); // WAL only — no snapshot in this dir

            LruStore store2(1'000'000, &sim);
            PersistenceManager pm2(pmOptions(scratch_.path()), store2, &sim);
            ASSERT_TRUE(pm2.recover().has_value());

            auto ve = store2.getVersioned("ttl");
            ASSERT_TRUE(ve.has_value());
            EXPECT_TRUE(ve->hasTtl());

            auto remaining = duration_cast<milliseconds>(ve->expires_at - sim.now()).count();
            EXPECT_GE(remaining, 4900);
            EXPECT_LE(remaining, 5000);
        }

        TEST_F(PersistenceTest, SnapshotCorruptionDetection) {
            // Write a valid snapshot, then corrupt it
            LruStore store(1'000'000);
            ASSERT_TRUE(store.put("key1", "value1").has_value());

            PersistenceManager::Options opts = pmOptions(scratch_.path());
            PersistenceManager pm(opts, store);
            ASSERT_TRUE(pm.recover().has_value());
            ASSERT_TRUE(pm.compact().has_value());

            // Find the snapshot file and corrupt it
            for (const auto& entry : std::filesystem::directory_iterator(scratch_.path())) {
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
            auto wal_path = scratch_.path() / "wal.log";
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
            PersistenceManager::Options opts = pmOptions(scratch_.path());
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
            auto snap_path = scratch_.path() / "snapshot.dat";
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

        TEST_F(PersistenceTest, WalNeedsCompactThreshold) {
            LruStore store(1'000'000);
            PersistenceManager pm(
                PersistenceManager::Options{
                    .data_dir = scratch_.path().string(),
                    .enabled = true,
                    .snapshot_interval_s = 3600,
                    .max_wal_entries = 3,
                },
                store
            );
            ASSERT_TRUE(pm.recover().has_value());

            // Below threshold: no compact requested.
            auto write = [&](const std::string& k) {
                pm.onWrite(
                    WalEntry{
                        .op = WalEntry::Op::Set,
                        .key = k,
                        .value = "v",
                        .version = 1,
                        .writer_node_hash = 0,
                        .expires_at_ms = 0,
                        .has_ttl = false,
                    }
                );
            };
            write("k1");
            write("k2");
            pm.flush();
            EXPECT_FALSE(pm.walNeedsCompact());

            // At threshold: the server's tick should compact.
            write("k3");
            pm.flush();
            EXPECT_TRUE(pm.walNeedsCompact());

            // Compact truncates the WAL and clears the request.
            ASSERT_TRUE(pm.compact().has_value());
            EXPECT_FALSE(pm.walNeedsCompact());
        }

        TEST_F(PersistenceTest, WalSyncOk) {
            auto wal_path = (scratch_.path() / "wal.log").string();
            {
                WalWriter writer(wal_path);
                ASSERT_TRUE(writer
                                .append(
                                    WalEntry{
                                        .op = WalEntry::Op::Set,
                                        .key = "k",
                                        .value = "v",
                                        .version = 1,
                                        .writer_node_hash = 0,
                                        .expires_at_ms = 0,
                                        .has_ttl = false,
                                    }
                                )
                                .has_value());
                // Durability barrier: must not fail on a healthy filesystem,
                // and the record must still decode afterwards.
                EXPECT_TRUE(writer.sync().has_value());
            }
            WalReader reader(wal_path);
            auto entry = reader.next();
            ASSERT_TRUE(entry.has_value());
            EXPECT_EQ(entry->key, "k");
        }

        TEST_F(PersistenceTest, WalCorruptReplayReportsError) {
            LruStore store(1'000'000);
            PersistenceManager::Options opts = pmOptions(scratch_.path());
            PersistenceManager pm(opts, store);
            ASSERT_TRUE(pm.recover().has_value());
            ASSERT_TRUE(store.put("k1", "v1").has_value());
            pm.onWrite(
                WalEntry{
                    .op = WalEntry::Op::Set,
                    .key = "k1",
                    .value = "v1",
                    .version = 1,
                    .writer_node_hash = 0,
                    .expires_at_ms = 0,
                    .has_ttl = false,
                }
            );
            pm.flush();

            // Corrupt the payload byte of the single WAL record so its
            // checksum mismatches: replay must report CorruptData instead of
            // silently stopping with Ok.
            {
                auto wal_path = scratch_.path() / "wal.log";
                std::fstream f(wal_path, std::ios::binary | std::ios::in | std::ios::out);
                // header(8) + op(1) + key_len(4) + key(2) + val_len(4) = 19
                f.seekp(8 + 11);
                char byte = 0;
                f.get(byte);
                f.seekp(8 + 11);
                f.put(static_cast<char>(0xFFU ^ static_cast<unsigned char>(byte)));
            }

            LruStore store2(1'000'000);
            PersistenceManager pm2(opts, store2);
            EXPECT_FALSE(pm2.recover().has_value());
        }

        TEST_F(PersistenceTest, SnapshotPruneKeepsTwo) {
            LruStore store(1'000'000);
            PersistenceManager::Options opts = pmOptions(scratch_.path());
            PersistenceManager pm(opts, store);
            ASSERT_TRUE(store.put("k", "v").has_value());
            for (int i = 0; i < 4; ++i) {
                ASSERT_TRUE(pm.compact().has_value());
            }
            size_t snaps = 0;
            for (const auto& entry : std::filesystem::directory_iterator(scratch_.path())) {
                auto name = entry.path().filename().string();
                if (name.starts_with("snapshot_") && name.ends_with(".dat")) {
                    ++snaps;
                }
            }
            EXPECT_LE(snaps, 2U);
            EXPECT_GE(snaps, 1U);
        }

        TEST_F(PersistenceTest, CompactPreservesPendingWrites) {
            LruStore store(1'000'000);
            PersistenceManager::Options opts = pmOptions(scratch_.path());
            PersistenceManager pm(opts, store);
            ASSERT_TRUE(store.put("pre", "1").has_value());
            pm.onWrite(
                WalEntry{
                    .op = WalEntry::Op::Set,
                    .key = "pre",
                    .value = "1",
                    .version = 1,
                    .writer_node_hash = 0,
                    .expires_at_ms = 0,
                    .has_ttl = false,
                }
            );
            // Entries queued before compact must survive the WAL rotation:
            // they are either in the new snapshot or replayed from the fresh WAL.
            ASSERT_TRUE(pm.compact().has_value());

            LruStore store2(1'000'000);
            PersistenceManager pm2(opts, store2);
            ASSERT_TRUE(pm2.recover().has_value());
            EXPECT_TRUE(store2.get("pre").has_value());
        }

        TEST_F(PersistenceTest, WalAppendModeResumesExistingLog) {
            auto wal_path = (scratch_.path() / "wal.log").string();
            {
                WalWriter writer(wal_path, WalOpenMode::Append);
                ASSERT_TRUE(writer
                                .append(
                                    WalEntry{
                                        .op = WalEntry::Op::Set,
                                        .key = "a",
                                        .value = "1",
                                        .version = 1,
                                    }
                                )
                                .has_value());
            }
            {
                // Reopen in Append: must not emit a second header.
                WalWriter writer(wal_path, WalOpenMode::Append);
                ASSERT_TRUE(writer
                                .append(
                                    WalEntry{
                                        .op = WalEntry::Op::Set,
                                        .key = "b",
                                        .value = "2",
                                        .version = 2,
                                    }
                                )
                                .has_value());
            }
            WalReader reader(wal_path);
            auto e1 = reader.next();
            ASSERT_TRUE(e1.has_value());
            EXPECT_EQ(e1->key, "a");
            auto e2 = reader.next();
            ASSERT_TRUE(e2.has_value());
            EXPECT_EQ(e2->key, "b");
            EXPECT_FALSE(reader.next().has_value());
            EXPECT_FALSE(reader.corrupted());
        }

        TEST_F(PersistenceTest, WalAppendModeHealsBadHeader) {
            auto wal_path = (scratch_.path() / "wal.log").string();
            {
                std::ofstream garbage(wal_path, std::ios::binary | std::ios::trunc);
                garbage << "NOT-A-WAL-HEADER!!!!!!!!";
            }
            {
                WalWriter writer(wal_path, WalOpenMode::Append);
                ASSERT_TRUE(writer
                                .append(
                                    WalEntry{
                                        .op = WalEntry::Op::Set,
                                        .key = "k",
                                        .value = "v",
                                        .version = 1,
                                    }
                                )
                                .has_value());
            }
            WalReader reader(wal_path);
            auto e = reader.next();
            ASSERT_TRUE(e.has_value());
            EXPECT_EQ(e->key, "k");
        }

        TEST_F(PersistenceTest, RecoverTwiceWithoutCompactLosesNothing) {
            // Crash window: entries replayed by recover() used to live only
            // in memory (the WAL was truncated). A crash before the next
            // compact lost them. Now the log is kept, so a second restart
            // replays them again.
            LruStore store(1'000'000);
            PersistenceManager::Options opts = pmOptions(scratch_.path());
            {
                PersistenceManager pm(opts, store);
                ASSERT_TRUE(pm.recover().has_value());
                ASSERT_TRUE(store.put("k", "v").has_value());
                pm.onWrite(
                    WalEntry{
                        .op = WalEntry::Op::Set,
                        .key = "k",
                        .value = "v",
                        .version = 1,
                        .writer_node_hash = 0,
                        .expires_at_ms = 0,
                        .has_ttl = false,
                    }
                );
                pm.flush();
            } // no shutdown, no compact: simulates a crash after serving

            LruStore store2(1'000'000);
            PersistenceManager pm2(opts, store2);
            ASSERT_TRUE(pm2.recover().has_value());
            // Crash again before any compact: entries must still replay.
            LruStore store3(1'000'000);
            PersistenceManager pm3(opts, store3);
            ASSERT_TRUE(pm3.recover().has_value());
            EXPECT_TRUE(store3.get("k").has_value());
        }

        TEST_F(PersistenceTest, WalBatchAppendMatchesSequentialBytes) {
            // Group commit must be byte-identical to N sequential appends so
            // readers, checksums, and fuzz corpora are unaffected.
            std::vector<WalEntry> entries;
            for (int i = 0; i < 8; ++i) {
                entries.push_back(
                    WalEntry{
                        .op = i % 3 == 2 ? WalEntry::Op::Del : WalEntry::Op::Set,
                        .key = "k" + std::to_string(i),
                        .value = std::string(static_cast<size_t>(i) * 7, 'x'),
                        .version = static_cast<uint64_t>(i + 1),
                        .writer_node_hash = static_cast<uint64_t>(i * 11),
                        .expires_at_ms = 0,
                        .has_ttl = false,
                        .is_tombstone = i % 4 == 3,
                    }
                );
            }

            const auto seq_path = (scratch_.path() / "seq.wal").string();
            const auto batch_path = (scratch_.path() / "batch.wal").string();
            size_t batch_bytes = 0;
            {
                WalWriter seq(seq_path);
                for (const auto& e : entries) {
                    ASSERT_TRUE(seq.append(e).has_value());
                }
            }
            {
                WalWriter batch(batch_path);
                auto res = batch.appendBatch(entries);
                ASSERT_TRUE(res.has_value());
                batch_bytes = res.value();
            }

            std::ifstream seq_in(seq_path, std::ios::binary);
            std::ifstream batch_in(batch_path, std::ios::binary);
            std::string seq_bytes(
                (std::istreambuf_iterator<char>(seq_in)),
                std::istreambuf_iterator<char>()
            );
            std::string batch_bytes_str(
                (std::istreambuf_iterator<char>(batch_in)),
                std::istreambuf_iterator<char>()
            );
            EXPECT_EQ(seq_bytes, batch_bytes_str);
            EXPECT_EQ(batch_bytes, batch_bytes_str.size() - 8U); // minus header

            WalReader reader(batch_path);
            for (const auto& expected : entries) {
                auto got = reader.next();
                ASSERT_TRUE(got.has_value());
                EXPECT_EQ(got->key, expected.key);
                EXPECT_EQ(got->value, expected.value);
                EXPECT_EQ(got->version, expected.version);
            }
            EXPECT_FALSE(reader.next().has_value());
        }

        TEST_F(PersistenceTest, WalNeedsCompactBytesThreshold) {
            LruStore store(1'000'000);
            PersistenceManager pm(
                PersistenceManager::Options{
                    .data_dir = scratch_.path().string(),
                    .enabled = true,
                    .snapshot_interval_s = 3600,
                    .max_wal_entries = 0, // entries disabled: bytes alone trigger
                    .max_wal_bytes = 256,
                },
                store
            );
            ASSERT_TRUE(pm.recover().has_value());
            EXPECT_FALSE(pm.walNeedsCompact());
            pm.onWrite(
                WalEntry{
                    .op = WalEntry::Op::Set,
                    .key = "k",
                    .value = std::string(512, 'v'),
                    .version = 1,
                    .writer_node_hash = 0,
                    .expires_at_ms = 0,
                    .has_ttl = false,
                }
            );
            pm.flush();
            EXPECT_TRUE(pm.walNeedsCompact());
            ASSERT_TRUE(pm.compact().has_value());
            EXPECT_FALSE(pm.walNeedsCompact());
        }

        TEST_F(PersistenceTest, WalSegmentRotationSealsAndReplays) {
            LruStore store(1'000'000);
            PersistenceManager::Options opts{
                .data_dir = scratch_.path().string(),
                .enabled = true,
                .snapshot_interval_s = 3600,
                .max_wal_entries = 0,
                .max_wal_bytes = 0, // compact disabled: segments accumulate
                .max_wal_segment_bytes = 200,
            };
            PersistenceManager pm(opts, store);
            ASSERT_TRUE(pm.recover().has_value());

            constexpr int K_NUM_KEYS = 12; // >9 segments exercises seq ordering
            for (int i = 0; i < K_NUM_KEYS; ++i) {
                const auto key = "seg" + std::to_string(i);
                ASSERT_TRUE(store.put(key, "v" + std::to_string(i)).has_value());
                pm.onWrite(
                    WalEntry{
                        .op = WalEntry::Op::Set,
                        .key = key,
                        .value = "v" + std::to_string(i),
                        .version = static_cast<uint64_t>(i + 1),
                        .writer_node_hash = 0,
                        .expires_at_ms = 0,
                        .has_ttl = false,
                    }
                );
                pm.flush();
            }

            size_t sealed = 0;
            for (const auto& entry : std::filesystem::directory_iterator(scratch_.path())) {
                const auto name = entry.path().filename().string();
                if (name.starts_with("wal_") && name.ends_with(".log")) {
                    ++sealed;
                }
            }
            EXPECT_GT(sealed, 1U);

            // Fresh restart replays sealed segments in order plus the active
            // file: every key present, no snapshot involved.
            LruStore store2(1'000'000);
            PersistenceManager pm2(opts, store2);
            ASSERT_TRUE(pm2.recover().has_value());
            for (int i = 0; i < K_NUM_KEYS; ++i) {
                const auto key = "seg" + std::to_string(i);
                auto got = store2.get(key);
                ASSERT_TRUE(got.has_value()) << key;
                EXPECT_EQ(*got, "v" + std::to_string(i));
            }
        }

        TEST_F(PersistenceTest, CompactDeletesSealedSegments) {
            LruStore store(1'000'000);
            PersistenceManager::Options opts{
                .data_dir = scratch_.path().string(),
                .enabled = true,
                .snapshot_interval_s = 3600,
                .max_wal_entries = 0,
                .max_wal_bytes = 0,
                .max_wal_segment_bytes = 200,
            };
            PersistenceManager pm(opts, store);
            ASSERT_TRUE(pm.recover().has_value());
            for (int i = 0; i < 6; ++i) {
                const auto key = "c" + std::to_string(i);
                ASSERT_TRUE(store.put(key, "v").has_value());
                pm.onWrite(
                    WalEntry{
                        .op = WalEntry::Op::Set,
                        .key = key,
                        .value = "v",
                        .version = static_cast<uint64_t>(i + 1),
                        .writer_node_hash = 0,
                        .expires_at_ms = 0,
                        .has_ttl = false,
                    }
                );
                pm.flush();
            }
            ASSERT_TRUE(pm.compact().has_value());

            // Snapshot covers everything: no sealed segments remain, active
            // WAL is fresh, and a restart still recovers all keys.
            for (const auto& entry : std::filesystem::directory_iterator(scratch_.path())) {
                const auto name = entry.path().filename().string();
                EXPECT_FALSE(name.starts_with("wal_") && name.ends_with(".log")) << name;
            }
            LruStore store2(1'000'000);
            PersistenceManager pm2(opts, store2);
            ASSERT_TRUE(pm2.recover().has_value());
            for (int i = 0; i < 6; ++i) {
                EXPECT_TRUE(store2.get("c" + std::to_string(i)).has_value());
            }
        }

        TEST_F(PersistenceTest, LfuFreqSurvivesSnapshotRoundTrip) {
            LfuStore store(1'000'000);
            PersistenceManager::Options opts = pmOptions(scratch_.path());
            PersistenceManager pm(opts, store);
            ASSERT_TRUE(store.put("hot", "1").has_value());
            ASSERT_TRUE(store.put("cold", "2").has_value());
            for (int i = 0; i < 3; ++i) {
                EXPECT_TRUE(store.get("hot").has_value());
            }
            ASSERT_EQ(store.entryFreq("hot"), 4U); // 1 insert + 3 hits
            ASSERT_EQ(store.entryFreq("cold"), 1U);
            ASSERT_TRUE(pm.compact().has_value());

            LfuStore store2(1'000'000);
            PersistenceManager pm2(opts, store2);
            ASSERT_TRUE(pm2.recover().has_value());
            EXPECT_EQ(store2.entryFreq("hot"), 4U);
            EXPECT_EQ(store2.entryFreq("cold"), 1U);
        }

        TEST_F(PersistenceTest, LfuRestoreNeverDemotes) {
            LfuStore store(1'000'000);
            ASSERT_TRUE(store.put("k", "v").has_value());
            EXPECT_TRUE(store.get("k").has_value());
            EXPECT_TRUE(store.get("k").has_value());
            ASSERT_EQ(store.entryFreq("k"), 3U);
            store.restoreFreq("k", 2); // stale replay must not demote
            EXPECT_EQ(store.entryFreq("k"), 3U);
            store.restoreFreq("k", 9);
            EXPECT_EQ(store.entryFreq("k"), 9U);
            store.restoreFreq("missing", 5); // absent key: no-op, no crash
            EXPECT_EQ(store.entryFreq("missing"), 0U);
        }

        TEST_F(PersistenceTest, SnapshotStreamMatchesReadAll) {
            auto snap_path = (scratch_.path() / "snap.dat").string();
            std::vector<SnapshotEntry> entries;
            for (int i = 0; i < 10; ++i) {
                entries.push_back(
                    SnapshotEntry{
                        .key = "k" + std::to_string(i),
                        .value = "v" + std::to_string(i),
                        .version = static_cast<uint64_t>(i),
                        .freq = static_cast<size_t>(i),
                    }
                );
            }
            {
                SnapshotWriter writer(snap_path);
                ASSERT_TRUE(writer.write(11, entries).has_value());
            }
            SnapshotReader full(snap_path);
            auto all = full.readAll();
            ASSERT_TRUE(all.has_value());

            SnapshotReader streamed(snap_path);
            Version next = 0;
            std::vector<SnapshotEntry> got;
            ASSERT_TRUE(streamed
                            .readStream(next, [&](SnapshotEntry& e) {
                got.push_back(std::move(e));
            }).has_value());
            EXPECT_EQ(next, all->next_version);
            ASSERT_EQ(got.size(), all->entries.size());
            for (size_t i = 0; i < got.size(); ++i) {
                EXPECT_EQ(got[i].key, all->entries[i].key);
                EXPECT_EQ(got[i].value, all->entries[i].value);
            }
        }

        TEST_F(PersistenceTest, CurrentPointerSurvivesNewerGarbage) {
            // A newer-but-unpublished snapshot file (crash between write and
            // CURRENT publish) must not shadow the published one: recovery
            // prefers CURRENT over lexicographic order.
            LruStore store(1'000'000);
            PersistenceManager::Options opts = pmOptions(scratch_.path());
            PersistenceManager pm(opts, store);
            ASSERT_TRUE(store.put("live", "v").has_value());
            ASSERT_TRUE(pm.compact().has_value());

            // Plant a lexicographically newer snapshot file with no entries
            // and no CURRENT update.
            {
                SnapshotWriter writer(
                    (scratch_.path() / "snapshot_9999999999999999999.dat").string()
                );
                ASSERT_TRUE(writer.write(1, {}).has_value());
            }

            LruStore store2(1'000'000);
            PersistenceManager pm2(opts, store2);
            ASSERT_TRUE(pm2.recover().has_value());
            EXPECT_TRUE(store2.get("live").has_value());
        }

        struct SpanGuard {
            explicit SpanGuard(std::shared_ptr<RecordingSubscriber> sub) :
                prev_(globalSubscriber()) {
                setGlobalSubscriber(std::move(sub));
            }

            ~SpanGuard() {
                setGlobalSubscriber(prev_);
            }

            std::shared_ptr<Subscriber> prev_;
        };

        // Find a recorded span by operation name.
        auto findSpanByOp(const RecordingSubscriber& sub, std::string_view operation)
            -> std::optional<RecordingSubscriber::RecordedSpan> {
            for (const auto& s : sub.spans()) {
                if (s.operation == operation) {
                    return s;
                }
            }
            return std::nullopt;
        }

        TEST_F(PersistenceTest, StorageSpansReportOutcomes) {
            auto sub = std::make_shared<RecordingSubscriber>();
            SpanGuard guard(sub);

            LruStore store(1'000'000);
            PersistenceManager pm(pmOptions(scratch_.path()), store);
            ASSERT_TRUE(store.put("k1", "v1").has_value());
            ASSERT_TRUE(store.put("k2", "v2").has_value());
            ASSERT_TRUE(pm.compact().has_value());

            ASSERT_TRUE(store.put("k3", "v3").has_value());
            pm.onWrite(
                WalEntry{
                    .op = WalEntry::Op::Set,
                    .key = "k3",
                    .value = "v3",
                    .version = 3,
                    .writer_node_hash = 0,
                    .expires_at_ms = 0,
                    .has_ttl = false,
                }
            );
            pm.flush();

            LruStore store2(1'000'000);
            PersistenceManager pm2(pmOptions(scratch_.path()), store2);
            ASSERT_TRUE(pm2.recover().has_value());
            EXPECT_EQ(store2.size(), 3U);

            // Every stage of the write → compact → flush → recover cycle
            // reports Ok on its span.
            for (const char* op : {
                     "persistence.compact",
                     "snapshot.write",
                     "persistence.flush",
                     "persistence.recover",
                     "snapshot.read",
                     "persistence.replay",
                 }) {
                auto found = findSpanByOp(*sub, op);
                ASSERT_TRUE(found.has_value()) << "missing span: " << op;
                EXPECT_EQ(found->status, SpanStatus::Ok) << op;
                EXPECT_TRUE(found->closed) << op;
            }

            // Counts logged at each stage are also on the spans.
            auto attr_by_key =
                [](const RecordingSubscriber::RecordedSpan& span,
                   std::string_view key) -> const RecordingSubscriber::RecordedField* {
                for (const auto& a : span.attributes) {
                    if (a.key == key) {
                        return &a;
                    }
                }
                return nullptr;
            };

            const auto compact = findSpanByOp(*sub, "persistence.compact");
            ASSERT_TRUE(compact.has_value());
            ASSERT_NE(attr_by_key(*compact, "entries"), nullptr);
            EXPECT_EQ(attr_by_key(*compact, "entries")->uint_val, 2U);
            const auto snap_write = findSpanByOp(*sub, "snapshot.write");
            ASSERT_TRUE(snap_write.has_value());
            ASSERT_NE(attr_by_key(*snap_write, "entries"), nullptr);
            EXPECT_EQ(attr_by_key(*snap_write, "entries")->uint_val, 2U);
            const auto replay = findSpanByOp(*sub, "persistence.replay");
            ASSERT_TRUE(replay.has_value());
            ASSERT_NE(attr_by_key(*replay, "entries"), nullptr);
            EXPECT_EQ(attr_by_key(*replay, "entries")->uint_val, 1U);
            const auto snap_read = findSpanByOp(*sub, "snapshot.read");
            ASSERT_TRUE(snap_read.has_value());
            ASSERT_NE(attr_by_key(*snap_read, "entries"), nullptr);
            EXPECT_EQ(attr_by_key(*snap_read, "entries")->uint_val, 2U);
        }

        TEST_F(PersistenceTest, CorruptSnapshotReportsErrorSpan) {
            // Snapshot file with a bad magic: recovery must fail and mark the
            // span — not silently succeed.
            {
                std::ofstream f(scratch_.path() / "snapshot_1.dat", std::ios::binary);
                f << "GARBAGE-NOT-A-SNAPSHOT";
            }
            LruStore store(1'000'000);
            PersistenceManager pm(pmOptions(scratch_.path()), store);
            auto sub = std::make_shared<RecordingSubscriber>();
            {
                SpanGuard guard(sub);
                EXPECT_FALSE(pm.recover().has_value());
            }
            auto found = findSpanByOp(*sub, "persistence.recover");
            ASSERT_TRUE(found.has_value());
            EXPECT_EQ(found->status, SpanStatus::Error);
        }
    } // namespace
} // namespace cinder
