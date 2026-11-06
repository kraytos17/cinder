// Direct unit tests for HintJournal durability and its spans.
#include <chrono>
#include <fcntl.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include "cinder/common/tracing.hpp"
#include "cinder/node/hint_journal.hpp"
#include "cinder/node/hint_queue.hpp"
#include "cinder/node/replication_manager.hpp"
#include "test_scratch.hpp"
#include "unit/hint_test_helpers.hpp"
#include "unit/trace_test_helpers.hpp"

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::minutes;
using std::chrono::steady_clock;
using std::chrono::system_clock;

namespace cinder {
    namespace {

        struct HintJournalDir {
            // Unique per process and per case (see test_scratch.hpp).
            test::ScratchDir scratch;
            std::string dir;
            std::string journal;

            HintJournalDir() :
                dir(scratch.path().string()),
                journal((scratch.path() / "hints.journal").string()) {}
        };

        auto hjMakeHint(const std::string& target, const std::string& key, uint64_t version)
            -> HintQueue::Hint {
            auto h = cctMakeHint(target, key, version);
            h.req.value = key + "-v" + std::to_string(version);
            return h;
        }

        TEST(HintJournalTest, RoundtripPreservesLiveAndDropsTombstoned) {
            HintJournalDir paths;
            const uint64_t far_future =
                static_cast<uint64_t>(
                    duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()
                )
                + 60'000U;
            {
                HintQueue staging;
                HintJournal journal;
                EXPECT_EQ(journal.open(paths.journal, staging), 0U);
                journal.appendAdd(hjMakeHint("n2", "keep", 1), far_future);
                journal.appendAdd(hjMakeHint("n2", "gone", 1), far_future);
                journal.appendDel("n2", "gone", 1);
                journal.sync();
            }
            {
                HintQueue recovered;
                HintJournal journal;
                EXPECT_EQ(journal.open(paths.journal, recovered), 1U);
                EXPECT_EQ(recovered.size(), 1U);
                auto snap = recovered.snapshot(steady_clock::now());
                ASSERT_EQ(snap.size(), 1U);
                EXPECT_EQ(snap.front().hint.req.key, "keep");
            }
        }

        TEST(HintJournalTest, RoundtripPreservesTombstoneAndExpiry) {
            HintJournalDir paths;
            const uint64_t far_future =
                static_cast<uint64_t>(
                    duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()
                )
                + 60'000U;
            {
                HintQueue staging;
                HintJournal journal;
                EXPECT_EQ(journal.open(paths.journal, staging), 0U);
                auto tomb = hjMakeHint("n2", "gone", 7);
                tomb.req.tombstone = true;
                tomb.req.expires_at = system_clock::now() + milliseconds(300'000);
                tomb.req.writer_node_hash = 11;
                journal.appendAdd(tomb, far_future);
                journal.sync();
            }
            {
                HintQueue recovered;
                HintJournal journal;
                EXPECT_EQ(journal.open(paths.journal, recovered), 1U);
                auto snap = recovered.snapshot(steady_clock::now());
                ASSERT_EQ(snap.size(), 1U);
                const auto& req = snap.front().hint.req;
                EXPECT_EQ(req.key, "gone");
                EXPECT_EQ(req.version, 7U);
                EXPECT_EQ(req.writer_node_hash, 11U);
                EXPECT_TRUE(req.tombstone);
                EXPECT_TRUE(req.expires_at.has_value());
            }
        }

        TEST(HintJournalTest, ExpiredAddsAreSkippedOnRecovery) {
            HintJournalDir paths;
            {
                HintQueue staging;
                HintJournal journal;
                EXPECT_EQ(journal.open(paths.journal, staging), 0U);
                journal.appendAdd(hjMakeHint("n2", "stale", 1), 1U); // long past
                journal.sync();
            }
            {
                HintQueue recovered;
                HintJournal journal;
                EXPECT_EQ(journal.open(paths.journal, recovered), 0U);
                EXPECT_EQ(recovered.size(), 0U);
            }
        }

        TEST(HintJournalTest, TornTailTruncatesWithoutLosingHistory) {
            HintJournalDir paths;
            const uint64_t far_future =
                static_cast<uint64_t>(
                    duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()
                )
                + 60'000U;
            {
                HintQueue staging;
                HintJournal journal;
                journal.open(paths.journal, staging);
                journal.appendAdd(hjMakeHint("n2", "k1", 1), far_future);
                journal.appendAdd(hjMakeHint("n2", "k2", 1), far_future);
                journal.sync();
            }
            {
                // Simulate a crash mid-append: garbage after valid records.
                int fd = ::open(paths.journal.c_str(), O_WRONLY | O_APPEND);
                ASSERT_GE(fd, 0);
                const char garbage[] = {"\x05\x00\x00partial"};
                (void)::write(fd, garbage, sizeof(garbage));
                ::close(fd);
            }
            {
                HintQueue recovered;
                HintJournal journal;
                EXPECT_EQ(journal.open(paths.journal, recovered), 2U);
                EXPECT_EQ(journal.recordCount(), 2U);
                EXPECT_EQ(recovered.size(), 2U);
            }
        }

        TEST(HintJournalTest, CompactionRewritesFromLiveSnapshot) {
            HintJournalDir paths;
            const uint64_t far_future =
                static_cast<uint64_t>(
                    duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()
                )
                + 600'000U;
            {
                HintQueue staging;
                HintJournal journal;
                journal.open(paths.journal, staging);
                // 10 live hints buried in churn: each key rewritten often so
                // most records are superseded or tombstoned. Mirrors
                // enqueueHint: queue and journal advance together.
                for (int round = 0; round < 500; ++round) {
                    for (int k = 0; k < 10; ++k) {
                        auto h = hjMakeHint("n2", "ck" + std::to_string(k), 1);
                        h.req.value = "r" + std::to_string(round);
                        h.expires_at = steady_clock::now() + minutes(10);
                        journal.appendAdd(h, far_future);
                        staging.push(std::move(h));
                        if (round % 50 == 0) {
                            journal.appendDel("n2", "ck" + std::to_string(k), 1);
                            net::Request probe;
                            probe.key = "ck" + std::to_string(k);
                            probe.version = 1;
                            staging.remove("n2", probe);
                        }
                    }
                }
                // Round 499 re-adds every key after its last tombstone
                // (round 450), so 10 hints are live at the end.
                EXPECT_EQ(staging.size(), 10U);
                EXPECT_TRUE(journal.needsCompaction());
                journal.compact(staging);
                EXPECT_FALSE(journal.needsCompaction());
                journal.sync();
            }
            {
                HintQueue recovered;
                HintJournal journal;
                auto n = journal.open(paths.journal, recovered);
                EXPECT_EQ(n, 10U);
                EXPECT_EQ(recovered.size(), 10U);
            }
        }

        // Hint journal spans (Phase 6)
        // -----------------------------------------------------------------

        TEST(HintJournalSpanTest, OpenReportsOk) {
            HintJournalDir paths;
            auto sub = std::make_shared<RecordingSubscriber>();
            {
                TraceGuard guard(sub);
                HintQueue staging;
                HintJournal journal;
                EXPECT_EQ(journal.open(paths.journal, staging), 0U);
                journal.appendAdd(hjMakeHint("n2", "k", 1), 9'999'999'999'999ULL);
                journal.sync();
                HintQueue recovered;
                HintJournal reader;
                EXPECT_EQ(reader.open(paths.journal, recovered), 1U);
            }
            auto spans = sub->spans();
            bool opened = false;
            for (const auto& s : spans) {
                if (s.operation == "hint.journal.open") {
                    opened = true;
                    EXPECT_EQ(s.status, SpanStatus::Ok);
                    EXPECT_TRUE(s.closed);
                }
            }
            // Two opens (staging + recovery) both report Ok.
            EXPECT_TRUE(opened);
            // The recovery open reports how many hints it replayed.
            bool hints_seen = false;
            for (const auto& s : spans) {
                if (s.operation == "hint.journal.open") {
                    for (const auto& a : s.attributes) {
                        if (a.key == "hints" && a.uint_val == 1) {
                            hints_seen = true;
                        }
                    }
                }
            }
            EXPECT_TRUE(hints_seen);
        }

        TEST(HintJournalSpanTest, CompactReportsOk) {
            HintJournalDir paths;
            // 4100 live adds trip the 4096-record compaction threshold.
            {
                HintQueue staging;
                HintJournal journal;
                journal.open(paths.journal, staging);
                const uint64_t far_future =
                    static_cast<uint64_t>(
                        duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()
                    )
                    + 600'000ULL;
                for (int i = 0; i < 4100; ++i) {
                    journal.appendAdd(hjMakeHint("n2", "ck" + std::to_string(i), 1), far_future);
                }
                ASSERT_TRUE(journal.needsCompaction());
            }
            auto sub = std::make_shared<RecordingSubscriber>();
            {
                TraceGuard guard(sub);
                HintQueue staging;
                HintJournal journal;
                // Recovery replays the 4100 adds (the bounded queue keeps
                // K_CAPACITY of them); the journal itself is still over the
                // 4096-record compaction threshold.
                EXPECT_GT(journal.open(paths.journal, staging), 0U);
                EXPECT_TRUE(journal.needsCompaction());
                journal.compact(staging);
                EXPECT_FALSE(journal.needsCompaction());
            }
            auto spans = sub->spans();
            const auto* compacted = findSpan(spans, "hint.journal.compact");
            ASSERT_NE(compacted, nullptr);
            EXPECT_EQ(compacted->status, SpanStatus::Ok);
            EXPECT_TRUE(compacted->closed);
        }

    } // namespace
} // namespace cinder
