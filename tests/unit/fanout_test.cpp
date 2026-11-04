#include <atomic>
#include <chrono>
#include <future>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

#include "cinder/common/status.hpp"
#include "cinder/node/fanout.hpp"

namespace cinder {
    namespace {

        using VoidResult = Result<void>;

        // All completions available synchronously: the decision must fire
        // inline on the calling thread with no coroutine involved.
        TEST(FanOutTest, AllSyncCompletesInlineOnCallerThread) {
            const auto caller = std::this_thread::get_id();
            std::thread::id fired_on;
            bool fired = false;
            int fire_count = 0;

            fanOutQuorum<VoidResult>(3, [](auto report) {
                report(0, ok());
                report(1, ok());
                report(2, ok());
            }, [](std::size_t, VoidResult, bool) {}, [] { return true; }, [&] {
                fired = true;
                ++fire_count;
                fired_on = std::this_thread::get_id();
            }, [] { FAIL() << "fail must not run when all succeed"; });

            EXPECT_TRUE(fired);
            EXPECT_EQ(fire_count, 1);
            EXPECT_EQ(fired_on, caller);
        }

        // Completions arriving from other threads: exactly-once decision on
        // the async tail.
        TEST(FanOutTest, AsyncCompletionsDecideOnce) {
            std::promise<void> done;
            auto done_future = done.get_future();
            std::atomic<int> fires{0};
            std::atomic<int> fails{0};

            fanOutQuorum<VoidResult>(3, [](auto report) {
                std::thread([report]() mutable {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    report(0, ok());
                }).detach();
                std::thread([report]() mutable {
                    std::this_thread::sleep_for(std::chrono::milliseconds(20));
                    report(1, ok());
                }).detach();
                std::thread([report]() mutable {
                    std::this_thread::sleep_for(std::chrono::milliseconds(30));
                    report(2, ok());
                }).detach();
            }, [](std::size_t, VoidResult, bool) {}, [] { return true; }, [&] {
                ++fires;
                done.set_value();
            }, [&] { ++fails; });

            ASSERT_EQ(done_future.wait_for(std::chrono::seconds(10)), std::future_status::ready);
            EXPECT_EQ(fires.load(), 1);
            EXPECT_EQ(fails.load(), 0);
        }

        // Duplicate reports for the same target are dropped before they can
        // touch accounting: per-index callbacks run once, pending counts once.
        TEST(FanOutTest, DuplicateDeliveryIgnored) {
            std::vector<int> seen_counts(3, 0);
            int fires = 0;

            fanOutQuorum<VoidResult>(3, [](auto report) {
                report(0, ok());
                report(0, ok()); // duplicate: dropped
                report(0, ok()); // duplicate: dropped
                report(1, ok());
                report(2, ok());
            }, [&](std::size_t i, VoidResult, bool) { ++seen_counts[i]; }, [] {
                return true;
            }, [&] { ++fires; }, [] { FAIL() << "fail must not run"; });

            EXPECT_EQ(seen_counts, (std::vector<int>{1, 1, 1}));
            EXPECT_EQ(fires, 1);
        }

        // Fail-fast fires the decision while sends are outstanding, but late
        // completions are still delivered to on_each (side effects run).
        TEST(FanOutTest, FailFastStillDrainsLateCompletions) {
            std::promise<void> done;
            auto done_future = done.get_future();
            std::atomic<int> late_seen{0};
            std::atomic<int> fires{0};

            fanOutQuorum<VoidResult>(
                3,
                [](auto report) {
                // Quorum of 3 with w=3 impossible after 1 failure: report
                // the failure only; the other two arrive late.
                report(0, err<void>(Error(Errc::NotReady, "down")));
                std::thread([report]() mutable {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    report(1, ok());
                    report(2, ok());
                }).detach();
            },
                [&](std::size_t i, VoidResult, bool) {
                if (i > 0) {
                    ++late_seen;
                }
            },
                // Fail as soon as any failure makes 3 acks impossible.
                [] { return true; },
                [&] {
                ++fires;
                done.set_value();
            },
                [] { FAIL() << "explicit fail must not run (decision via fire)"; }
            );

            ASSERT_EQ(done_future.wait_for(std::chrono::seconds(10)), std::future_status::ready);
            EXPECT_EQ(fires.load(), 1);
            // Give the detached late sends a chance to land.
            for (int i = 0; i < 100 && late_seen.load() < 2; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            EXPECT_EQ(late_seen.load(), 2);
        }

        // Mixed sync + async: inline drain handles what is ready, the tail
        // finishes the rest, decision still fires exactly once.
        TEST(FanOutTest, MixedSyncAsyncCompletesOnce) {
            std::promise<void> done;
            auto done_future = done.get_future();
            std::atomic<int> fires{0};

            fanOutQuorum<VoidResult>(3, [](auto report) {
                report(0, ok());
                std::thread([report]() mutable {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    report(1, ok());
                    report(2, ok());
                }).detach();
            }, [](std::size_t, VoidResult, bool) {}, [] { return true; }, [&] {
                ++fires;
                done.set_value();
            }, [] { FAIL() << "fail must not run"; });

            ASSERT_EQ(done_future.wait_for(std::chrono::seconds(10)), std::future_status::ready);
            EXPECT_EQ(fires.load(), 1);
        }

    } // namespace
} // namespace cinder
