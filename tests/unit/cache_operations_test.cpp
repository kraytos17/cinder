#include <gtest/gtest.h>

#include "cinder/cluster/clock.hpp"
#include "cinder/cluster/transport.hpp"
#include "cinder/node/cache_operations.hpp"
#include "cinder/store/lru_store.hpp"

namespace cinder {
    namespace {

        class GetTransport final : public Transport {
        public:

            enum class Reply {
                Value,
                Missing,
                Tombstone,
            };

            Reply reply = Reply::Value;
            Version version = 5;
            bool fail_write = false;
            std::vector<NodeId> write_targets;
            std::vector<net::Request> write_requests;
            std::vector<NodeId> read_targets;

            void sendAsync(const NodeId& to, const net::Request& req, SendCallback done) override {
                write_targets.push_back(to);
                write_requests.push_back(req);
                if (fail_write) {
                    done(err(Error(Errc::NotReady, "replica unavailable")));
                } else {
                    done(ok());
                }
            }

            void sendRequestAsync(
                const NodeId& to,
                const net::Request& /*req*/,
                RequestCallback done
            ) override {
                read_targets.push_back(to);
                net::Response response;
                if (reply == Reply::Missing) {
                    response.status = Errc::NotFound;
                } else {
                    response.status = Errc::OK;
                    response.version = version;
                    response.writer_node_hash = 9;
                    response.tombstone = reply == Reply::Tombstone;
                    if (!response.tombstone) {
                        response.value = "remote";
                    }
                }
                done(ok(std::move(response)));
            }

            void onMessage(MessageHandler /*handler*/) override {}
        };

        struct Fixture {
            RealClock clock;
            LruStore store{1U << 20U};
            ConsistentHashRing ring;
            GetTransport transport;
            ReplicationManager replication{store, "n1", clock, transport};
            CacheOperations operations;

            explicit Fixture(
                int replica_factor = 1,
                ConsistencyMode mode = ConsistencyMode::Async
            ) :
                operations(store, ring, "n1", replica_factor, &replication, mode) {
                ring.addNode("n1");
            }
        };

        TEST(CacheOperationsGetTest, SingleReplicaReadsLocalStore) {
            Fixture fixture;
            ASSERT_TRUE(fixture.store.put("k", "local").has_value());

            bool done = false;
            fixture.operations.getAsync("k", [&](CacheOperations::GetResult result) {
                ASSERT_TRUE(result.has_value());
                EXPECT_EQ(*result, "local");
                done = true;
            });

            EXPECT_TRUE(done);
        }

        TEST(CacheOperationsGetTest, SingleReplicaMissIsNotFound) {
            Fixture fixture;
            bool done = false;

            fixture.operations.getAsync("missing", [&](CacheOperations::GetResult result) {
                ASSERT_FALSE(result.has_value());
                EXPECT_EQ(result.error().code(), Errc::NotFound);
                done = true;
            });

            EXPECT_TRUE(done);
        }

        TEST(CacheOperationsGetTest, QuorumReadUsesSharedReplicaPlan) {
            Fixture fixture(2);
            fixture.ring.addNode("n2");
            bool done = false;

            fixture.operations.getAsync("k", [&](CacheOperations::GetResult result) {
                ASSERT_TRUE(result.has_value());
                EXPECT_EQ(*result, "remote");
                done = true;
            });

            EXPECT_TRUE(done);
            fixture.operations.setAsync("k", "written", std::nullopt, [](Result<void> result) {
                EXPECT_TRUE(result.has_value());
            });

            ASSERT_FALSE(fixture.transport.read_targets.empty());
            for (const auto& target : fixture.transport.read_targets) {
                EXPECT_EQ(target, "n2");
            }

            ASSERT_FALSE(fixture.transport.write_targets.empty());
            for (const auto& target : fixture.transport.write_targets) {
                EXPECT_EQ(target, "n2");
            }
        }

        TEST(CacheOperationsGetTest, QuorumTombstoneMapsToNotFound) {
            Fixture fixture(2);
            fixture.ring.addNode("n2");
            fixture.ring.addNode("n3");
            fixture.transport.reply = GetTransport::Reply::Tombstone;
            bool done = false;

            fixture.operations.getAsync("k", [&](CacheOperations::GetResult result) {
                ASSERT_FALSE(result.has_value());
                EXPECT_EQ(result.error().code(), Errc::NotFound);
                done = true;
            });

            EXPECT_TRUE(done);
        }

        TEST(CacheOperationsSetTest, SingleReplicaWritesToLocalStore) {
            Fixture fixture;
            int callback_count = 0;

            fixture.operations.setAsync("k", "local", std::nullopt, [&](Result<void> result) {
                ASSERT_TRUE(result.has_value());
                ++callback_count;
            });

            EXPECT_EQ(callback_count, 1);
            ASSERT_TRUE(fixture.store.get("k").has_value());
            EXPECT_EQ(*fixture.store.get("k"), "local");
            EXPECT_TRUE(fixture.transport.write_targets.empty());
        }

        TEST(CacheOperationsSetTest, ReplicatedWriteUsesReplicaPlanAndPreservesTtl) {
            Fixture fixture(2);
            fixture.ring.addNode("n2");
            std::optional<milliseconds> ttl = milliseconds(5000);
            int callback_count = 0;

            fixture.operations.setAsync("k", "replicated", ttl, [&](Result<void> result) {
                ASSERT_TRUE(result.has_value());
                ++callback_count;
            });

            EXPECT_EQ(callback_count, 1);
            ASSERT_EQ(fixture.transport.write_targets.size(), 1U);
            EXPECT_EQ(fixture.transport.write_targets.front(), "n2");
            ASSERT_EQ(fixture.transport.write_requests.size(), 1U);
            EXPECT_EQ(fixture.transport.write_requests.front().key, "k");
            EXPECT_EQ(fixture.transport.write_requests.front().value, "replicated");
            EXPECT_TRUE(fixture.transport.write_requests.front().expires_at.has_value());
            const auto stored = fixture.store.getVersioned("k");
            ASSERT_TRUE(stored.has_value());
            EXPECT_TRUE(stored->hasTtl());
        }

        TEST(CacheOperationsSetTest, QuorumFailureIsReturnedExactlyOnce) {
            Fixture fixture(2, ConsistencyMode::Quorum);
            fixture.ring.addNode("n2");
            fixture.transport.fail_write = true;
            int callback_count = 0;
            Errc result_code = Errc::OK;

            fixture.operations.setAsync("k", "value", std::nullopt, [&](Result<void> result) {
                ++callback_count;
                if (!result) {
                    result_code = result.error().code();
                }
            });

            EXPECT_EQ(callback_count, 1);
            EXPECT_EQ(result_code, Errc::NotReady);
            EXPECT_TRUE(fixture.store.get("k").has_value());
        }

        TEST(CacheOperationsDeleteTest, SingleReplicaCreatesStandaloneTombstone) {
            Fixture fixture;
            ASSERT_TRUE(fixture.store.put("k", "value").has_value());
            int callback_count = 0;

            fixture.operations.deleteAsync("k", [&](Result<void> result) {
                ASSERT_TRUE(result.has_value());
                ++callback_count;
            });

            EXPECT_EQ(callback_count, 1);
            EXPECT_FALSE(fixture.store.get("k").has_value());
            const auto tombstone = fixture.store.getVersioned("k");
            ASSERT_TRUE(tombstone.has_value());
            EXPECT_TRUE(tombstone->isTombstone());
            EXPECT_EQ(tombstone->writer_node_hash, 0U);
            EXPECT_TRUE(fixture.transport.write_targets.empty());
        }

        TEST(CacheOperationsDeleteTest, ReplicatedDeleteUsesReplicaPlanAndTombstone) {
            Fixture fixture(2);
            fixture.ring.addNode("n2");
            int callback_count = 0;

            fixture.operations.deleteAsync("k", [&](Result<void> result) {
                ASSERT_TRUE(result.has_value());
                ++callback_count;
            });

            EXPECT_EQ(callback_count, 1);
            ASSERT_EQ(fixture.transport.write_targets.size(), 1U);
            EXPECT_EQ(fixture.transport.write_targets.front(), "n2");
            ASSERT_EQ(fixture.transport.write_requests.size(), 1U);
            EXPECT_EQ(fixture.transport.write_requests.front().key, "k");
            EXPECT_TRUE(fixture.transport.write_requests.front().tombstone);
            const auto tombstone = fixture.store.getVersioned("k");
            ASSERT_TRUE(tombstone.has_value());
            EXPECT_TRUE(tombstone->isTombstone());
        }

        TEST(CacheOperationsDeleteTest, QuorumFailureIsReturnedExactlyOnce) {
            Fixture fixture(2, ConsistencyMode::Quorum);
            fixture.ring.addNode("n2");
            fixture.transport.fail_write = true;
            int callback_count = 0;
            Errc result_code = Errc::OK;

            fixture.operations.deleteAsync("k", [&](Result<void> result) {
                ++callback_count;
                if (!result) {
                    result_code = result.error().code();
                }
            });

            EXPECT_EQ(callback_count, 1);
            EXPECT_EQ(result_code, Errc::NotReady);
            const auto tombstone = fixture.store.getVersioned("k");
            ASSERT_TRUE(tombstone.has_value());
            EXPECT_TRUE(tombstone->isTombstone());
        }

    } // namespace
} // namespace cinder
