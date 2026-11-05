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

            void sendAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                SendCallback done
            ) override {
                done(ok());
            }

            void sendRequestAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                RequestCallback done
            ) override {
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

            explicit Fixture(int replica_factor = 1) :
                operations(store, ring, "n1", replica_factor, &replication) {
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
            fixture.ring.addNode("n3");
            bool done = false;

            fixture.operations.getAsync("k", [&](CacheOperations::GetResult result) {
                ASSERT_TRUE(result.has_value());
                EXPECT_EQ(*result, "remote");
                done = true;
            });

            EXPECT_TRUE(done);
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

    } // namespace
} // namespace cinder
