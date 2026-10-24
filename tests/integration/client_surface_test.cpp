#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cinder/client/cache_client.hpp"
#include "integration/test_helpers.hpp"

using cinder::net::test::NodeProcGuard;
using cinder::net::test::pickHeldPort;
using cinder::net::test::spawnNode;
using cinder::net::test::waitForNode;

namespace cinder {
    namespace {

        class ClientSurfaceTest : public ::testing::Test {
        protected:

            void SetUp() override {
                port_ = pickHeldPort();
                ASSERT_NE(port_, 0);
                node_ = std::make_unique<NodeProcGuard>(spawnNode(port_, "node1", ""));
                ASSERT_TRUE(waitForNode(port_, "node1")) << "node did not start";
                ClusterConfig config;
                config.nodes.push_back({"node1", "127.0.0.1", port_});
                client_ = std::make_unique<CacheClient>(config);
            }

            uint16_t port_ = 0;
            std::unique_ptr<NodeProcGuard> node_;
            std::unique_ptr<CacheClient> client_;
        };

        TEST_F(ClientSurfaceTest, MultiSetStoresAndReadsBack) {
            std::vector<std::pair<std::string, std::string>> kvs;
            for (int i = 0; i < 20; i++) {
                kvs.emplace_back("mk" + std::to_string(i), "v" + std::to_string(i));
            }
            EXPECT_EQ(client_->multiSet(kvs), kvs.size());

            std::vector<std::string> keys;
            for (const auto& [k, v] : kvs) {
                (void)v;
                keys.push_back(k);
            }
            auto found = client_->multiGet(keys);
            EXPECT_EQ(found.size(), keys.size());
            for (const auto& [k, v] : kvs) {
                auto it = found.find(k);
                ASSERT_NE(it, found.end()) << k << " missing";
                EXPECT_EQ(it->second, v);
            }
        }

        TEST_F(ClientSurfaceTest, MultiSetEmptyIsNoop) {
            EXPECT_EQ(client_->multiSet({}), 0U);
        }

        TEST_F(ClientSurfaceTest, GetVersionedCarriesMetadata) {
            ASSERT_TRUE(
                client_->set("vkey", "vval", std::chrono::milliseconds(60'000)).has_value()
            );
            auto got = client_->getVersioned("vkey");
            ASSERT_TRUE(got.has_value());
            EXPECT_EQ(got->value, "vval");
            EXPECT_GT(got->version, 0U);
            ASSERT_TRUE(got->expires_at.has_value());
            EXPECT_GT(*got->expires_at, std::chrono::system_clock::now());
        }

        TEST_F(ClientSurfaceTest, GetVersionedNoTtlHasNoExpiry) {
            ASSERT_TRUE(client_->set("plain", "v").has_value());
            auto got = client_->getVersioned("plain");
            ASSERT_TRUE(got.has_value());
            EXPECT_EQ(got->value, "v");
            EXPECT_FALSE(got->expires_at.has_value());
        }

        TEST_F(ClientSurfaceTest, GetVersionedMissingIsNullopt) {
            EXPECT_FALSE(client_->getVersioned("absent").has_value());
        }

        TEST_F(ClientSurfaceTest, TtlRemainingWithinBounds) {
            ASSERT_TRUE(client_->set("ttlkey", "v", std::chrono::milliseconds(60'000)).has_value());
            auto remaining = client_->ttl("ttlkey");
            ASSERT_TRUE(remaining.has_value());
            EXPECT_GT(remaining->count(), 0);
            EXPECT_LE(remaining->count(), 60'000);
        }

        TEST_F(ClientSurfaceTest, TtlMissingOrTimelessIsNullopt) {
            EXPECT_FALSE(client_->ttl("absent").has_value());
            ASSERT_TRUE(client_->set("timeless", "v").has_value());
            EXPECT_FALSE(client_->ttl("timeless").has_value());
        }
    } // namespace
} // namespace cinder
