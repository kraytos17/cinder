#ifdef CINDER_ENABLE_GRPC

#include <chrono>
#include <csignal>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <unistd.h>

#include "cinder/v1/cache.grpc.pb.h"

using cinder::net::test::NodeProcGuard;
using cinder::net::test::spawnNode;
using cinder::net::test::waitForPort;
using std::chrono::milliseconds;

namespace {

constexpr int K_GRPC_PORT1 = 17'980;
constexpr int K_TCP_PORT1 = 17'981;

auto
makeChannel(int port) -> std::shared_ptr<grpc::Channel> {
    return grpc::CreateChannel(
        "127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials());
}

auto
spawnNodeWithGrpc(int tcp_port, int grpc_port, const std::string& id) -> NodeProc {
    auto tcp_str = std::to_string(tcp_port);
    auto grpc_str = std::to_string(grpc_port);
    pid_t pid = fork();
    if (pid == -1) {
        ADD_FAILURE() << "fork failed";
        return {};
    }
    if (pid == 0) {
        // NOLINTNEXTLINE
        execl(CINDER_TEST_CINDERD_PATH,
            "cinderd",
            "--port",
            tcp_str.c_str(),
            "--node-id",
            id.c_str(),
            "--grpc-port",
            grpc_str.c_str(),
            nullptr);
        _exit(1);
    }
    return {pid, tcp_port, id};
}

bool
waitForGrpcPort(int port, int max_retries = 50) {
    for (int i = 0; i < max_retries; ++i) {
        auto channel = makeChannel(port);
        auto stub = cinder::v1::CinderCacheService::NewStub(channel);
        grpc::ClientContext ctx;
        cinder::v1::PingRequest req;
        cinder::v1::PingResponse resp;
        auto status = stub->Ping(&ctx, req, &resp);
        if (status.ok()) {
            return true;
        }
        std::this_thread::sleep_for(milliseconds(50));
    }
    return false;
}

TEST(GrpcGatewayTest, SetAndGet) {
    NodeProcGuard node{spawnNodeWithGrpc(K_TCP_PORT1, K_GRPC_PORT1, "grpc-node1")};
    ASSERT_TRUE(waitForPort(K_TCP_PORT1));
    ASSERT_TRUE(waitForGrpcPort(K_GRPC_PORT1));

    auto stub = cinder::v1::CinderCacheService::NewStub(makeChannel(K_GRPC_PORT1));
    // Set
    {
        grpc::ClientContext ctx;
        cinder::v1::SetRequest req;
        cinder::v1::SetResponse resp;
        req.set_key("grpc-key");
        req.set_value("grpc-value");
        auto status = stub->Set(&ctx, req, &resp);
        ASSERT_TRUE(status.ok()) << status.error_message();
        EXPECT_EQ(resp.status(), cinder::v1::STATUS_CODE_UNSPECIFIED);
    }

    // Get
    {
        grpc::ClientContext ctx;
        cinder::v1::GetRequest req;
        cinder::v1::GetResponse resp;
        req.set_key("grpc-key");
        auto status = stub->Get(&ctx, req, &resp);
        ASSERT_TRUE(status.ok()) << status.error_message();
        EXPECT_EQ(resp.status(), cinder::v1::STATUS_CODE_UNSPECIFIED);
        EXPECT_EQ(resp.value(), "grpc-value");
    }
}

TEST(GrpcGatewayTest, GetNotFound) {
    NodeProcGuard node{spawnNodeWithGrpc(K_TCP_PORT1, K_GRPC_PORT1, "grpc-node1")};
    ASSERT_TRUE(waitForPort(K_TCP_PORT1));
    ASSERT_TRUE(waitForGrpcPort(K_GRPC_PORT1));

    auto stub = cinder::v1::CinderCacheService::NewStub(makeChannel(K_GRPC_PORT1));
    grpc::ClientContext ctx;
    cinder::v1::GetRequest req;
    cinder::v1::GetResponse resp;
    req.set_key("missing-key");
    auto status = stub->Get(&ctx, req, &resp);
    ASSERT_TRUE(status.ok()) << status.error_message();
    EXPECT_EQ(resp.status(), cinder::v1::STATUS_CODE_NOT_FOUND);
}

TEST(GrpcGatewayTest, Delete) {
    NodeProcGuard node{spawnNodeWithGrpc(K_TCP_PORT1, K_GRPC_PORT1, "grpc-node1")};
    ASSERT_TRUE(waitForPort(K_TCP_PORT1));
    ASSERT_TRUE(waitForGrpcPort(K_GRPC_PORT1));

    auto stub = cinder::v1::CinderCacheService::NewStub(makeChannel(K_GRPC_PORT1));
    // Set then delete
    {
        grpc::ClientContext ctx;
        cinder::v1::SetRequest req;
        cinder::v1::SetResponse resp;
        req.set_key("del-key");
        req.set_value("del-val");
        stub->Set(&ctx, req, &resp);
    }
    {
        grpc::ClientContext ctx;
        cinder::v1::DeleteRequest req;
        cinder::v1::DeleteResponse resp;
        req.set_key("del-key");
        auto status = stub->Delete(&ctx, req, &resp);
        ASSERT_TRUE(status.ok()) << status.error_message();
        EXPECT_EQ(resp.status(), cinder::v1::STATUS_CODE_UNSPECIFIED);
    }
    // Verify deleted
    {
        grpc::ClientContext ctx;
        cinder::v1::GetRequest req;
        cinder::v1::GetResponse resp;
        req.set_key("del-key");
        stub->Get(&ctx, req, &resp);
        EXPECT_EQ(resp.status(), cinder::v1::STATUS_CODE_NOT_FOUND);
    }
}

TEST(GrpcGatewayTest, Ping) {
    NodeProcGuard node{spawnNodeWithGrpc(K_TCP_PORT1, K_GRPC_PORT1, "grpc-node1")};
    ASSERT_TRUE(waitForPort(K_TCP_PORT1));
    ASSERT_TRUE(waitForGrpcPort(K_GRPC_PORT1));

    auto stub = cinder::v1::CinderCacheService::NewStub(makeChannel(K_GRPC_PORT1));
    grpc::ClientContext ctx;
    cinder::v1::PingRequest req;
    cinder::v1::PingResponse resp;
    auto status = stub->Ping(&ctx, req, &resp);
    ASSERT_TRUE(status.ok()) << status.error_message();
    EXPECT_EQ(resp.status(), cinder::v1::STATUS_CODE_UNSPECIFIED);
}

TEST(GrpcGatewayTest, Info) {
    NodeProcGuard node{spawnNodeWithGrpc(K_TCP_PORT1, K_GRPC_PORT1, "grpc-node1")};
    ASSERT_TRUE(waitForPort(K_TCP_PORT1));
    ASSERT_TRUE(waitForGrpcPort(K_GRPC_PORT1));

    auto stub = cinder::v1::CinderCacheService::NewStub(makeChannel(K_GRPC_PORT1));
    grpc::ClientContext ctx;
    cinder::v1::InfoRequest req;
    cinder::v1::InfoResponse resp;
    auto status = stub->Info(&ctx, req, &resp);
    ASSERT_TRUE(status.ok()) << status.error_message();
    EXPECT_TRUE(resp.json().contains("node_id"));
    EXPECT_TRUE(resp.json().contains("port"));
    EXPECT_TRUE(resp.json().contains("capacity_bytes"));
}

TEST(GrpcGatewayTest, ClusterInfo) {
    NodeProcGuard node{spawnNodeWithGrpc(K_TCP_PORT1, K_GRPC_PORT1, "grpc-node1")};
    ASSERT_TRUE(waitForPort(K_TCP_PORT1));
    ASSERT_TRUE(waitForGrpcPort(K_GRPC_PORT1));

    auto stub = cinder::v1::CinderCacheService::NewStub(makeChannel(K_GRPC_PORT1));
    grpc::ClientContext ctx;
    cinder::v1::ClusterInfoRequest req;
    cinder::v1::ClusterInfoResponse resp;
    auto status = stub->ClusterInfo(&ctx, req, &resp);
    ASSERT_TRUE(status.ok()) << status.error_message();
    EXPECT_TRUE(resp.json().contains("alive"));
    EXPECT_TRUE(resp.json().contains("grpc-node1"));
}

TEST(GrpcGatewayTest, RingInfo) {
    NodeProcGuard node{spawnNodeWithGrpc(K_TCP_PORT1, K_GRPC_PORT1, "grpc-node1")};
    ASSERT_TRUE(waitForPort(K_TCP_PORT1));
    ASSERT_TRUE(waitForGrpcPort(K_GRPC_PORT1));

    auto stub = cinder::v1::CinderCacheService::NewStub(makeChannel(K_GRPC_PORT1));
    grpc::ClientContext ctx;
    cinder::v1::RingInfoRequest req;
    cinder::v1::RingInfoResponse resp;
    auto status = stub->RingInfo(&ctx, req, &resp);
    ASSERT_TRUE(status.ok()) << status.error_message();
    EXPECT_TRUE(resp.json().contains("self"));
    EXPECT_TRUE(resp.json().contains("vnodes_per_node"));
}

TEST(GrpcGatewayTest, MultiGet) {
    NodeProcGuard node{spawnNodeWithGrpc(K_TCP_PORT1, K_GRPC_PORT1, "grpc-node1")};
    ASSERT_TRUE(waitForPort(K_TCP_PORT1));
    ASSERT_TRUE(waitForGrpcPort(K_GRPC_PORT1));

    auto stub = cinder::v1::CinderCacheService::NewStub(makeChannel(K_GRPC_PORT1));
    // Set two keys
    {
        grpc::ClientContext ctx;
        cinder::v1::SetRequest req;
        cinder::v1::SetResponse resp;
        req.set_key("mg-a");
        req.set_value("val-a");
        stub->Set(&ctx, req, &resp);
    }
    {
        grpc::ClientContext ctx;
        cinder::v1::SetRequest req;
        cinder::v1::SetResponse resp;
        req.set_key("mg-b");
        req.set_value("val-b");
        stub->Set(&ctx, req, &resp);
    }

    // MultiGet
    {
        grpc::ClientContext ctx;
        cinder::v1::MultiGetRequest req;
        cinder::v1::MultiGetResponse resp;
        req.add_keys("mg-a");
        req.add_keys("mg-b");
        req.add_keys("mg-missing");
        auto status = stub->MultiGet(&ctx, req, &resp);
        ASSERT_TRUE(status.ok()) << status.error_message();
        ASSERT_EQ(resp.responses_size(), 3);
        EXPECT_EQ(resp.responses(0).value(), "val-a");
        EXPECT_EQ(resp.responses(1).value(), "val-b");
        EXPECT_EQ(resp.responses(2).status(), cinder::v1::STATUS_CODE_NOT_FOUND);
    }
}

TEST(GrpcGatewayTest, MultiSet) {
    NodeProcGuard node{spawnNodeWithGrpc(K_TCP_PORT1, K_GRPC_PORT1, "grpc-node1")};
    ASSERT_TRUE(waitForPort(K_TCP_PORT1));
    ASSERT_TRUE(waitForGrpcPort(K_GRPC_PORT1));

    auto stub = cinder::v1::CinderCacheService::NewStub(makeChannel(K_GRPC_PORT1));
    // MultiSet
    {
        grpc::ClientContext ctx;
        cinder::v1::MultiSetRequest req;
        cinder::v1::MultiSetResponse resp;
        auto* s1 = req.add_requests();
        s1->set_key("ms-x");
        s1->set_value("val-x");
        auto* s2 = req.add_requests();
        s2->set_key("ms-y");
        s2->set_value("val-y");
        auto status = stub->MultiSet(&ctx, req, &resp);
        ASSERT_TRUE(status.ok()) << status.error_message();
        ASSERT_EQ(resp.responses_size(), 2);
        EXPECT_EQ(resp.responses(0).status(), cinder::v1::STATUS_CODE_UNSPECIFIED);
        EXPECT_EQ(resp.responses(1).status(), cinder::v1::STATUS_CODE_UNSPECIFIED);
    }

    // Verify via Get
    {
        grpc::ClientContext ctx;
        cinder::v1::GetRequest req;
        cinder::v1::GetResponse resp;
        req.set_key("ms-x");
        stub->Get(&ctx, req, &resp);
        EXPECT_EQ(resp.value(), "val-x");
    }
    {
        grpc::ClientContext ctx;
        cinder::v1::GetRequest req;
        cinder::v1::GetResponse resp;
        req.set_key("ms-y");
        stub->Get(&ctx, req, &resp);
        EXPECT_EQ(resp.value(), "val-y");
    }
}

TEST(GrpcGatewayTest, SetWithTtl) {
    NodeProcGuard node{spawnNodeWithGrpc(K_TCP_PORT1, K_GRPC_PORT1, "grpc-node1")};
    ASSERT_TRUE(waitForPort(K_TCP_PORT1));
    ASSERT_TRUE(waitForGrpcPort(K_GRPC_PORT1));

    auto stub = cinder::v1::CinderCacheService::NewStub(makeChannel(K_GRPC_PORT1));
    // Set with 1s TTL
    {
        grpc::ClientContext ctx;
        cinder::v1::SetRequest req;
        cinder::v1::SetResponse resp;
        req.set_key("ttl-key");
        req.set_value("ttl-val");
        req.set_ttl_ms(1'000);
        auto status = stub->Set(&ctx, req, &resp);
        ASSERT_TRUE(status.ok()) << status.error_message();
        EXPECT_EQ(resp.status(), cinder::v1::STATUS_CODE_UNSPECIFIED);
    }

    // Get immediately — should exist
    {
        grpc::ClientContext ctx;
        cinder::v1::GetRequest req;
        cinder::v1::GetResponse resp;
        req.set_key("ttl-key");
        stub->Get(&ctx, req, &resp);
        EXPECT_EQ(resp.status(), cinder::v1::STATUS_CODE_UNSPECIFIED);
        EXPECT_EQ(resp.value(), "ttl-val");
    }
}
} // namespace
#endif
