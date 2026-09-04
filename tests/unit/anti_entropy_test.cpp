#include <chrono>
#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "cinder/cluster/transport.hpp"
#include "cinder/common/metrics.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/node/anti_entropy.hpp"
#include "cinder/store/lru_store.hpp"

using std::chrono::seconds;

namespace cinder {
namespace {

class FakeTransport : public Transport {
  public:

    void sendAsync(
        const NodeId& /*to*/, const net::Request& /*req*/, SendCallback on_done) override {
        on_done(ok());
    }

    void sendRequestAsync(
        const NodeId& /*to*/, const net::Request& /*req*/, RequestCallback on_done) override {
        on_done(err<net::Response>(Error(Errc::NotReady, "no peer")));
    }

    void onMessage(MessageHandler /*handler*/) override {}
};

auto
makeEntry(std::string value, Version version, uint64_t writer = 7) -> VersionedEntry {
    VersionedEntry e;
    e.value = std::move(value);
    e.version = version;
    e.writer_node_hash = writer;
    return e;
}

auto
allBuckets(uint32_t n) -> std::vector<uint32_t> {
    std::vector<uint32_t> ids(n);
    for (uint32_t i = 0; i < n; ++i) {
        ids[i] = i;
    }
    return ids;
}

struct Node {
    RealClock clock;
    LruStore store{1U << 20U};
    ConsistentHashRing ring;
    FakeTransport transport;
    MetricsCollector metrics;
};

TEST(AntiEntropyDigestTest, EmptyStoresMatch) {
    Node a;
    Node b;
    AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
    AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);
    EXPECT_EQ(ma.computeDigest(), mb.computeDigest());
}

TEST(AntiEntropyDigestTest, IdenticalStoresSameDigest) {
    Node a;
    Node b;
    for (auto* n : {&a, &b}) {
        ASSERT_TRUE(n->store.putVersioned("k1", makeEntry("v1", 1)).has_value());
        ASSERT_TRUE(n->store.putVersioned("k2", makeEntry("v2", 2)).has_value());
        ASSERT_TRUE(n->store.putVersioned("k3", makeEntry("v3", 3)).has_value());
    }
    AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
    AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);
    EXPECT_EQ(ma.computeDigest(), mb.computeDigest());
}

TEST(AntiEntropyDigestTest, DifferentValueDifferentDigest) {
    Node a;
    Node b;
    ASSERT_TRUE(a.store.putVersioned("k1", makeEntry("v1", 1)).has_value());
    ASSERT_TRUE(b.store.putVersioned("k1", makeEntry("DIFFERENT", 1)).has_value());
    AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
    AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);
    EXPECT_NE(ma.computeDigest(), mb.computeDigest());
}

TEST(AntiEntropyDigestTest, InsertionOrderIndependent) {
    Node a;
    Node b;
    ASSERT_TRUE(a.store.putVersioned("k1", makeEntry("v1", 1)).has_value());
    ASSERT_TRUE(a.store.putVersioned("k2", makeEntry("v2", 2)).has_value());
    // Same data, reverse insertion order (different LRU recency order).
    ASSERT_TRUE(b.store.putVersioned("k2", makeEntry("v2", 2)).has_value());
    ASSERT_TRUE(b.store.putVersioned("k1", makeEntry("v1", 1)).has_value());
    AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
    AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);
    EXPECT_EQ(ma.computeDigest(), mb.computeDigest());
}

TEST(AntiEntropyEncodeTest, DigestRoundTrip) {
    std::vector<uint64_t> digest = {1, 2, 3, 0xFFFFFFFFFFFFFFFFULL};
    auto blob = AntiEntropyManager::encodeDigest(digest);
    auto back = AntiEntropyManager::decodeDigest(blob, digest.size());
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(*back, digest);
}

TEST(AntiEntropyEncodeTest, DigestRejectsBadInput) {
    std::vector<uint64_t> digest = {1, 2, 3};
    auto blob = AntiEntropyManager::encodeDigest(digest);
    EXPECT_FALSE(AntiEntropyManager::decodeDigest(blob, digest.size() + 1).has_value());
    EXPECT_FALSE(AntiEntropyManager::decodeDigest(blob.substr(0, blob.size() - 1), 3).has_value());
    EXPECT_FALSE(AntiEntropyManager::decodeDigest("", 3).has_value());
    EXPECT_FALSE(AntiEntropyManager::decodeDigest(blob + "x", 3).has_value());
}

TEST(AntiEntropyEncodeTest, BucketIdsRoundTrip) {
    std::vector<uint32_t> ids = {0, 5, 255};
    auto blob = AntiEntropyManager::encodeBucketIds(ids);
    auto back = AntiEntropyManager::decodeBucketIds(blob);
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(*back, ids);

    auto empty = AntiEntropyManager::encodeBucketIds({});
    auto back_empty = AntiEntropyManager::decodeBucketIds(empty);
    ASSERT_TRUE(back_empty.has_value());
    EXPECT_TRUE(back_empty->empty());

    EXPECT_FALSE(AntiEntropyManager::decodeBucketIds(blob.substr(0, blob.size() - 1)).has_value());
    EXPECT_FALSE(AntiEntropyManager::decodeBucketIds("").has_value());
}

TEST(AntiEntropySyncTest, CollectApplyRoundTrip) {
    Node a;
    Node b;
    ASSERT_TRUE(a.store.putVersioned("k1", makeEntry("v1", 10)).has_value());
    ASSERT_TRUE(a.store.putVersioned("k2", makeEntry("v2", 20)).has_value());
    AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
    AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);

    auto blob = ma.collectEntries(allBuckets(8));
    EXPECT_EQ(mb.applyEntries(blob), 2);

    auto e1 = b.store.getVersioned("k1");
    ASSERT_TRUE(e1.has_value());
    EXPECT_EQ(e1->value, "v1");
    EXPECT_EQ(e1->version, 10);
    auto e2 = b.store.getVersioned("k2");
    ASSERT_TRUE(e2.has_value());
    EXPECT_EQ(e2->value, "v2");
}

TEST(AntiEntropySyncTest, ApplyIsLWW) {
    Node a;
    Node b;
    ASSERT_TRUE(a.store.putVersioned("k", makeEntry("old", 1)).has_value());
    ASSERT_TRUE(b.store.putVersioned("k", makeEntry("new", 5)).has_value());
    AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
    AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);

    // Stale write is a no-op success.
    EXPECT_EQ(mb.applyEntries(ma.collectEntries(allBuckets(8))), 1);
    EXPECT_EQ(b.store.getVersioned("k")->value, "new");

    // Newer write wins.
    EXPECT_EQ(ma.applyEntries(mb.collectEntries(allBuckets(8))), 1);
    EXPECT_EQ(a.store.getVersioned("k")->value, "new");
}

TEST(AntiEntropySyncTest, MalformedBlobDoesNotCrash) {
    Node b;
    AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);
    EXPECT_EQ(mb.applyEntries(""), 0);
    EXPECT_EQ(mb.applyEntries("garbage"), 0);
    EXPECT_EQ(mb.applyEntries(std::string("\x05\x00\x00\x00", 4)), 0);
    EXPECT_TRUE(b.store.getVersioned("k1") == std::nullopt);
}

TEST(AntiEntropySyncTest, TtlPreserved) {
    Node a;
    Node b;
    VersionedEntry e = makeEntry("ephemeral", 3);
    e.has_ttl = true;
    e.expires_at = a.clock.now() + seconds(60);
    ASSERT_TRUE(a.store.putVersioned("tk", std::move(e)).has_value());

    AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
    AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);
    EXPECT_EQ(mb.applyEntries(ma.collectEntries(allBuckets(8))), 1);

    auto got = b.store.getVersioned("tk");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->value, "ephemeral");
    EXPECT_TRUE(got->has_ttl);
}

TEST(AntiEntropyPartnerTest, NoPartnerWhenSingleReplica) {
    Node a;
    a.ring.addNode("n1");
    AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
    EXPECT_FALSE(ma.pickPartner(1).has_value());
}

TEST(AntiEntropyPartnerTest, NoPartnerWhenStoreEmpty) {
    Node a;
    a.ring.addNode("n1");
    a.ring.addNode("n2");
    AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
    EXPECT_FALSE(ma.pickPartner(2).has_value());
}

TEST(AntiEntropyPartnerTest, PicksReplicaPartner) {
    Node a;
    a.ring.addNode("n1");
    a.ring.addNode("n2");
    ASSERT_TRUE(a.store.putVersioned("k1", makeEntry("v1", 1)).has_value());
    ASSERT_TRUE(a.store.putVersioned("k2", makeEntry("v2", 2)).has_value());
    AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
    auto partner = ma.pickPartner(2);
    ASSERT_TRUE(partner.has_value());
    EXPECT_EQ(*partner, "n2");
}

TEST(AntiEntropyHandlerTest, BadDigestRejected) {
    Node b;
    AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);
    net::Request req;
    req.opcode = net::Opcode::AntiEntropyDigest;
    req.value = "not-a-digest";
    bool responded = false;
    mb.onDigestRequest("n1", req, [&](net::Response r) {
        responded = true;
        EXPECT_EQ(r.status, Errc::InvalidArgument);
    });
    EXPECT_TRUE(responded);
}

TEST(AntiEntropyHandlerTest, SyncAppliesEntriesAndCountsMetrics) {
    Node a;
    Node b;
    ASSERT_TRUE(a.store.putVersioned("k1", makeEntry("v1", 1)).has_value());
    AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
    AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8, &b.metrics);

    net::Request req;
    req.opcode = net::Opcode::AntiEntropySync;
    req.value = ma.collectEntries(allBuckets(8));
    bool responded = false;
    mb.onSyncRequest("n1", req, [&](net::Response r) {
        responded = true;
        EXPECT_EQ(r.status, Errc::OK);
    });
    EXPECT_TRUE(responded);
    ASSERT_TRUE(b.store.getVersioned("k1").has_value());
    EXPECT_EQ(b.metrics.replicationMetrics().anti_entropy_keys_repaired.load(), 1);
}

TEST(AntiEntropyExchangeTest, TwoManagersConverge) {
    constexpr uint32_t K_BUCKETS = 8;
    Node a;
    Node b;

    ASSERT_TRUE(a.store.putVersioned("k1", makeEntry("a2", 2)).has_value());
    ASSERT_TRUE(b.store.putVersioned("k1", makeEntry("b1", 1)).has_value());
    ASSERT_TRUE(b.store.putVersioned("k2", makeEntry("b2", 5)).has_value());

    AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, K_BUCKETS);
    AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, K_BUCKETS);

    // Phase 1: A sends its digest; B responds with its digest + entries.
    auto digest_a = ma.computeDigest();
    net::Request digest_req;
    digest_req.opcode = net::Opcode::AntiEntropyDigest;
    digest_req.value = AntiEntropyManager::encodeDigest(digest_a);
    net::Response digest_resp;
    mb.onDigestRequest("n1", digest_req, [&](net::Response r) { digest_resp = std::move(r); });
    ASSERT_EQ(digest_resp.status, Errc::OK);
    ASSERT_TRUE(digest_resp.value.has_value());

    // Split response into [digest section][entries section].
    size_t digest_len = sizeof(uint32_t) + static_cast<size_t>(K_BUCKETS) * sizeof(uint64_t);
    ASSERT_GE(digest_resp.value->size(), digest_len);
    auto digest_b = AntiEntropyManager::decodeDigest(
        std::string_view(digest_resp.value->data(), digest_len), K_BUCKETS);
    ASSERT_TRUE(digest_b.has_value());
    std::string_view entries_b(
        digest_resp.value->data() + digest_len, digest_resp.value->size() - digest_len);

    // A applies B's entries (k2 arrives; stale k1 is a no-op).
    ma.applyEntries(entries_b);
    EXPECT_EQ(a.store.getVersioned("k2")->value, "b2");
    EXPECT_EQ(a.store.getVersioned("k1")->value, "a2");

    // Phase 2: A pushes its entries for the divergent buckets back to B.
    std::vector<uint32_t> divergent;
    for (uint32_t i = 0; i < K_BUCKETS; ++i) {
        if (digest_a[i] != (*digest_b)[i]) {
            divergent.push_back(i);
        }
    }
    EXPECT_FALSE(divergent.empty());
    net::Request sync_req;
    sync_req.opcode = net::Opcode::AntiEntropySync;
    sync_req.value = ma.collectEntries(divergent);
    net::Response sync_resp;
    mb.onSyncRequest("n1", sync_req, [&](net::Response r) { sync_resp = std::move(r); });
    EXPECT_EQ(sync_resp.status, Errc::OK);

    // Both sides converged.
    EXPECT_EQ(a.store.getVersioned("k1")->value, "a2");
    EXPECT_EQ(b.store.getVersioned("k1")->value, "a2");
    EXPECT_EQ(a.store.getVersioned("k2")->value, "b2");
    EXPECT_EQ(b.store.getVersioned("k2")->value, "b2");
    EXPECT_EQ(ma.computeDigest(), mb.computeDigest());
}

TEST(AntiEntropyRunRoundTest, TransportFailureIsBenign) {
    Node a;
    a.ring.addNode("n1");
    a.ring.addNode("n2");
    ASSERT_TRUE(a.store.putVersioned("k1", makeEntry("v1", 1)).has_value());
    AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
    // FakeTransport fails every RPC; the round must simply skip without
    // crashing or hanging (callbacks fire synchronously in the fake).
    ma.runRound(2);
}

} // namespace
} // namespace cinder
