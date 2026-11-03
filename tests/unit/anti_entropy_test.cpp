#include <chrono>
#include <gtest/gtest.h>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "cinder/cluster/transport.hpp"
#include "cinder/common/metrics.hpp"
#include "cinder/common/tracing.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/node/anti_entropy.hpp"
#include "cinder/store/lru_store.hpp"

using std::chrono::seconds;

namespace cinder {
    namespace {

        class FakeTransport : public Transport {
        public:

            void sendAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                SendCallback on_done
            ) override {
                on_done(ok());
            }

            void sendRequestAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                RequestCallback on_done
            ) override {
                on_done(err<net::Response>(Error(Errc::NotReady, "no peer")));
            }

            void onMessage(MessageHandler /*handler*/) override {}
        };

        auto makeEntry(std::string value, Version version, uint64_t writer = 7) -> VersionedEntry {
            VersionedEntry e;
            e.value = std::move(value);
            e.setVersion(version);
            e.writer_node_hash = writer;
            return e;
        }

        auto allBuckets(uint32_t n) -> std::vector<uint32_t> {
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

        TEST(AntiEntropyDigestTest, SameWriteSameDigestDespiteValue) {
            // (writer, version) uniquely identifies a write, so values are
            // excluded from the digest: same key/version/writer converges.
            Node a;
            Node b;
            ASSERT_TRUE(a.store.putVersioned("k1", makeEntry("v1", 1)).has_value());
            ASSERT_TRUE(b.store.putVersioned("k1", makeEntry("DIFFERENT", 1)).has_value());
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
            AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);
            EXPECT_EQ(ma.computeDigest(), mb.computeDigest());
        }

        TEST(AntiEntropyDigestTest, NewerVersionDifferentDigest) {
            Node a;
            Node b;
            ASSERT_TRUE(a.store.putVersioned("k1", makeEntry("v1", 1)).has_value());
            ASSERT_TRUE(b.store.putVersioned("k1", makeEntry("v1", 2)).has_value());
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
            EXPECT_FALSE(
                AntiEntropyManager::decodeDigest(blob.substr(0, blob.size() - 1), 3).has_value()
            );
            EXPECT_FALSE(AntiEntropyManager::decodeDigest("", 3).has_value());
            EXPECT_FALSE(AntiEntropyManager::decodeDigest(blob + "x", 3).has_value());
        }

        TEST(AntiEntropyEncodeTest, LegacySchemaRejected) {
            // v1 layout: bare count-first blob with no magic. Must fail
            // cleanly so mixed-version peers skip the round.
            std::string legacy;
            uint32_t n = 3;
            legacy.append(reinterpret_cast<const char*>(&n), sizeof(n));
            for (uint64_t h : {1ULL, 2ULL, 3ULL}) {
                legacy.append(reinterpret_cast<const char*>(&h), sizeof(h));
            }
            EXPECT_FALSE(AntiEntropyManager::decodeDigest(legacy, 3).has_value());
        }

        TEST(AntiEntropyEncodeTest, MetaRowsRoundTrip) {
            Node a;
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
            std::vector<AntiEntropyManager::MetaRow> rows = {
                {0, 11, 1, 7},
                {0, 22, 2, 7},
                {5, 33, 1, 9},
            };
            auto blob = AntiEntropyManager::encodeMetaRows(rows);
            auto back = AntiEntropyManager::decodeMetaRows(blob);
            ASSERT_TRUE(back.has_value());
            ASSERT_EQ(back->size(), 3U);
            for (size_t i = 0; i < 3; ++i) {
                EXPECT_EQ((*back)[i].bucket, rows[i].bucket);
                EXPECT_EQ((*back)[i].key_hash, rows[i].key_hash);
                EXPECT_EQ((*back)[i].version, rows[i].version);
                EXPECT_EQ((*back)[i].writer, rows[i].writer);
            }

            auto empty = AntiEntropyManager::encodeMetaRows({});
            auto back_empty = AntiEntropyManager::decodeMetaRows(empty);
            ASSERT_TRUE(back_empty.has_value());
            EXPECT_TRUE(back_empty->empty());

            EXPECT_FALSE(
                AntiEntropyManager::decodeMetaRows(blob.substr(0, blob.size() - 1)).has_value()
            );
            EXPECT_FALSE(AntiEntropyManager::decodeMetaRows("").has_value());
            EXPECT_FALSE(AntiEntropyManager::decodeMetaRows("garbage-blob").has_value());
        }

        TEST(AntiEntropyEncodeTest, SyncRoundTrip) {
            Node a;
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
            AntiEntropyManager::SyncMessage msg;
            msg.entries.push_back({"k1", makeEntry("v1", 3)});
            msg.entries.push_back({"k2", makeEntry("v2", 4)});
            msg.needs = {11, 22};
            auto blob = ma.encodeSync(msg);
            auto back = ma.decodeSync(blob);
            ASSERT_TRUE(back.has_value());
            ASSERT_EQ(back->entries.size(), 2U);
            EXPECT_EQ(back->entries[0].key, "k1");
            EXPECT_EQ(back->entries[0].entry.value, "v1");
            EXPECT_EQ(back->entries[0].entry.version(), 3U);
            EXPECT_EQ(back->entries[1].key, "k2");
            EXPECT_EQ(back->needs, msg.needs);

            EXPECT_FALSE(ma.decodeSync("").has_value());
            EXPECT_FALSE(ma.decodeSync("garbage").has_value());
            EXPECT_FALSE(ma.decodeSync(blob.substr(0, blob.size() - 1)).has_value());
        }

        TEST(AntiEntropySyncTest, CollectApplyRoundTrip) {
            Node a;
            Node b;
            ASSERT_TRUE(a.store.putVersioned("k1", makeEntry("v1", 10)).has_value());
            ASSERT_TRUE(a.store.putVersioned("k2", makeEntry("v2", 20)).has_value());
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
            AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);

            auto meta = ma.collectMetadata(allBuckets(8), 1U << 20U);
            auto rows = AntiEntropyManager::decodeMetaRows(meta);
            ASSERT_TRUE(rows.has_value());
            ASSERT_EQ(rows->size(), 2U);
            auto diff = mb.diffRows(*rows, allBuckets(8));
            ASSERT_EQ(diff.need.size(), 2U);
            EXPECT_TRUE(diff.have.empty());

            auto fetched = ma.collectEntriesForHashes(diff.need, 1U << 20U);
            auto resp = mb.decodeSync(fetched);
            ASSERT_TRUE(resp.has_value());
            EXPECT_TRUE(resp->needs.empty());
            EXPECT_EQ(mb.applySync(std::move(*resp)), 2);

            auto e1 = b.store.getVersioned("k1");
            ASSERT_TRUE(e1.has_value());
            EXPECT_EQ(e1->value, "v1");
            EXPECT_EQ(e1->version(), 10);
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

            auto meta_a =
                AntiEntropyManager::decodeMetaRows(ma.collectMetadata(allBuckets(8), 1U << 20U));
            auto meta_b =
                AntiEntropyManager::decodeMetaRows(mb.collectMetadata(allBuckets(8), 1U << 20U));
            ASSERT_TRUE(meta_a.has_value());
            ASSERT_TRUE(meta_b.has_value());

            // B is newer: A fetches, B pushes its winner back.
            auto diff_a = ma.diffRows(*meta_b, allBuckets(8));
            ASSERT_EQ(diff_a.need.size(), 1U);
            EXPECT_TRUE(diff_a.have.empty());
            auto diff_b = mb.diffRows(*meta_a, allBuckets(8));
            EXPECT_TRUE(diff_b.need.empty());
            ASSERT_EQ(diff_b.have.size(), 1U);

            // Stale write is a no-op: returns 0 (not counted as repair).
            auto stale = ma.collectEntriesByKeys({"k"}, 1U << 20U);
            auto stale_msg = mb.decodeSync(stale);
            ASSERT_TRUE(stale_msg.has_value());
            EXPECT_EQ(mb.applySync(std::move(*stale_msg)), 0);
            EXPECT_EQ(b.store.getVersioned("k")->value, "new");

            // Newer write wins: returns 1 (counted as repair).
            auto fresh = mb.collectEntriesByKeys({"k"}, 1U << 20U);
            auto fresh_msg = ma.decodeSync(fresh);
            ASSERT_TRUE(fresh_msg.has_value());
            EXPECT_EQ(ma.applySync(std::move(*fresh_msg)), 1);
            EXPECT_EQ(a.store.getVersioned("k")->value, "new");
        }

        TEST(AntiEntropySyncTest, MalformedBlobDoesNotCrash) {
            Node b;
            AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);
            EXPECT_EQ(mb.applyEntries(""), 0);
            EXPECT_EQ(mb.applyEntries("garbage"), 0);
            EXPECT_EQ(mb.applyEntries(std::string("\x05\x00\x00\x00", 4)), 0);
            EXPECT_EQ(mb.applyEntries(AntiEntropyManager::encodeDigest({1, 2, 3})), 0);
            EXPECT_TRUE(b.store.getVersioned("k1") == std::nullopt);
        }

        TEST(AntiEntropySyncTest, TtlPreserved) {
            Node a;
            Node b;
            VersionedEntry e = makeEntry("ephemeral", 3);
            e.setHasTtl(true);
            e.expires_at = a.clock.now() + seconds(60);
            ASSERT_TRUE(a.store.putVersioned("tk", std::move(e)).has_value());

            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
            AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);
            auto meta =
                AntiEntropyManager::decodeMetaRows(ma.collectMetadata(allBuckets(8), 1U << 20U));
            ASSERT_TRUE(meta.has_value());
            auto diff = mb.diffRows(*meta, allBuckets(8));
            ASSERT_EQ(diff.need.size(), 1U);
            auto fetched = ma.collectEntriesForHashes(diff.need, 1U << 20U);
            auto resp = mb.decodeSync(fetched);
            ASSERT_TRUE(resp.has_value());
            EXPECT_EQ(mb.applySync(std::move(*resp)), 1);

            auto got = b.store.getVersioned("tk");
            ASSERT_TRUE(got.has_value());
            EXPECT_EQ(got->value, "ephemeral");
            EXPECT_TRUE(got->hasTtl());
        }

        TEST(AntiEntropySyncTest, FetchBudgetIsByteBounded) {
            Node a;
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);

            // 1 KiB values: a small byte budget admits only a few entries.
            for (int i = 0; i < 50; i++) {
                auto key = "key" + std::to_string(i);
                ASSERT_TRUE(
                    a.store.putVersioned(key, makeEntry(std::string(1024, 'x'), i)).has_value()
                );
            }

            auto meta =
                AntiEntropyManager::decodeMetaRows(ma.collectMetadata(allBuckets(8), 1U << 20U));
            ASSERT_TRUE(meta.has_value());
            ASSERT_EQ(meta->size(), 50U);

            // Collect with a 3 KiB budget: must stop once full entries would
            // exceed it, and the take must be a deterministic prefix.
            std::vector<std::string> keys;
            for (int i = 0; i < 50; i++) {
                keys.push_back("key" + std::to_string(i));
            }
            auto blob = ma.collectEntriesByKeys(keys, 3U << 10U);
            auto msg = ma.decodeSync(blob);
            ASSERT_TRUE(msg.has_value());
            EXPECT_GT(msg->entries.size(), 0U);
            EXPECT_LT(msg->entries.size(), 50U);
            size_t bytes = 0;
            for (const auto& item : msg->entries) {
                bytes += 4 + item.key.size() + 8 + 8 + 1 + 4 + item.entry.value.size();
            }
            EXPECT_LE(bytes, 3U << 10U);
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

        TEST(AntiEntropyPartnerTest, PartnerSetCachesAcrossRounds) {
            Node a;
            a.ring.addNode("n1");
            a.ring.addNode("n2");
            ASSERT_TRUE(a.store.putVersioned("k1", makeEntry("v1", 1)).has_value());
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
            // First pick populates the cache via scan; the second must agree
            // without rescanning (single-node set round-robins to itself).
            auto first = ma.pickPartner(2);
            ASSERT_TRUE(first.has_value());
            auto second = ma.pickPartner(2);
            ASSERT_TRUE(second.has_value());
            EXPECT_EQ(*first, *second);
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

        TEST(AntiEntropyHandlerTest, BadSyncRejected) {
            Node b;
            AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);
            net::Request req;
            req.opcode = net::Opcode::AntiEntropySync;
            req.value = "not-a-sync-message";
            bool responded = false;
            mb.onSyncRequest("n1", req, [&](net::Response r) {
                responded = true;
                EXPECT_EQ(r.status, Errc::InvalidArgument);
            });
            EXPECT_TRUE(responded);
            EXPECT_EQ(b.metrics.replicationMetrics().anti_entropy_keys_repaired.load(), 0);
        }

        TEST(AntiEntropyHandlerTest, SyncAppliesEntriesAndCountsMetrics) {
            Node a;
            Node b;
            ASSERT_TRUE(a.store.putVersioned("k1", makeEntry("v1", 1)).has_value());
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
            AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8, &b.metrics);

            net::Request req;
            req.opcode = net::Opcode::AntiEntropySync;
            AntiEntropyManager::SyncMessage msg;
            msg.entries.push_back({"k1", makeEntry("v1", 1)});
            req.value = ma.encodeSync(msg);
            bool responded = false;
            mb.onSyncRequest("n1", req, [&](net::Response r) {
                responded = true;
                EXPECT_EQ(r.status, Errc::OK);
            });
            EXPECT_TRUE(responded);
            ASSERT_TRUE(b.store.getVersioned("k1").has_value());
            EXPECT_EQ(b.metrics.replicationMetrics().anti_entropy_keys_repaired.load(), 1);
        }

        TEST(AntiEntropyHandlerTest, SyncServesNeedlist) {
            Node a;
            Node b;
            ASSERT_TRUE(a.store.putVersioned("k1", makeEntry("v1", 9)).has_value());
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
            AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);

            // B asks for k1 by hash without knowing the key.
            uint64_t h = 0;
            {
                auto meta = AntiEntropyManager::decodeMetaRows(
                    ma.collectMetadata(allBuckets(8), 1U << 20U)
                );
                ASSERT_TRUE(meta.has_value());
                ASSERT_EQ(meta->size(), 1U);
                h = (*meta)[0].key_hash;
            }

            net::Request req;
            req.opcode = net::Opcode::AntiEntropySync;
            AntiEntropyManager::SyncMessage msg;
            msg.needs = {h, 0xDEADBEEF};
            req.value = mb.encodeSync(msg);
            net::Response resp;
            bool responded = false;
            ma.onSyncRequest("n2", req, [&](net::Response r) {
                responded = true;
                resp = std::move(r);
            });
            EXPECT_TRUE(responded);
            ASSERT_EQ(resp.status, Errc::OK);
            ASSERT_TRUE(resp.value.has_value());
            auto back = ma.decodeSync(*resp.value);
            ASSERT_TRUE(back.has_value());
            // Only the known hash is served; the unknown one is skipped, and
            // the response carries no needlist of its own.
            ASSERT_EQ(back->entries.size(), 1U);
            EXPECT_EQ(back->entries[0].key, "k1");
            EXPECT_TRUE(back->needs.empty());
        }

        // In-process loopback: routes AE RPCs straight into the peer manager,
        // synchronously (handlers guarantee a synchronous respond).
        class LoopbackTransport : public Transport {
        public:

            AntiEntropyManager* peer = nullptr;
            std::string from;

            void sendAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                SendCallback on_done
            ) override {
                on_done(ok());
            }

            void sendRequestAsync(
                const NodeId& /*to*/,
                const net::Request& req,
                RequestCallback on_done
            ) override {
                net::Response resp;
                if (req.opcode == net::Opcode::AntiEntropyDigest) {
                    peer->onDigestRequest(from, req, [&](net::Response r) { resp = std::move(r); });
                } else {
                    peer->onSyncRequest(from, req, [&](net::Response r) { resp = std::move(r); });
                }
                on_done(ok(std::move(resp)));
            }

            void onMessage(MessageHandler /*handler*/) override {}
        };

        struct LoopbackPair {
            MetricsCollector metrics_a;
            MetricsCollector metrics_b;
            RealClock clock_a;
            RealClock clock_b;
            std::unique_ptr<AntiEntropyManager> ma;
            std::unique_ptr<AntiEntropyManager> mb;
            ConsistentHashRing ring_a;
            ConsistentHashRing ring_b;
            LoopbackTransport transport_a;
            LoopbackTransport transport_b;
            LruStore store_a{1U << 20U};
            LruStore store_b{1U << 20U};

            LoopbackPair() {
                ring_a.addNode("n1");
                ring_a.addNode("n2");
                ring_b.addNode("n1");
                ring_b.addNode("n2");
                ma = std::make_unique<AntiEntropyManager>(
                    store_a,
                    ring_a,
                    "n1",
                    clock_a,
                    transport_a,
                    8,
                    &metrics_a
                );
                mb = std::make_unique<AntiEntropyManager>(
                    store_b,
                    ring_b,
                    "n2",
                    clock_b,
                    transport_b,
                    8,
                    &metrics_b
                );
                transport_a.peer = mb.get();
                transport_a.from = "n1";
                transport_b.peer = ma.get();
                transport_b.from = "n2";
            }

            // Rounds alternate until the digests agree or the budget runs out.
            // Returns the rounds run.
            auto converge(int max_rounds = 10) const -> int {
                int rounds = 0;
                for (; rounds < max_rounds; ++rounds) {
                    ma->runRound(2);
                    mb->runRound(2);
                    if (ma->computeDigest() == mb->computeDigest()) {
                        ++rounds;
                        break;
                    }
                }
                return rounds;
            }
        };

        TEST(AntiEntropyExchangeTest, TwoManagersConverge) {
            LoopbackPair pair;
            ASSERT_TRUE(pair.store_a.putVersioned("k1", makeEntry("a2", 2)).has_value());
            ASSERT_TRUE(pair.store_b.putVersioned("k1", makeEntry("b1", 1)).has_value());
            ASSERT_TRUE(pair.store_b.putVersioned("k2", makeEntry("b2", 5)).has_value());

            EXPECT_LE(pair.converge(), 10);

            EXPECT_EQ(pair.store_a.getVersioned("k1")->value, "a2");
            EXPECT_EQ(pair.store_b.getVersioned("k1")->value, "a2");
            EXPECT_EQ(pair.store_a.getVersioned("k2")->value, "b2");
            EXPECT_EQ(pair.store_b.getVersioned("k2")->value, "b2");
            EXPECT_EQ(pair.ma->computeDigest(), pair.mb->computeDigest());
            // Metadata flowed both ways; entries were fetched, not flooded.
            EXPECT_GT(pair.metrics_a.replicationMetrics().anti_entropy_meta_rows_recv.load(), 0U);
            EXPECT_GT(pair.metrics_b.replicationMetrics().anti_entropy_meta_rows_sent.load(), 0U);
            EXPECT_GT(pair.metrics_a.replicationMetrics().anti_entropy_entries_fetched.load(), 0U);
        }

        TEST(AntiEntropyExchangeTest, ConvergedPairIsQuiet) {
            LoopbackPair pair;
            ASSERT_TRUE(pair.store_a.putVersioned("k1", makeEntry("same", 1)).has_value());
            ASSERT_TRUE(pair.store_b.putVersioned("k1", makeEntry("same", 1)).has_value());

            pair.ma->runRound(2);
            pair.mb->runRound(2);

            // No divergence: no metadata rows, no entries fetched.
            EXPECT_EQ(pair.metrics_a.replicationMetrics().anti_entropy_meta_rows_recv.load(), 0U);
            EXPECT_EQ(pair.metrics_b.replicationMetrics().anti_entropy_meta_rows_recv.load(), 0U);
            EXPECT_EQ(pair.metrics_a.replicationMetrics().anti_entropy_entries_fetched.load(), 0U);
        }

        TEST(AntiEntropyIncrementalTest, MatchesFullScan) {
            // Random puts, overwrites, removes and TTL expiries, including
            // under capacity pressure (small store forces evictions through
            // every hooked path: insert, overwrite, remove, expire, evict).
            // Past-dated TTLs expire deterministically without clock control.
            RealClock clock;
            LruStore store(1U << 20U);
            LruStore tiny(512);
            ConsistentHashRing ring;
            FakeTransport transport;
            constexpr uint32_t K_BUCKETS = 16;
            AntiEntropyManager manager(store, ring, "n1", clock, transport, K_BUCKETS);
            AntiEntropyManager tiny_mgr(tiny, ring, "n1", clock, transport, K_BUCKETS);

            // Non-constant seed (bugprone-random-generator-seed forbids
            // fixed ones); logged so a failure stays reproducible.
            const auto seed = std::random_device{}();
            RecordProperty("rng_seed", std::to_string(seed));
            std::mt19937_64 rng{seed};
            auto key = [&](int i) {
                return "key" + std::to_string(i);
            };
            auto expired_entry = [&](Version v) {
                VersionedEntry e = makeEntry("ttl", v, 1);
                e.setHasTtl(true);
                e.expires_at = clock.now() - seconds(1);
                return e;
            };

            for (int round = 0; round < 5; ++round) {
                for (int i = 0; i < 200; ++i) {
                    int op = static_cast<int>(rng() % 4);
                    int k = static_cast<int>(rng() % 30);
                    if (op < 2) {
                        Version v = static_cast<Version>(rng() % 50 + 1);
                        uint64_t w = rng() % 3;
                        // Any outcome is a legal test input (stale rejects,
                        // capacity errors): the property must hold regardless.
                        // NOLINTNEXTLINE(bugprone-unused-return-value)
                        (void)store.putVersioned(key(k), makeEntry("v", v, w));
                        // NOLINTNEXTLINE(bugprone-unused-return-value)
                        (void)tiny.putVersioned(key(k), makeEntry("v", v, w));
                    } else if (op == 2) {
                        (void)store.remove(key(k));
                        (void)tiny.remove(key(k));
                    } else {
                        Version v = 100 + static_cast<Version>(i);
                        // NOLINTNEXTLINE(bugprone-unused-return-value)
                        (void)store.putVersioned(key(k), expired_entry(v));
                        // NOLINTNEXTLINE(bugprone-unused-return-value)
                        (void)tiny.putVersioned(key(k), expired_entry(v));
                        (void)store.evictExpired();
                        (void)tiny.evictExpired();
                    }
                }
                // Incremental digest (accumulators + sink) must equal a fresh
                // full scan after arbitrary mutation sequences.
                EXPECT_EQ(manager.digestForRound(), manager.computeDigest());
                EXPECT_EQ(tiny_mgr.digestForRound(), tiny_mgr.computeDigest());
            }
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

        TEST(AntiEntropySyncTest, StaleEntryNotCountedAsRepair) {
            Node a;
            Node b;
            // B already has k1 at version 10 (newer than A's version 1).
            ASSERT_TRUE(a.store.putVersioned("k1", makeEntry("old", 1)).has_value());
            ASSERT_TRUE(b.store.putVersioned("k1", makeEntry("new", 10)).has_value());

            AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8, &b.metrics);
            // A sends a stale entry via an entries-only sync blob — B should
            // NOT count it as a repair.
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
            AntiEntropyManager::SyncMessage msg;
            msg.entries.push_back({"k1", makeEntry("old", 1)});
            auto blob = ma.encodeSync(msg);

            // Apply to B — stale entry is a no-op, repair count should be 0.
            size_t applied = mb.applyEntries(blob);
            EXPECT_EQ(applied, 0);
            // k1 should still have B's newer value.
            EXPECT_EQ(b.store.getVersioned("k1")->value, "new");
            EXPECT_EQ(b.metrics.replicationMetrics().anti_entropy_keys_repaired.load(), 0);
        }

        TEST(AntiEntropySyncTest, NewEntryCountedAsRepair) {
            Node a;
            Node b;
            // A has k1 at version 5, B has k1 at version 1 (A is newer).
            ASSERT_TRUE(a.store.putVersioned("k1", makeEntry("newer", 5)).has_value());
            ASSERT_TRUE(b.store.putVersioned("k1", makeEntry("older", 1)).has_value());

            AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8, &b.metrics);
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
            AntiEntropyManager::SyncMessage msg;
            msg.entries.push_back({"k1", makeEntry("newer", 5)});
            auto blob = ma.encodeSync(msg);

            size_t applied = mb.applyEntries(blob);
            EXPECT_EQ(applied, 1);
            EXPECT_EQ(b.store.getVersioned("k1")->value, "newer");
        }

        // Deferred scripted peer: captures digest requests and answers them on
        // demand, so the round's async callback can run after runRound returned
        // (as with a real transport) instead of inline.
        class DeferredScriptedTransport : public Transport {
        public:

            explicit DeferredScriptedTransport(AntiEntropyManager* manager, uint32_t buckets) :
                manager_(manager),
                buckets_(buckets) {}

            void setManager(AntiEntropyManager* manager) {
                manager_ = manager;
            }

            void sendAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                SendCallback on_done
            ) override {
                on_done(ok());
            }

            void sendRequestAsync(
                const NodeId& /*to*/,
                const net::Request& req,
                RequestCallback on_done
            ) override {
                if (req.opcode == net::Opcode::AntiEntropyDigest) {
                    seen_digest_trace = req.trace_id;
                    auto zeros = std::vector<uint64_t>(buckets_, 0);
                    net::Response resp{
                        .status = Errc::OK,
                        .value = AntiEntropyManager::encodeDigest(zeros)
                            + AntiEntropyManager::encodeMetaRows({}),
                    };
                    pending_digest_ = std::move(on_done);
                    pending_digest_response_ = std::move(resp);
                } else {
                    net::Response resp{
                        .status = Errc::OK,
                        .value = manager_->encodeSync({{}, {}}),
                    };
                    on_done(ok(std::move(resp)));
                }
            }

            void onMessage(MessageHandler /*handler*/) override {}

            void releaseDigest() {
                pending_digest_(ok(std::move(pending_digest_response_)));
            }

            uint64_t seen_digest_trace = 0;

        private:

            AntiEntropyManager* manager_ = nullptr;
            uint32_t buckets_;
            RequestCallback pending_digest_;
            net::Response pending_digest_response_;
        };

        TEST(AntiEntropyRoundTest, DigestCallbackKeepsRoundTrace) {
            constexpr uint32_t K_BUCKETS = 8;
            RealClock clock;
            LruStore store(1U << 20U);
            ASSERT_TRUE(store.putVersioned("k1", makeEntry("v1", 1)).has_value());
            ConsistentHashRing ring;
            ring.addNode("n1");
            ring.addNode("n2");
            DeferredScriptedTransport transport(nullptr, K_BUCKETS);
            AntiEntropyManager manager(store, ring, "n1", clock, transport, K_BUCKETS);
            transport.setManager(&manager);
            auto sub = std::make_shared<RecordingSubscriber>();

            {
                auto prev = globalSubscriber();
                setGlobalSubscriber(sub);
                manager.runRound(2);
                // Release the digest response under a decoy ambient span: without
                // the round-context attach in the callback, the "round
                // completed" event would carry the decoy's trace instead of the
                // round's — on a real transport the callback runs on an
                // unrelated I/O thread after runRound returned.
                Span decoy("decoy");
                transport.releaseDigest();
                setGlobalSubscriber(prev);

                auto spans = sub->spans();
                const RecordingSubscriber::RecordedSpan* round = nullptr;
                for (const auto& s : spans) {
                    if (s.operation == "anti_entropy.round") {
                        round = &s;
                    }
                }
                ASSERT_NE(round, nullptr);
                EXPECT_NE(round->trace_id, decoy.traceId());
                // The digest request carried the round's trace to the peer.
                EXPECT_EQ(transport.seen_digest_trace, round->trace_id);
                // The async completion still belongs to the round's trace.
                bool completed_on_round = false;
                for (const auto& log : sub->logEvents()) {
                    if (log.message == "round completed" && log.trace_id == round->trace_id) {
                        completed_on_round = true;
                    }
                }
                EXPECT_TRUE(completed_on_round) << "round completion lost the round trace";
                // The async result span joins the round trace with its outcome.
                const RecordingSubscriber::RecordedSpan* result = nullptr;
                for (const auto& s : spans) {
                    if (s.operation == "anti_entropy.round.result") {
                        result = &s;
                    }
                }
                ASSERT_NE(result, nullptr);
                EXPECT_EQ(result->trace_id, round->trace_id);
                EXPECT_EQ(result->status, SpanStatus::Ok);
                bool outcome_completed = false;
                bool partner_seen = false;
                for (const auto& a : result->attributes) {
                    if (a.key == "outcome" && a.str_val == "completed") {
                        outcome_completed = true;
                    }
                    if (a.key == "partner" && a.str_val == "n2") {
                        partner_seen = true;
                    }
                }
                EXPECT_TRUE(outcome_completed);
                EXPECT_TRUE(partner_seen);
            }
        }

        TEST(AntiEntropyRoundTest, NoPartnerSkipsRoundWithOutcome) {
            RealClock clock;
            LruStore store(1U << 20U);
            ConsistentHashRing ring;
            ring.addNode("n1");
            FakeTransport transport;
            AntiEntropyManager manager(store, ring, "n1", clock, transport, 8);
            auto sub = std::make_shared<RecordingSubscriber>();

            {
                auto prev = globalSubscriber();
                setGlobalSubscriber(sub);
                manager.runRound(1);
                setGlobalSubscriber(prev);

                auto spans = sub->spans();
                const RecordingSubscriber::RecordedSpan* round = nullptr;
                for (const auto& s : spans) {
                    if (s.operation == "anti_entropy.round") {
                        round = &s;
                    }
                }
                ASSERT_NE(round, nullptr);
                // Skipped, not failed: nothing was attempted, but the reason
                // is recorded so the skip is distinguishable from a bug.
                EXPECT_EQ(round->status, SpanStatus::Unset);
                bool skipped = false;
                bool no_partner = false;
                for (const auto& a : round->attributes) {
                    if (a.key == "outcome" && a.str_val == "skipped") {
                        skipped = true;
                    }
                    if (a.key == "reason" && a.str_val == "no-partner") {
                        no_partner = true;
                    }
                }
                EXPECT_TRUE(skipped);
                EXPECT_TRUE(no_partner);
            }
        }

        TEST(AntiEntropyRoundTest, DigestFailureReportsErrorResult) {
            RealClock clock;
            LruStore store(1U << 20U);
            ASSERT_TRUE(store.putVersioned("k1", makeEntry("v1", 1)).has_value());
            ConsistentHashRing ring;
            ring.addNode("n1");
            ring.addNode("n2");
            // FakeTransport answers digest requests with an inline error, so
            // the round's async callback runs the failure path.
            FakeTransport transport;
            AntiEntropyManager manager(store, ring, "n1", clock, transport, 8);
            auto sub = std::make_shared<RecordingSubscriber>();

            {
                auto prev = globalSubscriber();
                setGlobalSubscriber(sub);
                manager.runRound(2);
                setGlobalSubscriber(prev);

                auto spans = sub->spans();
                const RecordingSubscriber::RecordedSpan* round = nullptr;
                const RecordingSubscriber::RecordedSpan* result = nullptr;
                for (const auto& s : spans) {
                    if (s.operation == "anti_entropy.round") {
                        round = &s;
                    }
                    if (s.operation == "anti_entropy.round.result") {
                        result = &s;
                    }
                }
                ASSERT_NE(round, nullptr);
                ASSERT_NE(result, nullptr);
                EXPECT_EQ(result->trace_id, round->trace_id);
                EXPECT_EQ(result->status, SpanStatus::Error);
                bool digest_failed = false;
                for (const auto& a : result->attributes) {
                    if (a.key == "outcome" && a.str_val == "digest-failed") {
                        digest_failed = true;
                    }
                }
                EXPECT_TRUE(digest_failed);
            }
        }
    } // namespace
} // namespace cinder
