#include <chrono>
#include <gtest/gtest.h>
#include <memory>
#include <random>
#include <string>
#include <vector>
#include <xxhash.h>

#include "cinder/cluster/transport.hpp"
#include "cinder/common/metrics.hpp"
#include "cinder/common/tracing.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/node/anti_entropy/core.hpp"
#include "cinder/store/detail/io_utils.hpp"
#include "cinder/store/lru_store.hpp"
#include "unit/anti_entropy_fixtures.hpp"

using std::chrono::seconds;

namespace cinder {
    namespace {

        auto encodeCounterBundleForTest(
            const std::vector<std::pair<std::string, std::string>>& counters
        ) -> std::string {
            size_t total = 2 * sizeof(uint32_t);
            for (const auto& [key, state] : counters) {
                total += 2 * sizeof(uint32_t) + key.size() + state.size();
            }
            std::vector<std::byte> buf(total);
            detail::BufferWriter out(buf);
            out.write(uint32_t{0x43414531}); // CAE1
            out.write(static_cast<uint32_t>(counters.size()));
            for (const auto& [key, state] : counters) {
                out.write(static_cast<uint32_t>(key.size()));
                out.writeString(key);
                out.write(static_cast<uint32_t>(state.size()));
                out.writeString(state);
            }
            return detail::toWireString(buf);
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

        TEST(AntiEntropyCounterTest, ExchangesAndMergesBothDirections) {
            Node a;
            Node b;
            CrdtCounterStore counters_a(1U << 20U);
            CrdtCounterStore counters_b(1U << 20U);
            ASSERT_TRUE(counters_a.increment("visits", "n1", 5).has_value());
            ASSERT_TRUE(counters_b.decrement("visits", "n2", 2).has_value());

            AntiEntropyManager ae_a(
                a.store,
                a.ring,
                "n1",
                a.clock,
                a.transport,
                8,
                nullptr,
                1U << 20U,
                AntiEntropyManager::K_DEFAULT_FULL_REHASH_INTERVAL,
                &counters_a
            );
            AntiEntropyManager ae_b(
                b.store,
                b.ring,
                "n2",
                b.clock,
                b.transport,
                8,
                nullptr,
                1U << 20U,
                AntiEntropyManager::K_DEFAULT_FULL_REHASH_INTERVAL,
                &counters_b
            );

            auto encoded = crdt::encodeCounter(*counters_a.get("visits"));
            net::Request req;
            req.opcode = net::Opcode::CounterAntiEntropy;
            req.value = encodeCounterBundleForTest({{"visits", encoded}});
            std::optional<net::Response> response;
            ae_b.onCounterAntiEntropy("n1", req, [&](net::Response r) { response = std::move(r); });
            ASSERT_TRUE(response.has_value());
            ASSERT_EQ(response->status, Errc::OK);
            ASSERT_TRUE(response->value.has_value());

            net::Request reply;
            reply.value = *response->value;
            std::optional<net::Response> final_response;
            ae_a.onCounterAntiEntropy("n2", reply, [&](net::Response r) {
                final_response = std::move(r);
            });
            ASSERT_TRUE(final_response.has_value());
            EXPECT_EQ(counters_a.value("visits"), 3);
            EXPECT_EQ(counters_b.value("visits"), 3);
            EXPECT_EQ(counters_a.get("visits"), counters_b.get("visits"));
        }

        TEST(AntiEntropyCounterTest, RejectsMalformedAndOverBudgetBundle) {
            Node node;
            CrdtCounterStore counters(1U << 20U);
            AntiEntropyManager ae(
                node.store,
                node.ring,
                "n1",
                node.clock,
                node.transport,
                8,
                nullptr,
                32,
                AntiEntropyManager::K_DEFAULT_FULL_REHASH_INTERVAL,
                &counters
            );
            net::Request req;
            req.opcode = net::Opcode::CounterAntiEntropy;
            req.value = "bad";
            std::optional<net::Response> bad;
            ae.onCounterAntiEntropy("n2", req, [&](net::Response r) { bad = std::move(r); });
            ASSERT_TRUE(bad.has_value());
            EXPECT_EQ(bad->status, Errc::InvalidArgument);

            ASSERT_TRUE(
                counters.increment("key", "actor-name-that-exceeds-the-budget", 1).has_value()
            );
            req.value =
                encodeCounterBundleForTest({{"key", crdt::encodeCounter(*counters.get("key"))}});
            std::optional<net::Response> over;
            ae.onCounterAntiEntropy("n2", req, [&](net::Response r) { over = std::move(r); });
            ASSERT_TRUE(over.has_value());
            EXPECT_EQ(over->status, Errc::CapacityExceeded);
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
                    auto respond = [&](net::Response r) {
                        resp = std::move(r);
                    };
                    if (req.key == "bucket-refinement-v3") {
                        peer->onBucketRefinement(from, req, respond);
                    } else {
                        peer->onDigestRequest(from, req, respond);
                    }
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

        TEST(AntiEntropyExchangeTest, LargeBucketUsesIbltAndConverges) {
            LoopbackPair pair;
            std::vector<std::string> keys;
            for (uint64_t i = 0; keys.size() < 48; ++i) {
                auto key = "iblt-key-" + std::to_string(i);
                if (XXH3_64bits(key.data(), key.size()) % 8 == 3) {
                    keys.push_back(std::move(key));
                }
            }
            for (size_t i = 0; i < keys.size(); ++i) {
                ASSERT_TRUE(
                    pair.store_a
                        .putVersioned(
                            keys[i],
                            makeEntry("local-" + std::to_string(i), static_cast<Version>(i + 10))
                        )
                        .has_value()
                );
                ASSERT_TRUE(pair.store_b
                                .putVersioned(
                                    keys[i],
                                    makeEntry(
                                        i == 0 ? "remote-0" : "local-" + std::to_string(i),
                                        i == 0 ? 1 : static_cast<Version>(i + 10)
                                    )
                                )
                                .has_value());
            }

            EXPECT_LE(pair.converge(), 10);
            EXPECT_EQ(pair.ma->computeDigest(), pair.mb->computeDigest());
            for (const auto& key : keys) {
                ASSERT_TRUE(pair.store_a.getVersioned(key).has_value());
                ASSERT_TRUE(pair.store_b.getVersioned(key).has_value());
                EXPECT_EQ(
                    pair.store_a.getVersioned(key)->value,
                    pair.store_b.getVersioned(key)->value
                );
            }
            EXPECT_GT(
                pair.metrics_a.replicationMetrics().anti_entropy_iblt_successes.load()
                    + pair.metrics_b.replicationMetrics().anti_entropy_iblt_successes.load(),
                0U
            );
        }

        TEST(AntiEntropyExchangeTest, UnpeelableIbltFallsBackToMetadata) {
            LoopbackPair pair;
            std::vector<std::string> keys;
            for (uint64_t i = 0; keys.size() < 80; ++i) {
                auto key = "iblt-fallback-" + std::to_string(i);
                if (XXH3_64bits(key.data(), key.size()) % 8 == 5) {
                    keys.push_back(std::move(key));
                }
            }
            for (size_t i = 0; i < keys.size(); ++i) {
                ASSERT_TRUE(
                    pair.store_a
                        .putVersioned(
                            keys[i],
                            makeEntry("a-" + std::to_string(i), static_cast<Version>(i + 100))
                        )
                        .has_value()
                );
                ASSERT_TRUE(pair.store_b
                                .putVersioned(
                                    keys[i],
                                    makeEntry("b-" + std::to_string(i), static_cast<Version>(i + 1))
                                )
                                .has_value());
            }

            EXPECT_LE(pair.converge(12), 12);
            EXPECT_EQ(pair.ma->computeDigest(), pair.mb->computeDigest());
            EXPECT_GT(
                pair.metrics_a.replicationMetrics().anti_entropy_iblt_fallbacks.load()
                    + pair.metrics_b.replicationMetrics().anti_entropy_iblt_fallbacks.load(),
                0U
            );
            for (const auto& key : keys) {
                ASSERT_TRUE(pair.store_a.getVersioned(key).has_value());
                ASSERT_TRUE(pair.store_b.getVersioned(key).has_value());
                EXPECT_EQ(
                    pair.store_a.getVersioned(key)->value,
                    pair.store_b.getVersioned(key)->value
                );
            }
        }

        TEST(AntiEntropyExchangeTest, DisjointLargeBucketsFallBackToMetadata) {
            // Both sides hold 120 different keys in the same bucket: the 240-row
            // symmetric difference cannot peel from 64 cells, so the correct
            // path is the explicit-metadata fallback — convergence via P2.
            LoopbackPair pair;
            std::vector<std::string> keys_a;
            std::vector<std::string> keys_b;
            for (uint64_t i = 0; keys_a.size() < 120 || keys_b.size() < 120; ++i) {
                auto key_a = "iblt-disjoint-a-" + std::to_string(i);
                if (keys_a.size() < 120 && XXH3_64bits(key_a.data(), key_a.size()) % 8 == 6) {
                    keys_a.push_back(std::move(key_a));
                }
                auto key_b = "iblt-disjoint-b-" + std::to_string(i);
                if (keys_b.size() < 120 && XXH3_64bits(key_b.data(), key_b.size()) % 8 == 6) {
                    keys_b.push_back(std::move(key_b));
                }
            }
            for (size_t i = 0; i < keys_a.size(); ++i) {
                ASSERT_TRUE(pair.store_a
                                .putVersioned(
                                    keys_a[i],
                                    makeEntry("a-" + std::to_string(i), static_cast<Version>(i + 1))
                                )
                                .has_value());
            }
            for (size_t i = 0; i < keys_b.size(); ++i) {
                ASSERT_TRUE(pair.store_b
                                .putVersioned(
                                    keys_b[i],
                                    makeEntry("b-" + std::to_string(i), static_cast<Version>(i + 1))
                                )
                                .has_value());
            }
            // Disjoint key sets: neither side's keys appear on the other.
            for (const auto& key : keys_a) {
                ASSERT_FALSE(pair.store_b.getVersioned(key).has_value());
            }
            EXPECT_LE(pair.converge(12), 12);
            EXPECT_EQ(pair.ma->computeDigest(), pair.mb->computeDigest());
            for (const auto& key : keys_a) {
                EXPECT_TRUE(pair.store_b.getVersioned(key).has_value());
            }
            for (const auto& key : keys_b) {
                EXPECT_TRUE(pair.store_a.getVersioned(key).has_value());
            }
            EXPECT_GT(
                pair.metrics_a.replicationMetrics().anti_entropy_iblt_fallbacks.load()
                    + pair.metrics_b.replicationMetrics().anti_entropy_iblt_fallbacks.load(),
                0U
            );
        }

        TEST(AntiEntropyExchangeTest, DeletePropagatesAndStaysDeleted) {
            LoopbackPair pair;
            ASSERT_TRUE(pair.store_a.putVersioned("gone", makeEntry("v", 3)).has_value());
            ASSERT_TRUE(pair.store_b.putVersioned("gone", makeEntry("v", 3)).has_value());

            // Delete on A with a newer version; the tombstone must reach B
            // through anti-entropy and must not be resurrected by B's live
            // copy in later rounds (the pre-tombstone resurrection bug).
            ASSERT_TRUE(pair.store_a.removeVersioned("gone", 5, 7).has_value());
            EXPECT_FALSE(pair.store_a.get("gone").has_value());

            EXPECT_LE(pair.converge(12), 12);
            EXPECT_EQ(pair.ma->computeDigest(), pair.mb->computeDigest());
            EXPECT_FALSE(pair.store_a.get("gone").has_value());
            EXPECT_FALSE(pair.store_b.get("gone").has_value());
            // Absent on both, but the delete marker survives until GC.
            auto tomb_a = pair.store_a.getVersioned("gone");
            auto tomb_b = pair.store_b.getVersioned("gone");
            ASSERT_TRUE(tomb_a.has_value());
            ASSERT_TRUE(tomb_b.has_value());
            EXPECT_TRUE(tomb_a->isTombstone());
            EXPECT_TRUE(tomb_b->isTombstone());
        }

        TEST(AntiEntropyExchangeTest, DeleteOnEmptyNodeHealsReplica) {
            LoopbackPair pair;
            // A never held the key: the delete still plants a tombstone so
            // the replica's live copy cannot resurrect it.
            ASSERT_TRUE(pair.store_b.putVersioned("ghost", makeEntry("v", 3)).has_value());
            ASSERT_TRUE(pair.store_a.removeVersioned("ghost", 5, 7).has_value());

            EXPECT_LE(pair.converge(12), 12);
            EXPECT_EQ(pair.ma->computeDigest(), pair.mb->computeDigest());
            EXPECT_FALSE(pair.store_a.get("ghost").has_value());
            EXPECT_FALSE(pair.store_b.get("ghost").has_value());
            auto tomb = pair.store_b.getVersioned("ghost");
            ASSERT_TRUE(tomb.has_value());
            EXPECT_TRUE(tomb->isTombstone());
        }

        TEST(AntiEntropyExchangeTest, TombstoneWinsVersionWriterTies) {
            // Equal (version, writer), contradictory delete state: the
            // tombstone wins so both sides converge instead of stalling.
            Node node;
            AntiEntropyManager
                manager(node.store, node.ring, "n1", node.clock, node.transport, 8, &node.metrics);
            ASSERT_TRUE(node.store.putVersioned("tie", makeEntry("live", 4, 7)).has_value());
            auto buckets = manager.computeDigest();
            uint32_t live_bucket = 0;
            while (live_bucket < 8 && buckets[live_bucket] == 0) {
                ++live_bucket;
            }
            ASSERT_LT(live_bucket, 8U);
            const uint64_t key_hash = XXH3_64bits("tie", 3);
            AntiEntropyManager::MetaRow live_row{
                .bucket = live_bucket,
                .tombstone = false,
                .key_hash = key_hash,
                .version = 4,
                .writer = 7,
            };
            AntiEntropyManager::MetaRow dead_row = live_row;
            dead_row.tombstone = true;

            // Remote tombstone at the tied identity → fetch the delete.
            auto fetch = manager.diffRows({dead_row}, {live_bucket}, {live_bucket});
            ASSERT_EQ(fetch.need.size(), 1U);
            EXPECT_EQ(fetch.need[0], key_hash);
            EXPECT_TRUE(fetch.have.empty());

            // Remote live at the tied identity vs our tombstone → push it.
            ASSERT_TRUE(node.store.removeVersioned("tie", 4, 7).has_value());
            auto push = manager.diffRows({live_row}, {live_bucket}, {live_bucket});
            EXPECT_TRUE(push.need.empty());
            ASSERT_EQ(push.have.size(), 1U);
            EXPECT_EQ(push.have[0].second, "tie");
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

            explicit DeferredScriptedTransport(AntiEntropyManager* manager) :
                manager_(manager) {}

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
                    auto local = manager_->digestForRound();
                    net::Response resp{
                        .status = Errc::OK,
                        .value = AntiEntropyManager::encodeDigestResponse({
                            .group_hashes = AntiEntropyManager::groupDigests(local),
                            .bucket_hashes = {},
                            .covered_buckets = {},
                            .rows = {},
                        }),
                    };

                    pending_digest_ = std::move(on_done);
                    pending_digest_response_ = std::move(resp);
                } else if (req.opcode == net::Opcode::AntiEntropyDigest) {
                    on_done(err<net::Response>(Error(Errc::NotReady, "refinement not scripted")));
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
            RequestCallback pending_digest_;
            net::Response pending_digest_response_;
        };

        TEST(AntiEntropyHandlerTest, MatchingGroupDigestSendsNoBucketHashesOrRows) {
            Node a;
            ASSERT_TRUE(a.store.putVersioned("key", makeEntry("v", 1)).has_value());
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 32);
            auto groups = AntiEntropyManager::groupDigests(ma.computeDigest());
            net::Request req;
            req.opcode = net::Opcode::AntiEntropyDigest;
            req.value = AntiEntropyManager::encodeGroupDigest(groups);
            std::optional<net::Response> response;
            ma.onDigestRequest("n2", req, [&](net::Response r) { response = std::move(r); });
            ASSERT_TRUE(response.has_value());
            ASSERT_TRUE(response->value.has_value());
            auto decoded =
                AntiEntropyManager::decodeDigestResponse(*response->value, groups.size());
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded->group_hashes, groups);
            EXPECT_TRUE(decoded->bucket_hashes.empty());
            EXPECT_TRUE(decoded->covered_buckets.empty());
            EXPECT_TRUE(decoded->rows.empty());
        }

        TEST(AntiEntropyHandlerTest, DivergentGroupRefinesToItsBuckets) {
            constexpr uint32_t K_BUCKETS = 64;
            Node node;
            ASSERT_TRUE(node.store.putVersioned("key", makeEntry("v", 1)).has_value());
            AntiEntropyManager manager(
                node.store,
                node.ring,
                "n1",
                node.clock,
                node.transport,
                K_BUCKETS,
                &node.metrics
            );

            // Phase 1 carries group hashes only — no bucket hashes, coverage,
            // or rows, even when groups diverge.
            auto local_buckets = manager.computeDigest();
            auto local_groups = AntiEntropyManager::groupDigests(local_buckets);
            auto remote_groups = local_groups;
            remote_groups[1] ^= 0xfeedbeefU;
            net::Request req;
            req.opcode = net::Opcode::AntiEntropyDigest;
            req.value = AntiEntropyManager::encodeGroupDigest(remote_groups);
            std::optional<net::Response> response;
            manager.onDigestRequest("n2", req, [&](net::Response r) { response = std::move(r); });
            ASSERT_TRUE(response.has_value());
            ASSERT_EQ(response->status, Errc::OK);
            ASSERT_TRUE(response->value.has_value());
            auto decoded =
                AntiEntropyManager::decodeDigestResponse(*response->value, remote_groups.size());
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded->group_hashes, local_groups);
            EXPECT_TRUE(decoded->bucket_hashes.empty());
            EXPECT_TRUE(decoded->covered_buckets.empty());
            EXPECT_TRUE(decoded->rows.empty());

            // Phase 2: the initiator names the 16 buckets of the group that
            // actually holds data. Only buckets whose hashes differ get
            // metadata; empty-on-both-sides buckets agree silently.
            auto live = manager.computeDigest();
            uint32_t live_bucket = 0;
            while (live_bucket < K_BUCKETS && live[live_bucket] == 0) {
                ++live_bucket;
            }
            ASSERT_LT(live_bucket, K_BUCKETS);
            const uint32_t live_group = live_bucket / AntiEntropyManager::K_BUCKETS_PER_GROUP;
            AntiEntropyManager::BucketRefinement refinement;
            for (uint32_t bucket = live_group * AntiEntropyManager::K_BUCKETS_PER_GROUP;
                 bucket < (live_group + 1) * AntiEntropyManager::K_BUCKETS_PER_GROUP;
                 ++bucket) {
                refinement.bucket_hashes.emplace_back(bucket, 0);
            }
            net::Request refine_req;
            refine_req.opcode = net::Opcode::AntiEntropyDigest;
            refine_req.key = "bucket-refinement-v3";
            refine_req.value = AntiEntropyManager::encodeBucketRefinement(refinement);
            std::optional<net::Response> refined;
            manager.onBucketRefinement("n2", refine_req, [&](net::Response r) {
                refined = std::move(r);
            });
            ASSERT_TRUE(refined.has_value());
            ASSERT_EQ(refined->status, Errc::OK);
            ASSERT_TRUE(refined->value.has_value());
            auto refined_decoded =
                AntiEntropyManager::decodeDigestResponse(*refined->value, remote_groups.size());
            ASSERT_TRUE(refined_decoded.has_value());
            ASSERT_EQ(
                refined_decoded->bucket_hashes.size(),
                AntiEntropyManager::K_BUCKETS_PER_GROUP
            );
            for (uint32_t i = 0; i < AntiEntropyManager::K_BUCKETS_PER_GROUP; ++i) {
                EXPECT_EQ(
                    refined_decoded->bucket_hashes[i].first,
                    live_group * AntiEntropyManager::K_BUCKETS_PER_GROUP + i
                );
            }
            // Only the bucket whose hash actually differs is covered: the 15
            // empty buckets agree with the requester's zeros, so no metadata
            // is needed for them. Coverage still distinguishes them from
            // deferred buckets (none here).
            ASSERT_EQ(refined_decoded->covered_buckets.size(), 1U);
            ASSERT_EQ(refined_decoded->rows.size(), 1U);
            EXPECT_EQ(refined_decoded->rows[0].bucket, refined_decoded->covered_buckets[0]);
            EXPECT_EQ(node.metrics.replicationMetrics().anti_entropy_meta_rows_sent.load(), 1U);
        }

        TEST(AntiEntropyRoundTest, DigestCallbackKeepsRoundTrace) {
            constexpr uint32_t K_BUCKETS = 8;
            RealClock clock;
            LruStore store(1U << 20U);
            ASSERT_TRUE(store.putVersioned("k1", makeEntry("v1", 1)).has_value());
            ConsistentHashRing ring;
            ring.addNode("n1");
            ring.addNode("n2");
            DeferredScriptedTransport transport(nullptr);
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
