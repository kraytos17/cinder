#include <chrono>
#include <gtest/gtest.h>
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
            EXPECT_FALSE(
                AntiEntropyManager::decodeDigest(blob.substr(0, blob.size() - 1), 3).has_value()
            );
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

            EXPECT_FALSE(
                AntiEntropyManager::decodeBucketIds(blob.substr(0, blob.size() - 1)).has_value()
            );
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

            // Stale write is a no-op: returns 0 (not counted as repair).
            EXPECT_EQ(mb.applyEntries(ma.collectEntries(allBuckets(8))), 0);
            EXPECT_EQ(b.store.getVersioned("k")->value, "new");

            // Newer write wins: returns 1 (counted as repair).
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
            e.setHasTtl(true);
            e.expires_at = a.clock.now() + seconds(60);
            ASSERT_TRUE(a.store.putVersioned("tk", std::move(e)).has_value());

            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
            AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8);
            EXPECT_EQ(mb.applyEntries(ma.collectEntries(allBuckets(8))), 1);

            auto got = b.store.getVersioned("tk");
            ASSERT_TRUE(got.has_value());
            EXPECT_EQ(got->value, "ephemeral");
            EXPECT_TRUE(got->hasTtl());
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
            mb.onDigestRequest("n1", digest_req, [&](net::Response r) {
                digest_resp = std::move(r);
            });
            ASSERT_EQ(digest_resp.status, Errc::OK);
            ASSERT_TRUE(digest_resp.value.has_value());

            // Split response into [digest section][entries section].
            size_t digest_len =
                sizeof(uint32_t) + static_cast<size_t>(K_BUCKETS) * sizeof(uint64_t);
            ASSERT_GE(digest_resp.value->size(), digest_len);
            auto digest_b = AntiEntropyManager::decodeDigest(
                std::string_view(digest_resp.value->data(), digest_len),
                K_BUCKETS
            );
            ASSERT_TRUE(digest_b.has_value());
            std::string_view entries_b(
                digest_resp.value->data() + digest_len,
                digest_resp.value->size() - digest_len
            );

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

        TEST(AntiEntropySyncTest, StaleEntryNotCountedAsRepair) {
            Node a;
            Node b;
            // B already has k1 at version 10 (newer than A's version 1).
            ASSERT_TRUE(a.store.putVersioned("k1", makeEntry("old", 1)).has_value());
            ASSERT_TRUE(b.store.putVersioned("k1", makeEntry("new", 10)).has_value());

            AntiEntropyManager mb(b.store, b.ring, "n2", b.clock, b.transport, 8, &b.metrics);
            // A sends its stale entry (version 1) — B should NOT count it as a repair.
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);
            auto blob = ma.collectEntries(allBuckets(8));

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
            auto blob = ma.collectEntries(allBuckets(8));

            size_t applied = mb.applyEntries(blob);
            EXPECT_EQ(applied, 1);
            EXPECT_EQ(b.store.getVersioned("k1")->value, "newer");
        }

        TEST(AntiEntropySyncTest, CollectEntriesCapsAtLimit) {
            Node a;
            AntiEntropyManager ma(a.store, a.ring, "n1", a.clock, a.transport, 8);

            // Insert 1500 entries — all should land in bucket 0 for 8 buckets if
            // we use keys that all hash to the same bucket. Since we can't control
            // the hash, just insert 1500 entries and verify the blob doesn't
            // contain all of them.
            for (int i = 0; i < 1500; i++) {
                auto key = "key" + std::to_string(i);
                ASSERT_TRUE(a.store.putVersioned(key, makeEntry("v", i)).has_value());
            }

            auto blob = ma.collectEntries(allBuckets(8));
            // Decode the entry count from the blob: first 4 bytes = u32 count.
            uint32_t count = 0;
            std::memcpy(&count, blob.data(), sizeof(count));
            // The cap is 1000; some entries may be in different buckets, so the
            // collected count should be <= 1000 (not all 1500).
            EXPECT_LE(count, 1000U);
        }

        // Deferred scripted peer: captures digest requests and answers them on
        // demand, so the round's async callback can run after runRound returned
        // (as with a real transport) instead of inline.
        class DeferredScriptedTransport : public Transport {
        public:

            explicit DeferredScriptedTransport(uint32_t buckets) :
                buckets_(buckets) {}

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
                        .value = AntiEntropyManager::encodeDigest(zeros),
                    };
                    pending_digest_ = std::move(on_done);
                    pending_digest_response_ = std::move(resp);
                } else {
                    on_done(ok(net::Response{.status = Errc::OK, .value = std::nullopt}));
                }
            }

            void onMessage(MessageHandler /*handler*/) override {}

            void releaseDigest() {
                pending_digest_(ok(std::move(pending_digest_response_)));
            }

            uint64_t seen_digest_trace = 0;

        private:

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
            DeferredScriptedTransport transport(K_BUCKETS);
            AntiEntropyManager manager(store, ring, "n1", clock, transport, K_BUCKETS);
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
