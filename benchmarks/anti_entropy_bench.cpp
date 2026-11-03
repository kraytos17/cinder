#include <benchmark/benchmark.h>
#include <cassert>
#include <memory>
#include <string>

#include "cinder/cluster/transport.hpp"
#include "cinder/common/tracing.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/node/anti_entropy.hpp"
#include "cinder/store/lru_store.hpp"

// Anti-entropy digest costs (P0/P1): full-scan ground truth vs the
// incremental accumulator path. Converged rounds must serve the digest
// without touching the store.

namespace cinder {
    namespace {

        class NullTransport : public Transport {
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

        struct BenchStore {
            RealClock clock;
            LruStore store{1U << 30U};
            ConsistentHashRing ring;
            NullTransport transport;
            std::unique_ptr<AntiEntropyManager> manager;

            explicit BenchStore(uint32_t buckets, size_t n_keys = 100'000) {
                ring.addNode("n1");
                for (size_t i = 0; i < n_keys; ++i) {
                    VersionedEntry e;
                    e.value = std::string(96, 'v');
                    e.setVersion(static_cast<Version>(i + 1));
                    e.writer_node_hash = 7;
                    // Setup path (not timed): every put must succeed. NOLINT
                    // covers NDEBUG builds, where assert compiles out.
                    // NOLINTNEXTLINE(bugprone-unused-local-non-trivial-variable)
                    auto inserted =
                        store.putVersioned("bench-key-" + std::to_string(i), std::move(e));
                    assert(inserted.has_value());
                }
                manager = std::make_unique<AntiEntropyManager>(
                    store,
                    ring,
                    "n1",
                    clock,
                    transport,
                    buckets
                );
            }
        };

        void bmDigestFullScan(benchmark::State& state) {
            BenchStore bench(256);
            // NOLINTNEXTLINE
            for (auto _ : state) {
                auto digest = bench.manager->computeDigest();
                benchmark::DoNotOptimize(digest.data());
            }
            state.SetItemsProcessed(state.iterations());
        }

        BENCHMARK(bmDigestFullScan);

        void bmDigestIncremental(benchmark::State& state) {
            BenchStore bench(256);
            // Warm up: first call performs the full rehash and blesses the
            // accumulators; the loop measures the steady-state copy path.
            (void)bench.manager->digestForRound();
            // NOLINTNEXTLINE
            for (auto _ : state) {
                auto digest = bench.manager->digestForRound();
                benchmark::DoNotOptimize(digest.data());
            }
            state.SetItemsProcessed(state.iterations());
        }

        BENCHMARK(bmDigestIncremental);

        void bmMetadataCollect(benchmark::State& state) {
            BenchStore bench(256);
            std::vector<uint32_t> all;
            for (uint32_t i = 0; i < 256; ++i) {
                all.push_back(i);
            }
            // NOLINTNEXTLINE
            for (auto _ : state) {
                auto blob = bench.manager->collectMetadata(all, 1U << 20U);
                benchmark::DoNotOptimize(blob.data());
            }
            state.SetItemsProcessed(state.iterations());
        }

        BENCHMARK(bmMetadataCollect);

        // P2 wire sizes: converged rounds send only group hashes (16×8 B);
        // a single divergent bucket costs one refinement round-trip carrying
        // 16 bucket hashes plus metadata for exactly the differing bucket.
        void bmGroupDigestRoundTripBytes(benchmark::State& state) {
            BenchStore bench(256);
            auto buckets = bench.manager->computeDigest();
            auto groups = AntiEntropyManager::groupDigests(buckets);
            AntiEntropyManager::DigestResponse converged;
            converged.group_hashes = groups;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                auto blob = AntiEntropyManager::encodeDigestResponse(converged);
                benchmark::DoNotOptimize(blob.data());
                state.counters["converged_bytes"] = static_cast<double>(blob.size());
            }
            state.SetItemsProcessed(state.iterations());
        }

        BENCHMARK(bmGroupDigestRoundTripBytes);

        void bmSingleBucketDivergenceBytes(benchmark::State& state) {
            BenchStore bench(256);
            auto buckets = bench.manager->computeDigest();
            auto groups = AntiEntropyManager::groupDigests(buckets);
            AntiEntropyManager::BucketRefinement refinement;
            for (uint32_t b = 0; b < AntiEntropyManager::K_BUCKETS_PER_GROUP; ++b) {
                refinement.bucket_hashes.emplace_back(b, buckets[b] ^ 0x1ULL);
            }
            std::vector<uint32_t> group_buckets;
            for (uint32_t b = 0; b < AntiEntropyManager::K_BUCKETS_PER_GROUP; ++b) {
                group_buckets.push_back(b);
            }
            // NOLINTNEXTLINE
            for (auto _ : state) {
                // Phase 1: group digests both ways (request omits the key
                // list here; the refinement carries the bucket hashes).
                size_t bytes = AntiEntropyManager::encodeGroupDigest(groups).size()
                    + AntiEntropyManager::encodeBucketRefinement(refinement).size();
                // Phase 2: the responder collects metadata for exactly the
                // buckets whose hashes differ (here: all 16, since every
                // request hash was flipped).
                auto meta = bench.manager->collectMetadata(group_buckets, 1U << 20U);
                auto rows = AntiEntropyManager::decodeMetaRows(meta);
                bytes += meta.size();
                benchmark::DoNotOptimize(bytes);
                benchmark::DoNotOptimize(rows.has_value());
                state.counters["divergent_bytes"] = static_cast<double>(bytes);
            }
            state.SetItemsProcessed(state.iterations());
        }

        BENCHMARK(bmSingleBucketDivergenceBytes);

    } // namespace
} // namespace cinder
