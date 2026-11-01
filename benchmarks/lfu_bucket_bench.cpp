#include <ankerl/unordered_dense.h>
#include <benchmark/benchmark.h>
#include <btree/map.h>

// All textual includes precede the import; everything standard comes from
// import std below.
import std;

namespace {
    template <typename Map> auto makeBuckets(size_t count, size_t occupants = 1) -> Map {
        Map buckets;
        for (size_t i = 0; i < count; ++i) {
            auto& values = buckets[(i + 1) * 2];
            for (size_t j = 0; j < occupants; ++j) {
                values.push_back(static_cast<uint32_t>(i * occupants + j));
            }
        }
        return buckets;
    }

    template <typename Map> void bmBucketFindHit(benchmark::State& state) {
        const size_t count = static_cast<size_t>(state.range(0));
        const auto buckets = makeBuckets<Map>(count);
        size_t key = 2;
        // NOLINTNEXTLINE
        for (auto _ : state) {
            auto it = buckets.find(key);
            bool found = it != buckets.end();
            benchmark::DoNotOptimize(found);
            key += 2;
            if (key > count * 2) {
                key = 2;
            }
        }
        state.SetItemsProcessed(state.iterations());
    }

    template <typename Map> void bmBucketFindMiss(benchmark::State& state) {
        const size_t count = static_cast<size_t>(state.range(0));
        const auto buckets = makeBuckets<Map>(count);
        size_t key = count * 2 + 1;
        // NOLINTNEXTLINE
        for (auto _ : state) {
            auto it = buckets.find(key);
            bool found = it != buckets.end();
            benchmark::DoNotOptimize(found);
            key += 2;
            if (key > count * 2 + 1'000'000) {
                key = count * 2 + 1;
            }
        }
        state.SetItemsProcessed(state.iterations());
    }

    // Full pass of promotions into previously absent f+1 buckets; rebuild
    // the starting state outside the timed region.
    template <typename Map> void bmBucketPromotionNew(benchmark::State& state) {
        const size_t count = static_cast<size_t>(state.range(0));
        auto buckets = makeBuckets<Map>(count);
        // NOLINTNEXTLINE
        for (auto _ : state) {
            for (size_t i = 0; i < count; ++i) {
                const size_t key = (i + 1) * 2;
                auto it = buckets.find(key);
                auto values = std::move(it->second);
                buckets.erase(it);
                buckets.emplace(key + 1, std::move(values));
            }
            state.PauseTiming();
            buckets = makeBuckets<Map>(count);
            state.ResumeTiming();
        }
        state.SetItemsProcessed(state.iterations() * count);
    }

    // Full pass promoting entries into existing f+1 buckets. Use disjoint
    // adjacent pairs, with two source entries and one destination entry, so
    // every source remains valid throughout the pass. Rebuild outside timing.
    template <typename Map> void bmBucketPromotionExisting(benchmark::State& state) {
        const size_t count = static_cast<size_t>(state.range(0));
        auto make_state = [&] {
            Map buckets;
            for (size_t pair = 0; pair < count; ++pair) {
                const size_t source = pair * 2 + 1;
                buckets.emplace(
                    source,
                    std::vector<uint32_t>{
                        static_cast<uint32_t>(source),
                        static_cast<uint32_t>(source + 1),
                    }
                );
                buckets.emplace(
                    source + 1,
                    std::vector<uint32_t>{static_cast<uint32_t>(count + pair)}
                );
            }
            return buckets;
        };
        auto buckets = make_state();
        // NOLINTNEXTLINE
        for (auto _ : state) {
            for (size_t pair = 0; pair < count; ++pair) {
                const size_t source = pair * 2 + 1;
                auto src = buckets.find(source);
                auto dst = buckets.find(source + 1);
                const uint32_t value = src->second.back();
                src->second.pop_back();
                dst->second.push_back(value);
            }
            state.PauseTiming();
            buckets = make_state();
            state.ResumeTiming();
        }
        state.SetItemsProcessed(state.iterations() * count);
    }

    template <typename Map> void bmBucketCreateErase(benchmark::State& state) {
        const size_t count = static_cast<size_t>(state.range(0));
        auto buckets = makeBuckets<Map>(count);
        const size_t key = count * 2 + 2;
        // NOLINTNEXTLINE
        for (auto _ : state) {
            auto [it, inserted] = buckets.emplace(key, std::vector<uint32_t>{1});
            benchmark::DoNotOptimize(inserted);
            buckets.erase(it);
            benchmark::ClobberMemory();
        }
        state.SetItemsProcessed(state.iterations());
    }

    template <typename Map> void bmBucketEraseMinimum(benchmark::State& state) {
        const size_t count = static_cast<size_t>(state.range(0));
        auto buckets = makeBuckets<Map>(count);
        size_t next_key = count * 2 + 1;
        // NOLINTNEXTLINE
        for (auto _ : state) {
            auto it = buckets.begin();
            size_t minimum = it->first;
            auto values = std::move(it->second);
            buckets.erase(it);
            buckets.emplace(next_key++, std::move(values));
            benchmark::DoNotOptimize(minimum);
            benchmark::ClobberMemory();
        }
        state.SetItemsProcessed(state.iterations());
    }

    using StdMap = std::map<size_t, std::vector<uint32_t>>;
    using BtreeMap = btree::map<size_t, std::vector<uint32_t>>;
    using DenseMap = ankerl::unordered_dense::map<size_t, std::vector<uint32_t>>;

#define DEFINE_RUNNERS(Prefix, MapType)                                                            \
    void bm##Prefix##FindHit(benchmark::State& s) {                                                \
        bmBucketFindHit<MapType>(s);                                                               \
    }                                                                                              \
    void bm##Prefix##FindMiss(benchmark::State& s) {                                               \
        bmBucketFindMiss<MapType>(s);                                                              \
    }                                                                                              \
    void bm##Prefix##PromotionNew(benchmark::State& s) {                                           \
        bmBucketPromotionNew<MapType>(s);                                                          \
    }                                                                                              \
    void bm##Prefix##PromotionExisting(benchmark::State& s) {                                      \
        bmBucketPromotionExisting<MapType>(s);                                                     \
    }                                                                                              \
    void bm##Prefix##CreateErase(benchmark::State& s) {                                            \
        bmBucketCreateErase<MapType>(s);                                                           \
    }                                                                                              \
    void bm##Prefix##EraseMinimum(benchmark::State& s) {                                           \
        bmBucketEraseMinimum<MapType>(s);                                                          \
    }

    DEFINE_RUNNERS(Std, StdMap);
    DEFINE_RUNNERS(Btree, BtreeMap);
    DEFINE_RUNNERS(Dense, DenseMap);
#undef DEFINE_RUNNERS

    void bmDenseFindMinimumScan(benchmark::State& state);

#define REGISTER_SIX(Fn) BENCHMARK(Fn)->Arg(4)->Arg(16)->Arg(64)->Arg(256)->Arg(1024)->Arg(4096)
#define REGISTER_FIVE(Fn) BENCHMARK(Fn)->Arg(16)->Arg(64)->Arg(256)->Arg(1024)->Arg(4096)

    REGISTER_SIX(bmStdFindHit);
    REGISTER_SIX(bmStdFindMiss);
    REGISTER_SIX(bmStdPromotionNew);
    REGISTER_SIX(bmStdPromotionExisting);
    REGISTER_SIX(bmStdCreateErase);
    REGISTER_FIVE(bmStdEraseMinimum);
    REGISTER_SIX(bmBtreeFindHit);
    REGISTER_SIX(bmBtreeFindMiss);
    REGISTER_SIX(bmBtreePromotionNew);
    REGISTER_SIX(bmBtreePromotionExisting);
    REGISTER_SIX(bmBtreeCreateErase);
    REGISTER_FIVE(bmBtreeEraseMinimum);
    REGISTER_SIX(bmDenseFindHit);
    REGISTER_SIX(bmDenseFindMiss);
    REGISTER_SIX(bmDensePromotionNew);
    REGISTER_SIX(bmDensePromotionExisting);
    REGISTER_SIX(bmDenseCreateErase);
    REGISTER_SIX(bmDenseFindMinimumScan);
#undef REGISTER_SIX
#undef REGISTER_FIVE

    // An unordered map cannot identify the LFU minimum from begin(); scan for
    // the true minimum before erase to show the selection cost.
    void bmDenseFindMinimumScan(benchmark::State& state) {
        const size_t count = static_cast<size_t>(state.range(0));
        auto buckets = makeBuckets<DenseMap>(count);
        size_t next_key = count * 2;
        // NOLINTNEXTLINE
        for (auto _ : state) {
            auto min = buckets.end();
            for (auto it = buckets.begin(); it != buckets.end(); ++it) {
                if (min == buckets.end() || it->first < min->first) {
                    min = it;
                }
            }
            size_t min_key = min->first;
            benchmark::DoNotOptimize(min_key);
            auto values = std::move(min->second);
            buckets.erase(min);
            buckets.emplace(++next_key, std::move(values));
            benchmark::ClobberMemory();
        }
        state.SetItemsProcessed(state.iterations());
    }

} // namespace
