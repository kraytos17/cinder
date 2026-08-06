#include <benchmark/benchmark.h>
#include <string>
#include <unordered_map>

#include "cinder/store/lru_store.hpp"

namespace cinder {
namespace {

// Raw allocation baseline: `malloc`-backed std::string churn.
void
bmRawStringAlloc(benchmark::State& state) {
    for (auto _ : state) {
        std::string s(static_cast<size_t>(state.range(0)), 'x');
        benchmark::DoNotOptimize(s);
    }
}

// NOLINTNEXTLINE
BENCHMARK(bmRawStringAlloc)->Arg(16)->Arg(256)->Arg(1 << 12);

// std::unordered_map insert+erase churn (the store's backing structure shape).
void
bmUnorderedMapChurn(benchmark::State& state) {
    std::unordered_map<std::string, std::string> map;
    int i = 0;
    // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
    for (auto _ : state) {
        auto key = std::to_string(i++);
        map.emplace(key, std::string(static_cast<size_t>(state.range(0)), 'x'));
        map.erase(key);
    }
}

// NOLINTNEXTLINE
BENCHMARK(bmUnorderedMapChurn)->Arg(16)->Arg(256);

// LruStore put+get churn: what the cache actually does per request.
void
bmLruStoreChurn(benchmark::State& state) {
    LruStore store(static_cast<size_t>(state.range(0)));
    int i = 0;
    // NOLINTNEXTLINE
    for (auto _ : state) {
        auto key = "key" + std::to_string(i++);
        // NOLINTNEXTLINE
        (void)store.put(key, std::string(256, 'x'));
        (void)store.get(key);
    }
}

// NOLINTNEXTLINE
BENCHMARK(bmLruStoreChurn)->Range(1 << 16, 1 << 20);
} // namespace
} // namespace cinder
