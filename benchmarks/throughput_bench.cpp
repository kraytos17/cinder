#include <benchmark/benchmark.h>
#include <string>

#include "cinder/store/lfu_store.hpp"
#include "cinder/store/lru_store.hpp"

namespace cinder {
namespace {

static void
BM_LruStore_Put(benchmark::State& state) {
    LruStore store(static_cast<size_t>(state.range(0)));
    int i = 0;
    for (auto _ : state) {
        store.put(std::to_string(i++), std::string(256, 'x'));
    }
}

// NOLINTNEXTLINE
BENCHMARK(BM_LruStore_Put)->Range(1 << 16, 1 << 20);

static void
BM_LruStore_Get(benchmark::State& state) {
    LruStore store(static_cast<size_t>(state.range(0)));
    for (int i = 0; i < 1'000; i++) {
        store.put(std::to_string(i), std::string(256, 'x'));
    }
    
    int i = 0;
    for (auto _ : state) {
        store.get(std::to_string(i++ % 1'000));
    }
}

// NOLINTNEXTLINE
BENCHMARK(BM_LruStore_Get)->Range(1 << 16, 1 << 20);

static void
BM_LfuStore_Put(benchmark::State& state) {
    LfuStore store(static_cast<size_t>(state.range(0)));
    int i = 0;
    for (auto _ : state) {
        store.put(std::to_string(i++), std::string(256, 'x'));
    }
}

// NOLINTNEXTLINE
BENCHMARK(BM_LfuStore_Put)->Range(1 << 16, 1 << 20);

static void
BM_LfuStore_Get(benchmark::State& state) {
    LfuStore store(static_cast<size_t>(state.range(0)));
    for (int i = 0; i < 1'000; i++) {
        store.put(std::to_string(i), std::string(256, 'x'));
    }
    
    int i = 0;
    for (auto _ : state) {
        store.get(std::to_string(i++ % 1'000));
    }
}

// NOLINTNEXTLINE
BENCHMARK(BM_LfuStore_Get)->Range(1 << 16, 1 << 20);
} // namespace
} // namespace cinder
