#include <benchmark/benchmark.h>
#include <string>

#include "cinder/store/lfu_store.hpp"
#include "cinder/store/lru_store.hpp"

namespace cinder {
namespace {

void
bmLruStorePut(benchmark::State& state) {
    LruStore store(static_cast<size_t>(state.range(0)));
    int i = 0;
    // NOLINTNEXTLINE
    for (auto _ : state) {
        // NOLINTNEXTLINE
        (void)store.put(std::to_string(i++), std::string(256, 'x'));
    }
}

// NOLINTNEXTLINE
BENCHMARK(bmLruStorePut)->Range(1 << 16, 1 << 20);

void
bmLruStoreGet(benchmark::State& state) {
    LruStore store(static_cast<size_t>(state.range(0)));
    for (int i = 0; i < 1'000; i++) {
        // NOLINTNEXTLINE
        (void)store.put(std::to_string(i), std::string(256, 'x'));
    }

    int i = 0;
    // NOLINTNEXTLINE
    for (auto _ : state) {
        store.get(std::to_string(i++ % 1'000));
    }
}

// NOLINTNEXTLINE
BENCHMARK(bmLruStoreGet)->Range(1 << 16, 1 << 20);

void
bmLfuStorePut(benchmark::State& state) {
    LfuStore store(static_cast<size_t>(state.range(0)));
    int i = 0;
    // NOLINTNEXTLINE
    for (auto _ : state) {
        // NOLINTNEXTLINE
        (void)store.put(std::to_string(i++), std::string(256, 'x'));
    }
}

// NOLINTNEXTLINE
BENCHMARK(bmLfuStorePut)->Range(1 << 16, 1 << 20);

void
bmLfuStoreGet(benchmark::State& state) {
    LfuStore store(static_cast<size_t>(state.range(0)));
    for (int i = 0; i < 1'000; i++) {
        // NOLINTNEXTLINE
        (void)store.put(std::to_string(i), std::string(256, 'x'));
    }

    int i = 0;
    // NOLINTNEXTLINE
    for (auto _ : state) {
        store.get(std::to_string(i++ % 1'000));
    }
}

// NOLINTNEXTLINE
BENCHMARK(bmLfuStoreGet)->Range(1 << 16, 1 << 20);
} // namespace
} // namespace cinder
