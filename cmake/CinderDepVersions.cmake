# Single source of truth for pinned third-party dependencies.
#
# Consumed by:
#   * CMakeLists.txt        (FetchContent_Declare uses these REPO/TAG values)
#   * cmake/CinderDeps.cmake / cmake/FetchDeps.cmake (shared source cache)
#
# KEY is the lower-case FetchContent name. To bump a dependency, change its TAG
# here only: the shared cache directory is named "<key>-<tag>", so a new tag
# gets a fresh checkout and an old checkout can never be silently reused.

set(CINDER_DEP_KEYS asio xxhash spdlog cli11 yaml-cpp mimalloc googletest benchmark grpc)

set(CINDER_DEP_asio_REPO       https://github.com/chriskohlhoff/asio.git)
set(CINDER_DEP_asio_TAG        asio-1-38-2)

set(CINDER_DEP_xxhash_REPO     https://github.com/Cyan4973/xxHash.git)
set(CINDER_DEP_xxhash_TAG      v0.8.3)

# Pinned v2.x (v2.0.0 unreleased).
set(CINDER_DEP_spdlog_REPO     https://github.com/gabime/spdlog.git)
set(CINDER_DEP_spdlog_TAG      d24088deaa441a79267df8ae3dbc567fbe2a5e03)

set(CINDER_DEP_cli11_REPO      https://github.com/CLIUtils/CLI11.git)
set(CINDER_DEP_cli11_TAG       v2.7.2)

set(CINDER_DEP_yaml-cpp_REPO   https://github.com/jbeder/yaml-cpp.git)
set(CINDER_DEP_yaml-cpp_TAG    yaml-cpp-0.9.0)

set(CINDER_DEP_mimalloc_REPO   https://github.com/microsoft/mimalloc.git)
set(CINDER_DEP_mimalloc_TAG    v3.5.0)

set(CINDER_DEP_googletest_REPO https://github.com/google/googletest.git)
set(CINDER_DEP_googletest_TAG  v1.18.0)

set(CINDER_DEP_benchmark_REPO  https://github.com/google/benchmark.git)
set(CINDER_DEP_benchmark_TAG   v1.9.5)

# gRPC needs its submodules (abseil, protobuf, re2, c-ares, ...).
set(CINDER_DEP_grpc_REPO       https://github.com/grpc/grpc.git)
set(CINDER_DEP_grpc_TAG        v1.83.1)
set(CINDER_DEP_grpc_SUBMODULES TRUE)
