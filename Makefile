PRESET   ?= debug
BUILD_DIR = build/$(PRESET)
BIN_DIR   = $(BUILD_DIR)/bin
TESTS_DIR = $(BUILD_DIR)/tests
JOBS     ?= $(shell nproc 2>/dev/null || echo 4)
ARGS     ?=

SERVER = cinderd
CLI    = cinder-cli

.PHONY: all
all: build

.PHONY: configure
configure:
	@echo "==> Configuring $(PRESET)..."
	cmake --preset $(PRESET)

.PHONY: build debug release sanitized ci fast debug-clang ci-clang ci-gcc \
        asan tsan ubsan asan-clang msan release-clang
build:
	@if [ ! -f "$(BUILD_DIR)/CMakeCache.txt" ]; then \
		echo "==> Configuring $(PRESET)..."; \
		cmake --preset $(PRESET); \
	fi
	@echo "==> Building $(PRESET)..."
	CCACHE_MAXSIZE=25G cmake --build --preset $(PRESET) -j$(JOBS)

debug:        PRESET=debug
release:      PRESET=release
sanitized:    PRESET=sanitized
ci:           PRESET=ci
fast:         PRESET=fast
debug-clang:  PRESET=debug-clang
ci-clang:     PRESET=ci
ci-gcc:       PRESET=ci-gcc
asan:         PRESET=asan
tsan:         PRESET=tsan
ubsan:        PRESET=ubsan
asan-clang:   PRESET=asan-clang
msan:         PRESET=msan
release-clang: PRESET=release-clang

debug release sanitized ci fast debug-clang ci-clang ci-gcc \
asan tsan ubsan asan-clang msan release-clang: build

.PHONY: run run-cli
run: build
	"$(BIN_DIR)/$(SERVER)" $(ARGS)

run-cli: build
	"$(BIN_DIR)/$(CLI)" $(ARGS)

.PHONY: test test-unit test-integration test-sim \
        asan-test tsan-test ubsan-test asan-clang-test
test: build
	ctest --preset $(PRESET) --output-on-failure -j$(JOBS) -- $(ARGS)

test-unit: build
	"$(TESTS_DIR)/cinder_unit_tests"

test-integration: build
	"$(TESTS_DIR)/cinder_integration_tests"

test-sim: build
	"$(TESTS_DIR)/cinder_sim_tests"

asan-test: asan
	ASAN_OPTIONS="detect_leaks=1:abort_on_error=1:check_initialization_order=1:strict_init_order=1:suppressions=$(PWD)/suppressions/asan.supp" \
	LSAN_OPTIONS="suppressions=$(PWD)/suppressions/lsan.supp" \
	"$(TESTS_DIR)/cinder_unit_tests"

tsan-test: tsan
	TSAN_OPTIONS="halt_on_error=1:second_deadlock_stack=1:history_size=7:suppressions=$(PWD)/suppressions/tsan.supp" \
	"$(TESTS_DIR)/cinder_unit_tests"

ubsan-test: ubsan
	UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1:suppressions=$(PWD)/suppressions/ubsan.supp" \
	"$(TESTS_DIR)/cinder_unit_tests"

asan-clang-test: asan-clang
	ASAN_OPTIONS="detect_leaks=1:abort_on_error=1:check_initialization_order=1:strict_init_order=1:suppressions=$(PWD)/suppressions/asan.supp" \
	LSAN_OPTIONS="suppressions=$(PWD)/suppressions/lsan.supp" \
	"$(TESTS_DIR)/cinder_unit_tests"

.PHONY: bench
bench: build
	"$(BIN_DIR)/cinder_throughput_bench" $(ARGS)

.PHONY: format check-format
format:
	@if [ ! -f "$(BUILD_DIR)/CMakeCache.txt" ]; then \
		echo "==> Configuring $(PRESET)..."; \
		cmake --preset $(PRESET); \
	fi
	cmake --build "$(BUILD_DIR)" --target format -j$(JOBS)

check-format:
	@if [ ! -f "$(BUILD_DIR)/CMakeCache.txt" ]; then \
		echo "==> Configuring $(PRESET)..."; \
		cmake --preset $(PRESET); \
	fi
	cmake --build "$(BUILD_DIR)" --target check-format -j$(JOBS)

.PHONY: clean clean-all
clean:
	rm -rf "$(BUILD_DIR)"

clean-all:
	rm -rf build/ .cache/

.PHONY: install
install: build
	cmake --install "$(BUILD_DIR)"

.PHONY: help
help:
	@echo 'Usage: make <target> [PRESET=...] [ARGS=...]'
	@echo ''
	@echo 'Development (GCC):'
	@echo '  make / make debug / release / fast / sanitized / asan / tsan / ubsan'
	@echo ''
	@echo 'Development (Clang):'
	@echo '  make debug-clang / release-clang / asan-clang / msan'
	@echo '  asan-clang: ASan+UBSan + use-after-scope/return + ignorelist'
	@echo ''
	@echo 'CI:'
	@echo '  make ci / ci-gcc'
	@echo ''
	@echo 'Run:        make run ARGS="--port 7000" / run-cli ARGS="get foo"'
	@echo 'Test:       make test / test-unit / asan-test / tsan-test / ubsan-test / asan-clang-test'
	@echo 'Bench:      make bench ARGS="--benchmark_format=json"'
	@echo 'Format:     make format / check-format'
	@echo 'Install:    make install [DESTDIR=/tmp/staging]'
	@echo 'Clean:      make clean / clean-all'
