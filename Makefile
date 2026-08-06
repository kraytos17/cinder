PRESET   ?= debug
BUILD_DIR = build/$(PRESET)
BIN_DIR   = $(BUILD_DIR)/bin
TESTS_DIR = $(BUILD_DIR)/tests
JOBS     ?= $(shell nproc 2>/dev/null || echo 4)
ARGS     ?=

SERVER = cinderd
CLI    = cinder-cli

# GCC / Clang / CI presets defined in CMakePresets.json.
GCC_PRESETS = debug release fast sanitized asan tsan ubsan
CLANG_PRESETS = debug-clang release-clang asan-clang msan
CI_PRESETS = ci ci-gcc

.PHONY: all
all: build

.PHONY: configure
configure:
	@echo "==> Configuring $(PRESET)..."
	cmake --preset $(PRESET)

# Ensure the preset is configured before building/formatting (idempotent).
define ensure-configured
	@if [ ! -f "$(BUILD_DIR)/CMakeCache.txt" ]; then \
		echo "==> Configuring $(PRESET)..."; \
		cmake --preset $(PRESET); \
	fi
endef

.PHONY: build
build:
	$(ensure-configured)
	@echo "==> Building $(PRESET)..."
	CCACHE_MAXSIZE=25G cmake --build --preset $(PRESET) -j$(JOBS)

# One target per preset: `make asan` == `make asan` → PRESET=asan → build.
$(GCC_PRESETS) $(CLANG_PRESETS) $(CI_PRESETS): build

debug:          PRESET=debug
release:        PRESET=release
fast:           PRESET=fast
sanitized:      PRESET=sanitized
asan:           PRESET=asan
tsan:           PRESET=tsan
ubsan:          PRESET=ubsan
debug-clang:    PRESET=debug-clang
release-clang:  PRESET=release-clang
asan-clang:     PRESET=asan-clang
msan:           PRESET=msan
ci:             PRESET=ci
ci-gcc:         PRESET=ci-gcc

.PHONY: run run-cli
run: build
	"$(BIN_DIR)/$(SERVER)" $(ARGS)

run-cli: build
	"$(BIN_DIR)/$(CLI)" $(ARGS)

.PHONY: test test-unit test-sim test-cli test-integration test-all
test: build
	ctest --preset $(PRESET) --output-on-failure -j$(JOBS) -- $(ARGS)

test-unit: build
	"$(TESTS_DIR)/cinder_unit_tests"

test-sim: build
	"$(TESTS_DIR)/cinder_sim_tests"

test-cli: build
	"$(TESTS_DIR)/cinder_cli_tests"

test-integration: build
	"$(TESTS_DIR)/cinder_integration_tests"

# Run every test binary against the current preset.
test-all: build
	"$(TESTS_DIR)/cinder_unit_tests"
	"$(TESTS_DIR)/cinder_sim_tests"
	"$(TESTS_DIR)/cinder_cli_tests"
	"$(TESTS_DIR)/cinder_integration_tests"

# Sanitizer targets run the in-process suites (unit + sim) — no daemon forking,
# so failures are cleanly attributed under ASan/TSan/UBSan.
.PHONY: asan-test tsan-test ubsan-test asan-clang-test
asan-test: asan
	ASAN_OPTIONS="detect_leaks=1:abort_on_error=1:check_initialization_order=1:strict_init_order=1:suppressions=$(PWD)/suppressions/asan.supp" \
	LSAN_OPTIONS="suppressions=$(PWD)/suppressions/lsan.supp" \
	"$(TESTS_DIR)/cinder_unit_tests"
	ASAN_OPTIONS="detect_leaks=1:abort_on_error=1:check_initialization_order=1:strict_init_order=1:suppressions=$(PWD)/suppressions/asan.supp" \
	LSAN_OPTIONS="suppressions=$(PWD)/suppressions/lsan.supp" \
	"$(TESTS_DIR)/cinder_sim_tests"

tsan-test: tsan
	TSAN_OPTIONS="halt_on_error=1:second_deadlock_stack=1:history_size=7:suppressions=$(PWD)/suppressions/tsan.supp" \
	"$(TESTS_DIR)/cinder_unit_tests"
	TSAN_OPTIONS="halt_on_error=1:second_deadlock_stack=1:history_size=7:suppressions=$(PWD)/suppressions/tsan.supp" \
	"$(TESTS_DIR)/cinder_sim_tests"

ubsan-test: ubsan
	UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1:suppressions=$(PWD)/suppressions/ubsan.supp" \
	"$(TESTS_DIR)/cinder_unit_tests"
	UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1:suppressions=$(PWD)/suppressions/ubsan.supp" \
	"$(TESTS_DIR)/cinder_sim_tests"

asan-clang-test: asan-clang
	ASAN_OPTIONS="detect_leaks=1:abort_on_error=1:check_initialization_order=1:strict_init_order=1:suppressions=$(PWD)/suppressions/asan.supp" \
	LSAN_OPTIONS="suppressions=$(PWD)/suppressions/lsan.supp" \
	"$(TESTS_DIR)/cinder_unit_tests"
	ASAN_OPTIONS="detect_leaks=1:abort_on_error=1:check_initialization_order=1:strict_init_order=1:suppressions=$(PWD)/suppressions/asan.supp" \
	LSAN_OPTIONS="suppressions=$(PWD)/suppressions/lsan.supp" \
	"$(TESTS_DIR)/cinder_sim_tests"

.PHONY: bench
bench: build
	"$(BIN_DIR)/cinder_throughput_bench" $(ARGS)

# Kill any leftover cinderd daemons (e.g. orphans from an interrupted test run
# that are still holding test ports). `-x` matches the exact process name, so
# this can never kill the invoking shell.
.PHONY: kill-stale
kill-stale:
	@if pkill -x cinderd 2>/dev/null; then \
		echo "==> killed stale cinderd process(es)"; \
	else \
		echo "==> no stale cinderd processes found"; \
	fi

.PHONY: format check-format
format:
	$(ensure-configured)
	cmake --build "$(BUILD_DIR)" --target format -j$(JOBS)

check-format:
	$(ensure-configured)
	cmake --build "$(BUILD_DIR)" --target check-format -j$(JOBS)

.PHONY: install
install: build
	cmake --install "$(BUILD_DIR)"

.PHONY: clean clean-all
clean:
	rm -rf "$(BUILD_DIR)"

clean-all:
	rm -rf build/ .cache/

# Show the resolved preset/build environment for the current invocation.
.PHONY: info
info:
	@echo 'PRESET=$(PRESET)'
	@echo 'BUILD_DIR=$(BUILD_DIR)'
	@echo 'BIN_DIR=$(BIN_DIR)'
	@echo 'TESTS_DIR=$(TESTS_DIR)'
	@echo 'JOBS=$(JOBS)'

.PHONY: help
help:
	@echo 'Cinder — distributed cache. Usage: make <target> [PRESET=...] [ARGS=...] [JOBS=N]'
	@echo ''
	@echo 'Build (see CMakePresets.json):'
	@echo '  make / build                 Build current PRESET (default: debug)'
	@echo '  make <preset>                Build that preset, e.g.:'
	@echo '    GCC:        debug release fast sanitized asan tsan ubsan'
	@echo '    Clang:      debug-clang release-clang asan-clang msan'
	@echo '    CI:         ci (Clang+tidy)  ci-gcc (GCC+ASan+UBSan)'
	@echo '  make configure               Configure current PRESET only'
	@echo ''
	@echo 'Run:'
	@echo '  make run ARGS="--port 7000"           Start a cinderd server'
	@echo '  make run-cli ARGS="get foo"           Query via cinder-cli'
	@echo ''
	@echo 'Test (current PRESET):'
	@echo '  make test                            All suites via ctest'
	@echo '  make test-unit / test-sim / test-cli / test-integration'
	@echo '  make test-all                        Run all four binaries in sequence'
	@echo '  make asan-test / tsan-test / ubsan-test / asan-clang-test'
	@echo '                                       In-process suites under the sanitizer'
	@echo ''
	@echo 'Bench:'
	@echo '  make bench ARGS="--benchmark_format=json"'
	@echo ''
	@echo 'Format:'
	@echo '  make format        Apply clang-format'
	@echo '  make check-format  Verify formatting (used by CI)'
	@echo ''
	@echo 'Misc:'
	@echo '  make install [DESTDIR=/tmp/staging]'
	@echo '  make info          Show resolved PRESET/build paths'
	@echo '  make kill-stale    Kill leftover cinderd daemons holding test ports'
	@echo '  make clean         Remove build/<PRESET>'
	@echo '  make clean-all     Remove build/ and .cache/ (re-fetch dependencies)'
	@echo '  make help          Show this help'
