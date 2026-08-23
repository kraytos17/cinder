PRESET   ?= debug
BUILD_DIR = build/$(PRESET)
BIN_DIR   = $(BUILD_DIR)/bin
TESTS_DIR = $(BUILD_DIR)/tests
JOBS     ?= $(shell nproc 2>/dev/null || echo 4)
ARGS     ?=

SERVER = cinderd
CLI    = cinder-cli

# Auto-detect from CMakePresets.json; fallback keeps a manual DRY list.
PRESETS := $(shell jq -r '.configurePresets[].name' CMakePresets.json 2>/dev/null | tr '\n' ' ')
ifeq ($(strip $(PRESETS)),)
PRESETS := debug debug-tls debug-clang release release-clang sanitized asan asan-clang msan tsan ubsan ci ci-gcc fast
endif

TEST_BINS      := cinder_unit_tests cinder_sim_tests cinder_cli_tests cinder_integration_tests
SANITIZER_BINS := cinder_unit_tests cinder_sim_tests

ASAN_ENV  = ASAN_OPTIONS="detect_leaks=1:abort_on_error=1:check_initialization_order=1:strict_init_order=1:suppressions=$(PWD)/suppressions/asan.supp" LSAN_OPTIONS="suppressions=$(PWD)/suppressions/lsan.supp"
TSAN_ENV  = TSAN_OPTIONS="halt_on_error=1:second_deadlock_stack=1:history_size=7:suppressions=$(PWD)/suppressions/tsan.supp"
UBSAN_ENV = UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1:suppressions=$(PWD)/suppressions/ubsan.supp"

# Every name in PRESETS becomes: make <preset>  →  PRESET=<preset> && build
define MAKE_PRESET
.PHONY: $(1)
$(1): build
$(1): PRESET := $(1)
endef
$(foreach p,$(PRESETS),$(eval $(call MAKE_PRESET,$(p))))

.PHONY: all configure build
all: build

configure:
	@echo "==> Configuring $(PRESET)..."
	cmake --preset $(PRESET)

define ensure-configured
	@if [ ! -f "$(BUILD_DIR)/CMakeCache.txt" ]; then \
		echo "==> Configuring $(PRESET)..."; \
		cmake --preset $(PRESET); \
	fi
endef

build:
	$(ensure-configured)
	@echo "==> Building $(PRESET)..."
	CCACHE_MAXSIZE=25G cmake --build --preset $(PRESET) -j$(JOBS)

.PHONY: run run-cli
run: build
	"$(BIN_DIR)/$(SERVER)" $(ARGS)

run-cli: build
	"$(BIN_DIR)/$(CLI)" $(ARGS)

.PHONY: kill-stale
kill-stale:
	@if pkill -x cinderd 2>/dev/null; then \
		echo "==> killed stale cinderd process(es)"; \
	else \
		echo "==> no stale cinderd processes found"; \
	fi

.PHONY: test test-unit test-sim test-cli test-integration test-all
test: build
	ctest --preset $(PRESET) --output-on-failure -j$(JOBS) -- $(ARGS)

test-unit: build
	"$(TESTS_DIR)/cinder_unit_tests" --gtest_also_run_disabled_tests $(ARGS)

test-sim: build
	"$(TESTS_DIR)/cinder_sim_tests" --gtest_also_run_disabled_tests $(ARGS)

test-cli: build
	"$(TESTS_DIR)/cinder_cli_tests" --gtest_also_run_disabled_tests $(ARGS)

test-integration: build kill-stale
	"$(TESTS_DIR)/cinder_integration_tests" --gtest_also_run_disabled_tests $(ARGS)

test-all: build kill-stale
	@for bin in $(TEST_BINS); do \
		echo ""; \
		echo "==> $$bin..."; \
		echo "────────────────────────────────────────"; \
		"$(TESTS_DIR)/$$bin" --gtest_also_run_disabled_tests $(ARGS) \
			|| { echo ""; echo "FAILED: $$bin ($(PRESET))"; exit 1; }; \
		echo "PASSED: $$bin"; \
	done
	@echo ""
	@echo "════════════════════════════════════════"
	@echo "All test suites passed ($(PRESET))."

.PHONY: asan-test tsan-test ubsan-test asan-clang-test
asan-test: asan kill-stale
	$(ASAN_ENV) "$(TESTS_DIR)/cinder_unit_tests" --gtest_also_run_disabled_tests $(ARGS)
	$(ASAN_ENV) "$(TESTS_DIR)/cinder_sim_tests" --gtest_also_run_disabled_tests $(ARGS)

tsan-test: tsan kill-stale
	$(TSAN_ENV) "$(TESTS_DIR)/cinder_unit_tests" --gtest_also_run_disabled_tests $(ARGS)
	$(TSAN_ENV) "$(TESTS_DIR)/cinder_sim_tests" --gtest_also_run_disabled_tests $(ARGS)

ubsan-test: ubsan kill-stale
	$(UBSAN_ENV) "$(TESTS_DIR)/cinder_unit_tests" --gtest_also_run_disabled_tests $(ARGS)
	$(UBSAN_ENV) "$(TESTS_DIR)/cinder_sim_tests" --gtest_also_run_disabled_tests $(ARGS)

asan-clang-test: asan-clang kill-stale
	$(ASAN_ENV) "$(TESTS_DIR)/cinder_unit_tests" --gtest_also_run_disabled_tests $(ARGS)
	$(ASAN_ENV) "$(TESTS_DIR)/cinder_sim_tests" --gtest_also_run_disabled_tests $(ARGS)

.PHONY: bench
bench: build
	"$(BIN_DIR)/cinder_throughput_bench" $(ARGS)

.PHONY: format check-format
format:
	$(ensure-configured)
	cmake --build "$(BUILD_DIR)" --target format -j$(JOBS)

check-format:
	$(ensure-configured)
	cmake --build "$(BUILD_DIR)" --target check-format -j$(JOBS)

.PHONY: install clean clean-all
install: build
	cmake --install "$(BUILD_DIR)"

clean:
	rm -rf "$(BUILD_DIR)"

clean-all:
	rm -rf build/ .cache/

.PHONY: info help
info:
	@echo 'PRESET=$(PRESET)'
	@echo 'BUILD_DIR=$(BUILD_DIR)'
	@echo 'BIN_DIR=$(BIN_DIR)'
	@echo 'TESTS_DIR=$(TESTS_DIR)'
	@echo 'JOBS=$(JOBS)'
	@echo 'PRESETS=$(PRESETS)'

help:
	@echo 'Cinder — distributed cache'
	@echo ''
	@echo 'Usage: make <target> [PRESET=...] [ARGS=...] [JOBS=N]'
	@echo ''
	@echo 'Build:'
	@echo '  make build                  Build current PRESET (default: debug)'
	@echo '  make <preset>               Build that preset (auto-detected from CMakePresets.json)'
	@echo '  make configure              Configure current PRESET only'
	@echo ''
	@echo 'Run:'
	@echo '  make run ARGS="..."         Start cinderd with args'
	@echo '  make run-cli ARGS="..."     Run cinder-cli with args'
	@echo ''
	@echo 'Test (current PRESET):'
	@echo '  make test                   All suites via ctest'
	@echo '  make test-unit              Unit tests only'
	@echo '  make test-sim               Sim tests only'
	@echo '  make test-cli               CLI tests only'
	@echo '  make test-integration       Integration tests (kills stale daemons first)'
	@echo '  make test-all               All four binaries, stops on failure, kills stale daemons'
	@echo ''
	@echo 'Sanitizer tests:'
	@echo '  make asan-test              ASan unit+sim'
	@echo '  make tsan-test              TSan unit+sim'
	@echo '  make ubsan-test             UBSan unit+sim'
	@echo '  make asan-clang-test        Clang ASan unit+sim'
	@echo ''
	@echo 'Other:'
	@echo '  make bench                  Run throughput benchmark'
	@echo '  make format                 Apply clang-format'
	@echo '  make check-format           Verify formatting'
	@echo '  make install [DESTDIR=...]  Install'
	@echo '  make info                   Show resolved build environment'
	@echo '  make kill-stale             Kill orphaned cinderd daemons'
	@echo '  make clean                  Remove build/<PRESET>'
	@echo '  make clean-all              Remove build/ and .cache/'
	@echo ''
	@echo 'Available presets: $(PRESETS)'
