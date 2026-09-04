PRESET   ?= debug
BUILD_DIR = build/$(PRESET)
BIN_DIR   = $(BUILD_DIR)/bin
TESTS_DIR = $(BUILD_DIR)/tests
JOBS     ?= $(shell nproc 2>/dev/null || echo 4)
ARGS     ?=

SERVER = cinderd
CLI    = cinder-cli

PRESETS := $(shell jq -r '.configurePresets[].name' CMakePresets.json 2>/dev/null | tr '\n' ' ')
ifeq ($(strip $(PRESETS)),)

PRESETS := debug debug-tls debug-clang release release-clang sanitized asan asan-clang msan tsan ubsan ci ci-gcc fast fuzz
endif

TEST_BINS      := cinder_unit_tests cinder_sim_tests cinder_cli_tests cinder_integration_tests
SANITIZER_BINS := cinder_unit_tests cinder_sim_tests

ASAN_ENV  = ASAN_OPTIONS="detect_leaks=1:abort_on_error=1:check_initialization_order=1:strict_init_order=1:suppressions=$(PWD)/suppressions/asan.supp" LSAN_OPTIONS="suppressions=$(PWD)/suppressions/lsan.supp"
TSAN_ENV  = TSAN_OPTIONS="halt_on_error=1:second_deadlock_stack=1:history_size=7:suppressions=$(PWD)/suppressions/tsan.supp"
UBSAN_ENV = UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1:suppressions=$(PWD)/suppressions/ubsan.supp"

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

# ---------------------------------------------------------------------------
# Test binaries — one line per binary instead of one full recipe per binary.
# TEST_TARGETS: "make-target:binary-name:extra-prereqs" triples.
# ---------------------------------------------------------------------------
TEST_TARGETS := \
	unit:cinder_unit_tests: \
	sim:cinder_sim_tests: \
	cli:cinder_cli_tests: \
	integration:cinder_integration_tests:kill-stale

define RUN_TEST_BIN
.PHONY: test-$(1)
test-$(1): build $(3)
	"$$(TESTS_DIR)/$(2)" --gtest_also_run_disabled_tests $$(ARGS)
endef
$(foreach t,$(TEST_TARGETS),$(eval $(call RUN_TEST_BIN,$(word 1,$(subst :, ,$(t))),$(word 2,$(subst :, ,$(t))),$(word 3,$(subst :, ,$(t))))))

.PHONY: test test-all
test: build
	ctest --preset $(PRESET) --output-on-failure -j$(JOBS) -- $(ARGS)

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

# ---------------------------------------------------------------------------
# Sanitizer tests — one template instead of four near-identical targets.
# SAN_TARGETS: "make-target:preset:ENV_VAR_NAME" triples.
# ---------------------------------------------------------------------------
SAN_TARGETS := \
	asan:asan:ASAN_ENV \
	tsan:tsan:TSAN_ENV \
	ubsan:ubsan:UBSAN_ENV \
	asan-clang:asan-clang:ASAN_ENV

define SAN_TEST
.PHONY: $(1)-test
$(1)-test: PRESET := $(2)
$(1)-test: $(2) kill-stale
	@$(foreach bin,$(SANITIZER_BINS),echo ""; echo "==> $(bin) ($(1))..."; echo "────────────────────────────────────────"; $($(3)) "$$(TESTS_DIR)/$(bin)" --gtest_also_run_disabled_tests $$(ARGS) || { echo "FAILED: $(bin) ($(1))"; exit 1; }; echo "PASSED: $(bin)";)
endef
$(foreach s,$(SAN_TARGETS),$(eval $(call SAN_TEST,$(word 1,$(subst :, ,$(s))),$(word 2,$(subst :, ,$(s))),$(word 3,$(subst :, ,$(s))))))

.PHONY: fuzz-test
# FUZZ_TARGETS: fuzz binary base names. FUZZ_DICT_<name> supplies an optional
# -dict= flag. FUZZ_MAXLEN_<name> overrides -max_len. FUZZ_TIMEOUT_<name>
# overrides -timeout (seconds per input).
FUZZ_TARGETS := protocol_decode gossip_parse store_put snapshot wal anti_entropy http_parse
FUZZ_DICT_protocol_decode := -dict=tests/fuzz/corpus/protocol_decode/protocol.dict
FUZZ_MAXLEN_wal := -max_len=4096 -rss_limit_mb=512
FUZZ_MAXLEN_snapshot := -max_len=4096
FUZZ_MAXLEN_anti_entropy := -max_len=8192
FUZZ_MAXLEN_store_put := -max_len=1024
FUZZ_TIMEOUT_all := -timeout=5

fuzz-test: fuzz
	@$(foreach t,$(FUZZ_TARGETS),\
	  echo "==> $(t)_fuzz (10s)..."; \
	  echo "────────────────────────────────────────"; \
	  build/fuzz/tests/$(t)_fuzz \
	    -max_total_time=10 -print_final_stats=1 \
	    $(FUZZ_TIMEOUT_all) \
	    $(FUZZ_MAXLEN_$(t)) \
	    $(FUZZ_DICT_$(t)) \
	    tests/fuzz/corpus/$(t) $(ARGS) \
	    || { echo "FAILED: $(t)_fuzz"; exit 1; }; \
	  echo "PASSED: $(t)_fuzz"; echo "";)
	@echo "════════════════════════════════════════"
	@echo "All fuzz targets passed (fuzz)."

.PHONY: fuzz-long
# Longer soak run (5 min per target) — run before major releases.
fuzz-long: fuzz
	@$(foreach t,$(FUZZ_TARGETS),\
	  echo "==> $(t)_fuzz (5min)..."; \
	  echo "────────────────────────────────────────"; \
	  build/fuzz/tests/$(t)_fuzz \
	    -max_total_time=300 -print_final_stats=1 \
	    $(FUZZ_TIMEOUT_all) \
	    $(FUZZ_MAXLEN_$(t)) \
	    $(FUZZ_DICT_$(t)) \
	    tests/fuzz/corpus/$(t) $(ARGS) \
	    || { echo "FAILED: $(t)_fuzz"; exit 1; }; \
	  echo "PASSED: $(t)_fuzz"; echo "";)
	@echo "════════════════════════════════════════"
	@echo "All fuzz targets passed (fuzz-long)."

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
	@echo 'Fuzz tests:'
	@echo '  make fuzz-test              Run all fuzz targets for 10s each'
	@echo '  make fuzz-long              Run all fuzz targets for 5min each (soak)'
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
