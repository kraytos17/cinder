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

.PHONY: build debug release sanitized ci fast debug-clang ci-clang
build:
	@if [ ! -f "$(BUILD_DIR)/CMakeCache.txt" ]; then \
		echo "==> Configuring $(PRESET)..."; \
		cmake --preset $(PRESET); \
	fi
	@echo "==> Building $(PRESET)..."
	cmake --build --preset $(PRESET) -j$(JOBS)

debug:       PRESET=debug
release:     PRESET=release
sanitized:   PRESET=sanitized
ci:          PRESET=ci
fast:        PRESET=fast
debug-clang: PRESET=debug-clang
ci-clang:    PRESET=ci
ci-gcc:      PRESET=ci-gcc

debug release sanitized ci fast debug-clang ci-clang ci-gcc: build

.PHONY: run run-cli
run: build
	"$(BIN_DIR)/$(SERVER)" $(ARGS)

run-cli: build
	"$(BIN_DIR)/$(CLI)" $(ARGS)

.PHONY: test test-unit test-integration test-sim
test: build
	ctest --preset $(PRESET) --output-on-failure -j$(JOBS)

test-unit: build
	"$(TESTS_DIR)/cinder_unit_tests"

test-integration: build
	"$(TESTS_DIR)/cinder_integration_tests"

test-sim: build
	"$(TESTS_DIR)/cinder_sim_tests"

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
	rm -rf build/

.PHONY: install
install: build
	cmake --install "$(BUILD_DIR)"

.PHONY: help
help:
	@echo 'Usage: make <target> [PRESET=debug|release|sanitized|ci|fast|debug-clang|ci-clang] [ARGS=...]'
	@echo ''
	@echo 'Build:      make / make debug / release / sanitized / ci / fast / debug-clang / ci-clang'
	@echo 'Run:        make run ARGS="--port 7000"'
	@echo '            make run-cli ARGS="get foo"'
	@echo 'Test:       make test / test-unit / test-integration / test-sim'
	@echo 'Bench:      make bench ARGS="--benchmark_format=json"'
	@echo 'Format:     make format / check-format'
	@echo 'Install:    make install [DESTDIR=/tmp/staging]'
	@echo 'Clean:      make clean / clean-all'
