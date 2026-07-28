PRESET   ?= debug
BUILD_DIR = build/$(PRESET)
BIN_DIR   = $(BUILD_DIR)/bin
JOBS     ?= $(shell nproc 2>/dev/null || echo 4)
ARGS     ?=

SERVER = cinderd
CLI    = cinder-cli

.PHONY: all
all: build

$(BUILD_DIR)/build.ninja:
	@echo "==> Configuring $(PRESET)..."
	cmake --preset $(PRESET)

.PHONY: configure
configure: $(BUILD_DIR)/build.ninja

.PHONY: build debug release sanitized ci fast
build: $(BUILD_DIR)/build.ninja
	@echo "==> Building $(PRESET)..."
	cmake --build --preset $(PRESET) -j$(JOBS)
	@ln -sf $(BUILD_DIR)/compile_commands.json compile_commands.json

debug:     PRESET=debug
release:   PRESET=release
sanitized: PRESET=sanitized
ci:        PRESET=ci
fast:      PRESET=fast

debug release sanitized ci fast: build

.PHONY: run run-cli
run: build
	$(BIN_DIR)/$(SERVER) $(ARGS)

run-cli: build
	$(BIN_DIR)/$(CLI) $(ARGS)

.PHONY: test test-unit test-integration test-sim
test: build
	ctest --preset $(PRESET) --output-on-failure -j$(JOBS)

test-unit: build
	$(BIN_DIR)/cinder_unit_tests

test-integration: build
	$(BIN_DIR)/cinder_integration_tests

test-sim: build
	$(BIN_DIR)/cinder_sim_tests

.PHONY: bench
bench: build
	$(BIN_DIR)/cinder_throughput_bench

.PHONY: format check-format
format: $(BUILD_DIR)/build.ninja
	cmake --build $(BUILD_DIR) --target format -j$(JOBS)

check-format: $(BUILD_DIR)/build.ninja
	cmake --build $(BUILD_DIR) --target check-format -j$(JOBS)

.PHONY: clean clean-all
clean:
	rm -rf $(BUILD_DIR)

clean-all:
	rm -rf build/

.PHONY: help
help:
	@echo 'Usage: make <target> [PRESET=debug|release|sanitized|ci|fast] [ARGS=...]'
	@echo ''
	@echo 'Build:      make / make debug / release / sanitized / ci / fast'
	@echo 'Run:        make run ARGS="--port 7000"'
	@echo '            make run-cli ARGS="get foo"'
	@echo 'Test:       make test / test-unit / test-integration / test-sim'
	@echo 'Bench:      make bench'
	@echo 'Format:     make format / check-format'
	@echo 'Clean:      make clean / clean-all'
