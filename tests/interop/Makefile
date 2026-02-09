.PHONY: build-rust build-go build-all test test-cross test-mirror test-acp test-queries test-indexes test-bulk ffi-build ffi-test clean

# Paths
DEFRA_GO_PATH ?= ../../../defradb
DEFRA_RS_PATH ?= ../..

# =============================================================================
# P2P Tests (run Rust and Go nodes, test connectivity/sync)
# =============================================================================

# Build the Rust CLI binary
build-rust:
	cd ../.. && cargo build --release -p cli

# Build the Go defra binary
build-go:
	cd $(DEFRA_GO_PATH) && make build

# Build both binaries
build-all: build-rust build-go

# Run P2P tests (4 cross-implementation tests)
test: build-all
	go test -v -timeout 5m ./...

# Run cross-implementation P2P tests only
test-cross: build-all
	go test -v -timeout 5m -run TestCross ./...

# =============================================================================
# Mirror Tests (independent Rust/Go nodes, HTTP API comparison)
# =============================================================================

# Run all mirror-mode tests
test-mirror: build-all
	go test -v -timeout 10m -run "TestACP|TestQuery|TestIndex|TestBulk" ./...

# Run ACP isolation tests
test-acp: build-all
	go test -v -timeout 5m -run "TestACP" ./...

# Run query parity tests
test-queries: build-all
	go test -v -timeout 5m -run "TestQuery" ./...

# Run index tests
test-indexes: build-all
	go test -v -timeout 5m -run "TestIndex" ./...

# Run bulk data tests
test-bulk: build-all
	go test -v -timeout 10m -run "TestBulk" ./...

# =============================================================================
# FFI Tests (run Go integration tests against Rust via FFI)
# =============================================================================

# Build the Rust FFI library
ffi-build:
	cd $(DEFRA_RS_PATH) && cargo build --release -p ffi

# Run FFI tests for a specific package
# Usage: make ffi-test PKG=query/simple
ffi-test: ffi-build
	@if [ -z "$(PKG)" ]; then \
		echo "Usage: make ffi-test PKG=<package>"; \
		echo "Example: make ffi-test PKG=query/simple"; \
		echo ""; \
		echo "Available packages:"; \
		echo "  P0: query/simple, mutation/create, mutation/update, mutation/delete"; \
		echo "  P1: query/inline_array, query/commits, query/json, query/one_to_many, query/one_to_one"; \
		echo "  P2: query/one_to_many_to_one, query/one_to_many_multiple, index, explain"; \
		echo "  P3: view, subscription, collection, collection_version"; \
		exit 1; \
	fi
	cd $(DEFRA_GO_PATH) && \
	DEFRA_RS_PATH=$(abspath $(DEFRA_RS_PATH)) \
	./scripts/run-rust-ffi-tests.sh $(PKG) ./reports

# Run all P0 FFI tests
ffi-test-p0: ffi-build
	cd $(DEFRA_GO_PATH) && \
	DEFRA_RS_PATH=$(abspath $(DEFRA_RS_PATH)) && \
	for pkg in query/simple mutation/create mutation/update mutation/delete; do \
		./scripts/run-rust-ffi-tests.sh $$pkg ./reports; \
	done

# =============================================================================
# Cleanup
# =============================================================================

clean:
	go clean -testcache
