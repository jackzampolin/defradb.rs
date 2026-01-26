.PHONY: build-rust build-go build-all test test-connection test-cross clean

# Path to Go DefraDB repo (sibling directory)
DEFRA_GO_PATH ?= ../../../defradb

# Build the Rust FFI library
build-rust:
	cd ../.. && cargo build --release -p ffi

# Build the Go defra binary
build-go:
	cd $(DEFRA_GO_PATH) && make build

# Build both binaries
build-all: build-rust build-go

# Run all tests (builds both binaries first)
test: build-all
	go test -v -timeout 5m ./...

# Run connection tests only (Rust-only, faster)
test-connection: build-rust
	go test -v -timeout 5m -run TestConnection ./...

# Run cross-implementation tests (requires both binaries)
test-cross: build-all
	go test -v -timeout 5m -run TestCross ./...

# Clean test artifacts
clean:
	go clean -testcache
