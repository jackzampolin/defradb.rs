.PHONY: build-rust test test-connection clean

# Build the Rust defra binary
build-rust:
	cd ../.. && cargo build --release -p cli

# Run all tests
test: build-rust
	go test -v -timeout 5m ./...

# Run connection tests only
test-connection: build-rust
	go test -v -timeout 5m -run TestConnection ./...

# Clean test artifacts
clean:
	go clean -testcache
