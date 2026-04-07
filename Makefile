.PHONY: build-test build-fuse build-nfs build-all test test-fuse test-nfs clean build-release package

# Build the binaries needed by integration tests
build-test: build-fuse build-nfs

build-fuse:
	cargo build --features fuse --bin llmfs-mount-fuse

build-nfs:
	cargo build --features nfs --bin llmfs-mount-nfs

build-all:
	cargo build --features fuse,nfs

# Compile the test binaries (without running them)
test-build: build-test
	cargo test --features fuse,nfs --no-run

# Run all tests (builds binaries first)
test: build-test
	cargo test --features fuse,nfs

test-fuse: build-fuse
	cargo test --features fuse --test fuse_ops

test-nfs: build-nfs
	cargo test --features nfs --test nfs_ops

# Build all release binaries
build-release:
	cargo build --release --features fuse --bin llmfs-mount-fuse --bin llmfs-mount-fuse-sidecar
	cargo build --release --features nfs --bin llmfs-mount-nfs
	cargo build --release --bin llmfs-mount

# Build .deb and .rpm packages
package: build-release
	cargo deb --no-build --no-strip
	cargo generate-rpm
	@echo "\n=== Packages ==="
	@ls -1 target/debian/*.deb target/generate-rpm/*.rpm 2>/dev/null

clean:
	cargo clean
