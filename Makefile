.PHONY: build-test build-fuse build-nfs build-all test test-fuse test-nfs clean

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

clean:
	cargo clean
