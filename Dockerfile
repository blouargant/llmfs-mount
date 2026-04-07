# Build llmfs-mount binaries (Rust)
FROM rust:1.89-bookworm AS builder
WORKDIR /build
COPY . .
RUN cargo build --release --no-default-features --features fuse,vendored-openssl \
    --bin llmfs-mount-fuse --bin llmfs-mount-fuse-sidecar

# Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libfuse3-3 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/llmfs-mount-fuse /usr/local/bin/
COPY --from=builder /build/target/release/llmfs-mount-fuse-sidecar /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/llmfs-mount-fuse"]
