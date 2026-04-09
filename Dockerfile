# Polymarket HFT Arbitrage Bot - Production Dockerfile
# Multi-stage build for minimal image size

# =============================================================================
# Build Stage
# =============================================================================
FROM rust:1.76-slim-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Create dummy source to cache dependencies
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn lib() {}" > src/lib.rs

# Build dependencies (this layer is cached)
RUN cargo build --release && \
    rm -rf src target/release/deps/polymarket*

# Copy actual source code
COPY src ./src
COPY config ./config
COPY migrations ./migrations

# Build the application
RUN cargo build --release --features full

# =============================================================================
# Runtime Stage
# =============================================================================
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -r -s /bin/false polybot

# Create directories
RUN mkdir -p /app/config /app/logs /app/data && \
    chown -R polybot:polybot /app

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/polymarket-hft /app/
COPY --from=builder /app/config/default.toml /app/config/

# Set ownership
RUN chown -R polybot:polybot /app

# Switch to non-root user
USER polybot

# Environment variables
ENV RUST_LOG=info
ENV RUST_BACKTRACE=1

# Expose ports
# 8080 - Dashboard
# 8081 - Health check
# 9090 - Prometheus metrics
EXPOSE 8080 8081 9090

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8081/health || exit 1

# Default command
ENTRYPOINT ["/app/polymarket-hft"]
CMD ["run", "--config", "/app/config/config.toml"]
