FROM rust:bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY config ./config

RUN cargo build --release --workspace

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/api /app/bin/api
COPY --from=builder /app/target/release/ingest /app/bin/ingest
COPY --from=builder /app/target/release/feature /app/bin/feature
COPY --from=builder /app/target/release/decision /app/bin/decision
COPY --from=builder /app/target/release/execution /app/bin/execution
COPY --from=builder /app/target/release/training /app/bin/training
COPY --from=builder /app/target/release/notifier /app/bin/notifier
COPY --from=builder /app/config /app/config

ENV APP_ENV=production
ENV RUST_LOG=info
