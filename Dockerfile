FROM rust:1.95-bookworm AS build
WORKDIR /build
COPY Cargo.toml ./
COPY crates ./crates
RUN cargo generate-lockfile
RUN cargo build --release --locked -p metrics-summary-collector --all-features

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/target/release/metrics-summary-collector /usr/local/bin/metrics-summary-collector
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/metrics-summary-collector"]
CMD ["--config", "/etc/metrics-summary/collector.toml"]
