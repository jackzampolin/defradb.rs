FROM rust:1.93-bookworm AS builder
WORKDIR /build
COPY . .
RUN apt-get update && apt-get install -y libssl-dev pkg-config protobuf-compiler
RUN cargo build --release -p cli

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/defra /usr/local/bin/defra
EXPOSE 9161 9171 9181
ENTRYPOINT ["defra"]
