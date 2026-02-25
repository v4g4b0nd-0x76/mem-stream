FROM rust:1.85-slim AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /build/target/release/segmented-memory-storageserver /server
EXPOSE 8080
CMD ["/server"]
