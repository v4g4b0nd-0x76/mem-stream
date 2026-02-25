FROM rust:1.85-slim AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /build/target/release/client /client
ENV SERVER_ADDR=host.docker.internal:8080
ENV POOL_SIZE=10
ENV HTTP_ADDR=0.0.0.0:8000
EXPOSE 8000
CMD ["/client"]
