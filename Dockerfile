# Shopping Sessions: native Rust pipeline.
FROM rust:1.92-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main(){}' > src/main.rs && cargo fetch
COPY src ./src
# Scorers are Cascade Lake or newer; x86-64-v3 (AVX2) is safe on every one of them.
ARG TARGET_CPU=x86-64-v3
RUN case "$(uname -m)" in x86_64) export RUSTFLAGS="-C target-cpu=$TARGET_CPU";; esac; \
    cargo build --release --locked && cp target/release/solution /solution

FROM debian:bookworm-slim
COPY --from=build /solution /app/solution
USER 1000:1000
ENV HOME=/scratch
CMD ["/app/solution"]
