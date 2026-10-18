FROM rust:1.99-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential cmake libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
RUN --mount=type=cache,id=s/e13bd738-a4e3-4255-b6b8-77ecffbbdbf4-/usr/local/cargo/registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=s/e13bd738-a4e3-4255-b6b8-77ecffbbdbf4-/usr/local/cargo/git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=s/e13bd738-a4e3-4255-b6b8-77ecffbbdbf4-/build/target,target=/build/target,sharing=locked \
    cargo build --release --locked \
    && strip target/release/taz-faucet-zypherpunk-discord \
    && cp target/release/taz-faucet-zypherpunk-discord /build/faucet

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home faucet
COPY --from=build /build/faucet /usr/local/bin/taz-faucet-zypherpunk-discord
USER 10001:10001
WORKDIR /app
ENTRYPOINT ["/usr/local/bin/taz-faucet-zypherpunk-discord"]
