FROM rust:1.92.0-slim-trixie AS builder

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    ca-certificates \
    make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src

COPY glidefs/Cargo.toml glidefs/Cargo.lock ./glidefs/
COPY glidefs/src ./glidefs/src

WORKDIR /usr/src/glidefs

RUN cargo build --release

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3t64 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/glidefs/target/release/glidefs /usr/local/bin/glidefs

RUN useradd -m -u 1001 glidefs
USER glidefs

# Default ports that might be used - actual configuration comes from TOML file
EXPOSE 2049 5564 10809

ENTRYPOINT ["glidefs"]
