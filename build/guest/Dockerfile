FROM alpine:3.24 AS tools
RUN apk add --no-cache binutils ca-certificates coreutils cpio curl file findutils grep gzip jq libgcc python3 squashfs-tools tar xz
WORKDIR /src

FROM moby/buildkit:rootless AS buildkit
USER root
RUN apk upgrade --no-cache

FROM tools AS dependencies
COPY build/guest/dependencies.sh /usr/local/bin/guest-dependencies
RUN --mount=type=cache,id=agent-vmm-guest-downloads,target=/var/cache/agent-vmm-downloads,sharing=locked \
    sh /usr/local/bin/guest-dependencies

FROM ubuntu:24.04 AS l4h-build
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    bc bison build-essential ca-certificates clang cmake cpio curl \
    device-tree-compiler dwarves flex git libclang-dev \
    libelf-dev libssl-dev libxml2-utils musl-tools ninja-build patch pkg-config python3 \
    python3-pip python3-venv rsync xz-utils \
    && if [ "$(dpkg --print-architecture)" != arm64 ]; then \
         apt-get install -y --no-install-recommends gcc-aarch64-linux-gnu; \
       fi \
    && rm -rf /var/lib/apt/lists/*
COPY third_party/l4h/tools/sel4-requirements.txt /tmp/sel4-requirements.txt
RUN python3 -m venv /opt/sel4-venv \
    && /opt/sel4-venv/bin/pip install --no-cache-dir -r /tmp/sel4-requirements.txt \
    && rm -f /tmp/sel4-requirements.txt
RUN curl -fsSL https://sh.rustup.rs | sh -s -- -y --default-toolchain nightly-2026-08-21 --profile minimal \
    && /root/.cargo/bin/rustup component add rust-src --toolchain nightly-2026-08-21 \
    && /root/.cargo/bin/rustup target add "$(uname -m)-unknown-linux-musl" --toolchain nightly-2026-08-21
ENV PATH="/opt/sel4-venv/bin:/root/.cargo/bin:${PATH}"
WORKDIR /l4h-src
COPY third_party/l4h/ /l4h-src/
COPY build/guest/l4h-build.sh /usr/local/bin/guest-l4h-build
ARG BUILD_PROFILE=development
RUN --mount=type=cache,id=agent-vmm-l4h-downloads,target=/var/cache/l4h-downloads,sharing=locked \
    --mount=type=cache,id=agent-vmm-l4h-cargo,target=/root/.cargo/registry,sharing=locked \
    --mount=type=cache,id=agent-vmm-l4h-cargo-git,target=/root/.cargo/git,sharing=locked \
    --mount=type=cache,id=agent-vmm-l4h-build,target=/l4h-build-cache,sharing=locked \
    bash /usr/local/bin/guest-l4h-build "$BUILD_PROFILE"

FROM rust:1.96.0-alpine AS compile
RUN apk add --no-cache build-base linux-headers musl-dev perl protobuf-dev
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY xtask/ xtask/
COPY .cargo/ .cargo/
COPY crates/ crates/
COPY bins/ bins/
COPY third_party/l4h/ third_party/l4h/
COPY third_party/licenses/ third_party/licenses/
COPY api/ api/
COPY build/guest/compile.sh /usr/local/bin/guest-compile
RUN --mount=type=cache,id=agent-vmm-guest-rust-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=agent-vmm-guest-rust-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=agent-vmm-guest-rust-target,target=/src/target,sharing=locked \
    sh /usr/local/bin/guest-compile

FROM tools AS assembly
COPY --from=compile /usr/local/bin/xtask /usr/local/bin/
COPY --from=dependencies /dependencies/root/ /dependencies/root/
COPY --from=dependencies /dependencies/bundle/ /dependencies/bundle/
COPY --from=compile /binaries/ /binaries/
COPY --from=l4h-build /l4h-artifacts/ /l4h-artifacts/
COPY --from=buildkit / /assembly/builder-root/
COPY third_party/licenses/ /src/third_party/licenses/
COPY third_party/l4h/tools/add-modules.py /src/third_party/l4h/tools/add-modules.py
COPY build/guest/l4h.conf /src/build/guest/l4h.conf
COPY build/guest/root/ /src/build/guest/root/
COPY build/guest/initramfs/ /src/build/guest/initramfs/
COPY build/guest/assemble.sh /usr/local/bin/guest-assemble
COPY build/guest/smoke.sh /usr/local/bin/guest-smoke
ARG BUILD_PROFILE=development
RUN --mount=type=cache,id=agent-vmm-guest-module-zips,target=/var/cache/agent-vmm-go-proxy,sharing=locked \
    GO_PROXY_CACHE_DIR=/var/cache/agent-vmm-go-proxy sh /usr/local/bin/guest-assemble "$BUILD_PROFILE"

FROM scratch AS output
COPY --from=assembly /assembly/bundle/ /
