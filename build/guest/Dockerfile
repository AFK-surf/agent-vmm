FROM golang:1.26-alpine AS tools
RUN apk add --no-cache coreutils cpio curl file findutils grep jq squashfs-tools tar xz
WORKDIR /src
COPY go.mod go.sum ./
COPY build/guest/kernel/ build/guest/kernel/
COPY build/guest/oci/ build/guest/oci/
COPY build/notices/ build/notices/
RUN go build -o /usr/local/bin/guest-kernel ./build/guest/kernel && \
    go build -o /usr/local/bin/guest-oci ./build/guest/oci && \
    go build -o /usr/local/bin/guest-notices ./build/notices

FROM moby/buildkit:rootless AS buildkit
USER root
RUN apk upgrade --no-cache

FROM tools AS dependencies
COPY build/guest/dependencies.sh /usr/local/bin/guest-dependencies
RUN --mount=type=cache,id=agent-vmm-guest-downloads,target=/var/cache/agent-vmm-downloads,sharing=locked \
    sh /usr/local/bin/guest-dependencies

FROM rust:1.96.0-alpine AS compile
RUN apk add --no-cache build-base linux-headers musl-dev perl protobuf-dev
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY bins/ bins/
COPY data/ data/
COPY third_party/l4h/ third_party/l4h/
COPY third_party/licenses/ third_party/licenses/
COPY api/ api/
COPY --from=tools /usr/local/bin/guest-notices /usr/local/bin/
COPY build/guest/compile.sh /usr/local/bin/guest-compile
RUN --mount=type=cache,id=agent-vmm-guest-rust-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=agent-vmm-guest-rust-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=agent-vmm-guest-rust-target,target=/src/target,sharing=locked \
    sh /usr/local/bin/guest-compile

FROM tools AS assembly
COPY --from=dependencies /dependencies/root/ /dependencies/root/
COPY --from=dependencies /dependencies/bundle/ /dependencies/bundle/
COPY --from=compile /binaries/ /binaries/
COPY --from=buildkit / /assembly/builder-root/
COPY third_party/licenses/ /src/third_party/licenses/
COPY build/guest/root/ /src/build/guest/root/
COPY build/guest/initramfs/ /src/build/guest/initramfs/
COPY build/guest/assemble.sh /usr/local/bin/guest-assemble
COPY build/guest/smoke.sh /usr/local/bin/guest-smoke
ARG BUILD_PROFILE=development
RUN --mount=type=cache,id=agent-vmm-guest-go-modules,target=/go/pkg/mod,sharing=locked \
    sh /usr/local/bin/guest-assemble "$BUILD_PROFILE"

FROM scratch AS output
COPY --from=assembly /assembly/bundle/ /
