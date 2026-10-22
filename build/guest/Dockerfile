FROM golang:1.26-alpine AS tools
RUN timeout 600 apk add --no-cache bzip2 coreutils cpio curl file findutils git grep jq squashfs-tools tar xz

FROM moby/buildkit:rootless AS buildkit

FROM tools AS dependencies
WORKDIR /kernel-extract
COPY go.mod go.sum ./
COPY build/guest/kernel/ build/guest/kernel/
RUN --mount=type=cache,id=agent-vmm-guest-go-build,target=/root/.cache/go-build,sharing=locked \
    go build -o /usr/local/bin/guest-kernel ./build/guest/kernel
COPY build/guest/dependencies.sh /usr/local/bin/guest-dependencies
RUN --mount=type=cache,id=agent-vmm-guest-downloads,target=/var/cache/agent-vmm-downloads,sharing=locked \
    sh /usr/local/bin/guest-dependencies

FROM tools AS compile
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,id=agent-vmm-guest-go-modules,target=/go/pkg/mod,sharing=locked \
    timeout 900 go mod download
COPY api/ api/
COPY cmd/agent-vmmd/ cmd/agent-vmmd/
COPY cmd/agent-vmm-supervisor/ cmd/agent-vmm-supervisor/
COPY cmd/agent-vmm-mcp-appliance/ cmd/agent-vmm-mcp-appliance/
COPY internal/ internal/
COPY controller/remote/ controller/remote/
COPY build/appliance/manifest/ build/appliance/manifest/
COPY build/guest/oci/ build/guest/oci/
COPY build/guest/bundle/ build/guest/bundle/
COPY build/guest/compile.sh /usr/local/bin/guest-compile
RUN --mount=type=cache,id=agent-vmm-guest-go-modules,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,id=agent-vmm-guest-go-build,target=/root/.cache/go-build,sharing=locked \
    timeout 900 sh /usr/local/bin/guest-compile

FROM tools AS assembly
COPY --from=dependencies /dependencies/root/ /dependencies/root/
COPY --from=dependencies /dependencies/bundle/ /dependencies/bundle/
COPY --from=compile /binaries/ /binaries/
COPY --from=buildkit / /assembly/builder-root/
COPY build/guest/root/ /src/build/guest/root/
COPY build/guest/initramfs/ /src/build/guest/initramfs/
COPY build/guest/assemble.sh /usr/local/bin/guest-assemble
ARG BUILD_PROFILE=development
ARG BUNDLE_FLAVOR=standalone
RUN sh /usr/local/bin/guest-assemble "$BUILD_PROFILE" "$BUNDLE_FLAVOR"

FROM scratch AS output
COPY --from=assembly /assembly/bundle/ /
