# Build and package

## Guest

Build on Linux arm64 with a case-sensitive filesystem:

```sh
build/guest/build.sh OUTPUT_DIRECTORY [development|release] [standalone|runtime]
```

`standalone` includes the MCP appliance. `runtime` omits that appliance.
Set `AGENT_VMM_REFRESH_DEPENDENCIES=1` to refresh the build dependencies.
The build resolves current Alpine stable packages, gVisor, and BuildKit.
It does not pin their versions or input hashes.
The publisher runs smoke tests and generates SBOMs from the exact bundle before publication.

The completed Guest manifest records the bundle content identity.

Run the sandbox smoke test on Linux arm64:

```sh
unsquashfs -d ROOTFS OUTPUT_DIRECTORY/rootfs.squashfs
build/guest/smoke.sh ROOTFS
```

The smoke test runs `runsc --rootless --network=none do true` in the unpacked root filesystem.

## macOS Host package

```sh
AGENT_VMM_GUEST_BUNDLE=GUEST_DIRECTORY OUTPUT_DIRECTORY=HOST_DIRECTORY make headless-release
OUTPUT_DIRECTORY=HOST_DIRECTORY build/host/install.sh --service-type agent
```

Use the same `OUTPUT_DIRECTORY` for packaging and installation.
The installer rejects positional paths.
Use `--service-type agent` for a logged-in user's GUI launchd domain.
Use `--service-type daemon --service-user USER` for a non-root user managed by a LaunchDaemon.
`auto` selects daemon with administrator access. Otherwise, it requires the user's GUI domain.
Verify the installed helper set before you report the local version.

The Host embeds the canonical Guest manifest digest at link time.
Build the debugging app with `make swift-project`.

## Linux Host package

Build a Linux arm64 data worker before packaging:

```sh
cargo build --release --locked --target aarch64-unknown-linux-gnu \
  --manifest-path data/agent-vmm-data/Cargo.toml
AGENT_VMM_GUEST_BUNDLE=GUEST_DIRECTORY \
  AGENT_VMM_DATA_WORKER=data/agent-vmm-data/target/aarch64-unknown-linux-gnu/release/agent-vmm-data \
  OUTPUT_DIRECTORY=HOST_DIRECTORY build/host-linux/package.sh
```

The script writes a tar archive and a JSON artifact record.
The release's `release.json` lists its files, sizes, and SHA-256 content identities.
Read [the Linux Host guide](../test/kvmdev/README.md#linux-host-service) for installation and updates.

## Signing

Release packages build with `-tags keychain`.
Signing requires `CODE_SIGN_IDENTITY`, `NOTARYTOOL_PROFILE`, and `AGENT_VMM_PROVISIONING_PROFILE`.
The profile must authorize `D9AAN3VJK8.com.afk-surf.agent-vmm-host`.
The package embeds that profile and the fixed Keychain access group.
Ad-hoc development packages use file storage and carry no Keychain permissions.

The release signer runs `build/host/test-keychain.sh` in a logged-in user session.
Supply `OUTPUT_DIRECTORY`, `CODE_SIGN_IDENTITY`, and `AGENT_VMM_PROVISIONING_PROFILE`.
The script signs a test app and checks real Keychain storage with isolated test items.
Ordinary CI compiles the backend without accessing Keychain.
This repository does not publish binaries or hold signing secrets.

## Third-party notices and inventories

Packages include `LICENSE`, `NOTICE`, and Host and Guest `THIRD_PARTY_NOTICES` files.
The Guest manifest does not include the notices file. Runtime code does not read it.
Read [NOTICE](../NOTICE) for derived-source licenses.

SBOMs are CI inventories. They are not runtime authority and do not enter the Guest bundle.
Generate inventories from the unpacked Guest and packaged Host:

```sh
build/guest/sbom.sh ROOTFS INVENTORY_DIRECTORY
build/host/sbom.sh HOST_DIRECTORY HOST_INVENTORY_DIRECTORY
```

The Guest inventory covers its root filesystem and nested OCI images, including BuildKit.
