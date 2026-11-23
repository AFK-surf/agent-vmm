# Build and package

## Guest

Build on Linux arm64 with a case-sensitive filesystem:

```sh
build/guest/build.sh OUTPUT_DIRECTORY [development|release] [standalone|runtime]
```

The legacy `standalone` and `runtime` arguments select the same bundle.
Set `AGENT_VMM_REFRESH_DEPENDENCIES=1` to refresh the build dependencies.
The build resolves current Alpine stable packages and BuildKit.
It builds FL4, the l4h components, and the l4h Linux kernel from pinned upstream archives.
It builds `l4h-dom0d` and `vmm-guest-init` with Rust 1.96.0 for musl.

The workload initramfs is at `/usr/lib/agent-vmm/workload-initramfs.cpio` inside the root filesystem.
The dom0 service requires `/dev/l4hctl`. It reads a deployment configuration at `/etc/agent-vmm/dom0.json`.
Without that file, the service writes the configuration for the Carrier VM at start.

The bundle contains `Image`, `initramfs.xz`, `rootfs.squashfs`, and `manifest.json`.
`Image` holds the FL4 image loader, FL4, the l4h components, and the l4h Linux kernel.
The completed Guest manifest records the bundle content identity.

Check the extracted Guest root filesystem on Linux arm64:

```sh
unsquashfs -d ROOTFS OUTPUT_DIRECTORY/rootfs.squashfs
build/guest/smoke.sh ROOTFS
```

The smoke test checks the Rust and BuildKit executables in the unpacked root filesystem.

## macOS Host package

```sh
AGENT_VMM_GUEST_BUNDLE=GUEST_DIRECTORY OUTPUT_DIRECTORY=HOST_DIRECTORY make headless-release
OUTPUT_DIRECTORY=HOST_DIRECTORY build/host/install.sh --service-type agent
```

Use the same `OUTPUT_DIRECTORY` for packaging and installation.
The installer rejects positional paths.
Use `--service-type agent` for the GUI launchd domain of a logged-in user.
Use `--service-type daemon --service-user USER` for a non-root user managed by a LaunchDaemon.
`auto` selects daemon with administrator access. Otherwise, it requires the GUI domain of the user.

Verify the installed helper set before you report the local version.

The Carrier embeds the canonical Guest manifest digest at link time.
Use Rust 1.96.0 and the Protobuf compiler to build the package.
Build and check the console with `./apps/console/scripts/verify.sh`.

## Linux Host package

Build on Linux arm64:

```sh
AGENT_VMM_GUEST_BUNDLE=GUEST_DIRECTORY \
  OUTPUT_DIRECTORY=HOST_DIRECTORY build/host-linux/package.sh
```

Set `AGENT_VMM_HOST_BIN` to use an existing `agent-vmm` executable with the selected release ID.
Set `AGENT_VMM_DATA_WORKER` to include an existing data worker in the archive.
The script writes a tar archive and a JSON artifact record.
The `release.json` file of the release lists its files, sizes, and SHA-256 content identities.
Read [Install and update](operator/install-and-update.md) for installation and updates.

## Signing

Signing requires `CODE_SIGN_IDENTITY` and `NOTARYTOOL_PROFILE`.
Only the Carrier has the virtualization entitlement.
Set `AGENT_VMM_PROVISIONING_PROFILE` to grant Keychain access to the Host.
The profile must authorize `D9AAN3VJK8.com.afk-surf.agent-vmm-host`.
Existing Keychain installations need a package with Keychain access.
Ad-hoc development packages use file storage.

The release signer runs `build/host/test-keychain.sh` in a logged-in user session.
Use the same signing identity and optional profile as for the Host package.
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

The Guest inventory covers its root filesystem, including BuildKit.
