# Nested KVM smoke

Run this test on an Apple Silicon Mac with nested virtualization support and Docker.
The test uses a disposable Linux VM under Apple Virtualization, then starts a KVM Guest inside it.
The KVM Guest uses one vCPU and 512 MiB of RAM.

The local Docker image `agent-vmm-guest-tools:local` must exist.
An existing Guest bundle must contain `Image` and `initramfs.xz`.
The scripts copy the kernel and create disposable initramfs files.

## Run

1. Open a shell at the repository root.
2. Select the existing Guest bundle and a dedicated test directory.

   ```sh
   smoke_bundle="dist/Agent VMM Host.app/Contents/Resources/Guest"
   smoke_work=".build/kvmdev/smoke"
   ```

3. Build the Linux smoke binary and its initramfs.

   ```sh
   test/kvmdev/prepare-smoke.sh "$smoke_bundle" "$smoke_work"
   ```

4. Start the disposable outer VM.

   ```sh
   test/kvmdev/run.sh "$smoke_bundle" "$smoke_work"
   ```

The smoke has a two-minute deadline.
The outer VM powers off after the test.
Success requires `result.status` to contain `0` and `result.log` to end with `PASS nested KVM block/vsock/process smoke`.

Read `result.log` for test results.
Read `inner.log` for KVM Guest serial output.
Read `outer.log` for the outer VM serial output.
These files reside in the selected test directory.

## Coverage

- Boot the existing Alpine kernel through KVM and load the virtio drivers.
- Find the `AGENT_ROOT` and `AGENT_STATE` disks by their serial identifiers.
- Reject a root disk write and verify its original bytes.
- Write the state disk, call `Sync`, and read the bytes.
- Exercise Host connections to Guest port 4050 and Guest connections to Host port 4051.
- Verify finite responses, EOF propagation, and both connection directions through the VMM process relay.
- Adopt the existing VMM with a new controller, fence the previous controller, and preserve the Guest boot ID and disk bytes.
- Stop and restart the VMM while preserving disk bytes and changing the Guest boot ID.

The smoke creates its raw disks under `/tmp/kvm-smoke-*` inside the outer VM.
Each disk has a size of 4 MiB.
The smoke removes these files after the test.

## Limits

The stop operation tests explicit VMM cancellation.
This smoke does not cover graceful VMMD shutdown, containerd, gVisor, full Guest startup, or filesystem conversion.
It does not test legacy disk migration, amd64 runtime, or Windows Hyper-V runtime.

EOF and persistence checks use a small test program in the initramfs.
EOF checks include finite responses larger than one virtio-vsock packet.
EOF results do not replace the full Guest integration tests.

## Full Guest smoke

This test uses the complete Guest and current VMMD API.
The KVM Guest has one vCPU, 2 GiB of RAM, and a disposable 32 GiB sparse state disk.
The sparse disk belongs to the test fixture. It does not validate Host physical backing.

1. Select a Guest bundle built from the current source.

   ```sh
   guest_bundle=".build/linux-kvm/guest"
   guest_work=".build/kvmdev/guest-smoke-current"
   ```

2. Prepare the test binary, Guest copies, and a small offline Busybox OCI image.

   ```sh
   test/kvmdev/guest-smoke-prepare.sh "$guest_bundle" "$guest_work" --built-bundle
   ```

3. Run the full Guest test.

   ```sh
   test/kvmdev/guest-smoke-run.sh "$guest_bundle" "$guest_work"
   ```

The wrapper records macOS time before it starts the outer VM.
The test compares Guest time with outer Linux time and that independent macOS reference.
It does not set either Linux clock.
The Guest uses the emulated PL031 RTC.

Success requires healthy VMMD components, an egress handshake, namespace creation, OCI import, and a real gVisor workload.
The workload writes a known marker and its Unix time to the streamed logs, then exits with code zero.
The test calls `PrepareShutdown` and waits for Guest poweroff and VMM exit.
It has a four-minute deadline.

Read `result.log`, `guest-inner.log`, and `outer.log` in the selected test directory.
`result.status` must contain `0`.
The final result line must be `PASS Guest PrepareShutdown and complete VMM exit`.

To test current daemon code with cached dependencies, omit `--built-bundle` during preparation.
That mode rebuilds VMMD and the supervisor, then copies current Guest configuration and initramfs code into disposable images.
It does not change the source bundle.
Its result applies to the cached kernel and dependencies. It does not validate the current dependency set.
Both modes use the existing Docker tools image without downloads.

The current-bundle test passed on 2026-10-22 with kernel 6.18.58, containerd 2.3.7, and runsc release-20261012.0.
These versions record the tested artifacts. They are not dependency pins.
The test runs VMMD through the VMM process API, without the complete Host and Rust data worker.
It does not test public registry traffic, Host deployment, filesystem conversion, amd64 runtime, or Windows runtime.

## Linux Host development

Build on Linux arm64 with Go and an existing current Guest bundle.
The service user must have access to `/dev/kvm` and must not be root.
Allow the configured VM memory plus 4 GiB for Linux.
A new default state disk requires 64 GiB of physical backing plus 10 GiB of free emergency space.
The Host creates its files under the selected private state directory.

Link the canonical Guest manifest SHA-256 into the Linux CLI, as the macOS package script does.

```sh
guest_bundle="$(pwd)/.build/linux-kvm/guest"
guest_manifest_digest=$(sha256sum "$guest_bundle/manifest.json" | cut -d ' ' -f 1)
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build \
  -ldflags "-X main.guestManifestDigest=$guest_manifest_digest" \
  -o .build/linux-kvm/agent-vmm ./cmd/agent-vmm
```

Build or supply `agent-vmm-data` separately for Linux arm64.
Its source is `data/agent-vmm-data`.
The Host supervises this Rust executable. The Host and KVM process share the Go executable.
The following foreground example requires an existing Linux data worker at the selected absolute path.

```sh
agent_vmm="$(pwd)/.build/linux-kvm/agent-vmm"
data_worker="/absolute/path/to/agent-vmm-data"
agent_state="$HOME/.local/state/agent-vmm"
test -x "$data_worker"
"$agent_vmm" host \
  --bundle "$guest_bundle" \
  --instance "$agent_state/instance" \
  --cpus 1 --memory 2147483648 \
  --serial-log "$agent_state/serial.log" \
  --operator-socket "$agent_state/operator.sock" \
  --remote-compute-socket "$agent_state/remote.sock" \
  --database "$agent_state/host.db" \
  --session-directory "$agent_state/sessions" \
  --data-worker "$data_worker"
```

For a new instance, start the VM from another shell with the same variables.

```sh
"$agent_vmm" vm start --operator-socket "$agent_state/operator.sock" --request-id linux-dev-start
"$agent_vmm" vm status --operator-socket "$agent_state/operator.sock"
```

These commands run a foreground development Host.
The native foreground Host and independent Rust data worker passed the complete probe below.
Linux rejects Host directory sharing. The default vCPU count and memory follow the macOS sizing policy.
The foreground Host does not install a service or change device permissions.

## Linux Host service

The release installs one systemd unit, `agent-vmm-host.service`.
The unit runs `/opt/agent-vmm/current/agent-vmm host` as the `agent-vmm` user.
The installer creates this system user and adds it to the `kvm` group.
The Host keeps its state in `/var/lib/agent-vmm` and its logs in `/var/lib/agent-vmm/logs`.
A new default state disk requires 64 GiB of physical backing plus 10 GiB of free space in `/var/lib`.

Make the release tar on the build machine.
`AGENT_VMM_DATA_WORKER` must be a Linux arm64 `agent-vmm-data` executable.

```sh
AGENT_VMM_GUEST_BUNDLE="$(pwd)/.build/linux-kvm/guest" \
AGENT_VMM_DATA_WORKER="$(pwd)/.build/linux-kvm/agent-vmm-data" \
AGENT_VMM_RELEASE_ID=linux-dev-1 \
OUTPUT_DIRECTORY="$(pwd)/dist/linux-arm64" \
  build/host-linux/package.sh
```

The script writes `agent-vmm-linux-dev-1-linux-arm64.tar.gz` and a JSON record.
The record gives the release ID, the content ID, and the tar SHA-256 and size.

To install the release with a Salix install descriptor:

1. Copy the tar and the descriptor JSON to the Linux machine.
2. Extract the tar.

   ```sh
   tar -xzf agent-vmm-linux-dev-1-linux-arm64.tar.gz
   ```

3. Run the install as root. The request ID must be the descriptor `operation_id`.

   ```sh
   sudo ./agent-vmm-linux-dev-1/agent-vmm lifecycle install --operation-stdin \
     --request-id "$(jq -r .operation_id descriptor.json)" < descriptor.json
   ```

4. Read the result. A complete install reports `outcome` `succeeded`.

   ```sh
   sudo /opt/agent-vmm/current/agent-vmm inspect --json
   sudo /opt/agent-vmm/current/agent-vmm logs --component host --json
   ```

To retry an install, run the same command with the same descriptor.
To install without enrollment, omit `--operation-stdin` and use a new request ID.

The descriptor `exchange_url` must use HTTPS or loopback HTTP.
This rule keeps the one-time secret off a plain network connection.
To test with a development server on the outer host, forward a loopback port:

```sh
socat TCP-LISTEN:4000,bind=127.0.0.1,reuseaddr,fork TCP:192.168.64.1:4000 &
jq '.exchange_url |= sub("^http://[^/]+"; "http://127.0.0.1:4000")' descriptor.json > local-descriptor.json
```

Then use `local-descriptor.json` as the descriptor in step 3.

To update the release, serve the new tar over HTTPS and use its JSON record:

```sh
sudo /opt/agent-vmm/current/agent-vmm update --request-id linux-update-1 \
  --target-release-id linux-dev-2 --artifact-url https://example.test/agent-vmm-linux-dev-2-linux-arm64.tar.gz \
  --artifact-sha256 "$artifact_sha256" --artifact-size "$artifact_size" --json
```

The update keeps `/var/lib/agent-vmm`, including `instance/state.raw`, `host.sqlite`, keys, and registrations.
To remove the service and keep the data, run `sudo /opt/agent-vmm/current/agent-vmm lifecycle uninstall`.

## Complete Linux Host probe

Use this probe with a running Linux arm64 Host and its Rust data worker.
Build the probe locally, then copy it into the experiment directory under `bin/` on that Linux machine.
The probe does not deploy the Host or change system configuration.

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build \
  -o .build/linux-kvm/host-smoke ./test/kvmdev/host-smoke_linux.go
```

The native fixture uses a 16 GiB state disk, the supported minimum, and preserves the 10 GiB Host emergency space.
It uses existing sudo permission to select the KVM group for the Host process.
It does not change account group membership or device permissions.
For a new private experiment, select the copied Guest bundle and start the Host in one shell.

```sh
experiment="$HOME/Experiments/agent-vmm"
agent_state="$experiment/state"
guest_bundle="$experiment/guest"
sudo -n -u "$USER" -g kvm "$experiment/bin/agent-vmm" host \
  --bundle "$guest_bundle" \
  --instance "$agent_state/instance" \
  --cpus 1 --memory 2147483648 --state-size 17179869184 \
  --serial-log "$agent_state/serial.log" \
  --operator-socket "$agent_state/operator.sock" \
  --remote-compute-socket "$agent_state/remote.sock" \
  --database "$agent_state/host.db" \
  --session-directory "$agent_state/sessions" \
  --data-worker "$experiment/bin/agent-vmm-data"
```

Run the probe from another shell as the Host service user, with absolute paths.

```sh
experiment="$HOME/Experiments/agent-vmm"
"$experiment/bin/host-smoke" \
  --operator-socket "$experiment/state/operator.sock" \
  --data-worker-socket "$experiment/state/data-worker.sock" \
  --data-worker "$experiment/bin/agent-vmm-data" \
  --directory "$experiment" \
  --run-id linux-host-smoke-1
```

The probe starts a disposable Rust peer under the experiment directory.
It sends a real QUIC MeshControl request through the Host worker to the Host gRPC service.
The service must return `NotFound` for the absent export ID.
After the tunnel closes, worker counters must show traffic in both directions.
The probe closes the peer and removes its temporary files.

The probe then uses the existing Operator and Agent Runtime APIs.
It creates a manual integration, opens an agent session, and enables public Internet access for that environment.
Its 128 MiB writable quota keeps the fixture small.
The quota does not change product admission or storage checks.

The default image is [`quay.io/curl/curl:latest`](https://github.com/curl/curl-container), published by the curl project.
The default probe requires `downloading from registry` progress and a final digest from the public registry TLS path.
A cached image alone does not prove a fresh registry download.
Use `--require-registry-download=false` for an availability-only rerun, and retain the original cold-download result.
The probe labels cached results separately. It does not delete shared image content to force a cold pull.

The gVisor workload must fetch `https://example.com` with normal certificate verification.
A second request to `https://self-signed.badssl.com/` must fail with curl exit code `60`.
A timeout or another network error does not pass the certificate check.
The probe requires both log markers and workload exit code zero.

Use `--image`, `--https-url`, and `--invalid-cert-url` to select equivalent public fixtures.
These test options do not add a product network fallback.
The native test machine could reach Quay and both HTTPS fixtures, but Docker Hub timed out during the baseline check.

The probe revokes its agent session after the test.
It retains the integration, environment, image record, and container in the experiment instance for inspection.
Use a distinct run ID for a new probe run.

After the probe, use the existing VM command for normal Guest shutdown.

```sh
"$experiment/bin/agent-vmm" vm stop \
  --operator-socket "$experiment/state/operator.sock" \
  --request-id linux-host-smoke-stop
```

Stop the foreground Host with `Ctrl-C` after the VM stops.
Keep the experiment state and result logs for inspection.
Do not delete shared image content or an existing installation to prepare another probe.

### Native validation evidence

On 2026-10-22, the complete probe passed on Ubuntu 24.04 arm64 with kernel `7.0.0-1019-nvidia`.
The Host ran through `sudo -n -u darksky -g kvm` for that process.
The test did not change permanent account group membership.
Its 16 GiB state disk had full physical backing and preserved the 10 GiB Host emergency space.
The test did not register services, change system configuration, or install global dependencies.

The real Rust QUIC path carried 297 input bytes and 136 output bytes through the Host MeshControl service.
All 23 Linux Rust worker tests also passed, without Rust source or crate changes.
The public Quay pull reported `downloading from registry`, then committed image digest `sha256:65643c519ca49a564f069edf05dc9b0dc6f0a9d4ef62556d015b82a3c7e45d08`.
It reported 11,115,324 image bytes.
The gVisor workload passed trusted HTTPS, rejected the self-signed certificate with curl code `60`, and exited with code zero.

The Host restarted on the existing state disk and retained the earlier test environment.
The Host booted a new Guest from that retained disk, with boot ID `30e90a3d-5dc3-4ff2-b8d9-835de6e4fc43`.
This tested a disk restart, not a memory resume.
Normal Operator VM stop succeeded without force.
Host `SIGTERM` completed Guest poweroff and removed the Host, VMM, and worker processes.
The final `/proc` check found no executable under the experiment `bin/` directory.

| Artifact | SHA-256 |
| --- | --- |
| Native Host executable | `551041f7b0e9218d69df45a7ac108f7f39acc2726241e690d7f0c2b31286d7f3` |
| Linux Rust data worker | `13a006d3b670b2e9f103cf06af63bedf0afd209e3710bd30529bc6047d85403d` |
| Native probe executable | `6088a8389953c6b7245302a0e2fad8102ff9a40b6bcf84922307eedd06cf9cab` |
| Canonical Guest manifest | `0f6e15682691e560a2adfb56483f3c346cf9c9ad7c20acab5244da26a0954ec8` |

Result files reside in `.build/linux-kvm/native-results` after collection from the experiment machine.
They include `host-smoke.log`, `stop.json`, `host.log`, `serial.log`, and `native-summary.json`.
The summary records artifact hashes, physical state allocation, and an empty list of remaining test processes.
The first cold pull exposed Docker Schema 2 media types in the exported archive.
The fix normalizes the image to OCI before it computes the committed manifest digest.
The complete native probe passed after that fix.
Docker Hub remained unreachable during its baseline check. The successful registry test used the official Quay image.
Legacy disk conversion, service installation, amd64 runtime, and Windows runtime remain outside this native probe.
