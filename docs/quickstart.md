# Quickstart

## Prerequisites

Use macOS 15 or later on Apple Silicon.
Build or get a Host package. This repository does not publish binaries.
Use [the build guide](build.md) to create a package.

## Install and start

1. Install the package:

   ```sh
   OUTPUT_DIRECTORY=HOST_DIRECTORY build/host/install.sh --service-type agent
   ```

   Expect the lifecycle JSON result to report `outcome: succeeded`.

2. Add `~/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app/Contents/Helpers` to the `PATH` of your shell.
   Expect `agent-vmm inspect --json` to return one JSON object.

3. Start the VM:

   ```sh
   agent-vmm vm start --request-id first-start --json
   agent-vmm vm status --json
   ```

   Expect the VM status to report `running`.
   The VM contains seL4, l4h, and dom0. A VM start or stop affects every workload VM.

## Run a container

1. The lifecycle installer activates the bundled MCP appliance.
   Follow [MCP client setup](user/mcp.md#connect-a-client) to issue a token and connect a client.
   Expect the appliance frontend to report available.

2. Ask the MCP client to call `image_pull` for `alpine:latest`.
   Expect the image to become available in its environment.

3. Ask the client to create a volume, then call `container_run` with that volume mounted at `/data`.
   Use the numeric UID/GID that owns the volume.
   Expect the container to reach `running`.

4. Use `container_exec` to write `hello` to `/data/hello`.
   Stop and restart the container. Read `/data/hello` with `container_exec`.
   Expect `hello`. Keep the volume until you approve its deletion.

## Clean the environment

1. Ask the MCP client for a `cleanup_plan` with `kind: discard`.
2. Review the exact environment, plan revision, and volume set.
3. Confirm the complete volume set and irreversible deletion through `cleanup_execute`.
4. Require a terminal cleanup receipt for that environment.

Preserve required data before you confirm deletion.
