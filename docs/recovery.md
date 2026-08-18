# Appliance recovery

An installation owns one verified artifact and one encrypted data volume. Host
state records the current boot ID, container instance, bootstrap ID, connection
epoch, and installation revision. A restart creates a fresh boot/container
instance and revokes the previous runtime capabilities before publishing the
replacement.

Bootstrap envelopes are opaque, encrypted at rest, bound to the installation
and random single-use bootstrap ID, and deleted after consumption. A replay,
stale instance, wrong artifact ID, lost key, or volume-integrity failure fails
closed; publisher and platform trust failures enter `SEALED`.

Sealed repair is an explicit plan/execute operation. It never formats an
unknown volume and never restores a previous product state. Product updates
use a new installation replacement: install and verify the new artifact,
migrate product data inside the appliance, switch the consumer, then revoke
and remove the old installation.

Removal is revision-fenced. A plan first creates a pending-destroy tombstone;
execute checks the expected revision and domain emptiness, revokes trust and
leases, deletes the encrypted volume only after explicit confirmation, and
commits the terminal removal operation atomically.

## Pre-release and build-identity cleanup

Development and release installations do not share identity or state. Before
switching build identity, uninstall the current app with `build/host/uninstall.sh
--purge`, then install the newly signed bundle. Purging removes the current Agent
VMM application-support directory, including appliance secrets, the device-root
key, controller credentials, encrypted appliance volumes, and VM state. Product
data must be exported by the product before this step; the host does not migrate
it.

Older developer installations are unsupported. Remove them once, manually,
while Agent VMM is stopped:

1. Boot out `com.afk-surf.agent-vmm` and
   `com.afk-surf.agent-vmm-connect` from the current GUI launchd domain.
2. Delete the old Agent VMM app and any separately copied `agent-vmm-mcp`,
   `agent-vmm-connect`, or data-worker binaries.
3. Delete obsolete Agent VMM launchd plists, connector configuration, MCP client
   configuration, and the old application-support and log directories.
4. Install the current bundle and recreate controller registrations, MCP client
   bindings, and appliance installations from fresh identities.

Do not copy old SQLite databases, credentials, appliance secrets, device-root
keys, or data volumes into the new installation. Production code does not detect,
migrate, or read these formats.
