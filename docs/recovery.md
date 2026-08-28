# Appliance recovery

An installation owns one verified artifact and one encrypted data volume. Host
state records the current boot ID, container instance, bootstrap ID, connection
epoch, and installation revision. A restart creates a fresh boot/container
instance and revokes the previous runtime capabilities before publishing the
replacement.

Bootstrap envelopes are opaque, encrypted at rest, bound to the installation
and random single-use bootstrap ID, and deleted after consumption. A replay,
stale instance, wrong artifact ID, lost key, or volume-integrity failure fails
closed; platform-key and encrypted-volume trust failures enter `SEALED`.

Sealed repair is an explicit plan/execute operation. It never formats an
unknown volume and never restores a previous product state. Product updates
that replace an installation use a new installation: install and verify the
new artifact, migrate product data inside the appliance, switch the consumer,
then revoke and remove the old installation.

The first-party appliance bundled in a signed Host release is different: the
embedded Guest-manifest digest verifies both `mcp-appliance/manifest.pb` and
`mcp-appliance.digest`; the manifest digest and exact
`agent-vmm.local/mcp@<digest>` reference must match the Guest lock. At Host
startup, one unique desired installation for that product may atomically move
to the bundled artifact and a stopped revision. The installation ID,
registration, approved grant, and encrypted volume do not change. Multiple
matching installations fail closed, while sealed, disabled, and pending-stop
installations remain unchanged. A desired missing or old-digest runtime is
stopped and started once through the normal artifact, grant, bootstrap, and
instance fences; a failed attempt enters `FAILED` instead of retrying without
a bound.

### Egress-grant schema preflight

An existing schema-17 Host database created before the independent
`egress_mode` column requires one explicit operator update before installing a
Host that reads the column. Stop the Host, take a restorable SQLite backup, and
audit every `appliance_grants` row against the durable installation evidence
that authorized its exact approved grant. The
actual egress mode must come from `ApprovedGrant.egress_mode`; never copy
`max_egress_mode`, which is only the allocation ceiling. Abort if the row count,
installation identity, authorized manifest, or actual mode is not known exactly.

For the bounded staging state where the audit proves every installed appliance
has actual `DENY_ALL` (`1`), the operator may run:

```sql
BEGIN IMMEDIATE;
ALTER TABLE appliance_grants
  ADD COLUMN egress_mode INTEGER NOT NULL DEFAULT 1
  CHECK(egress_mode IN (1, 2));
UPDATE appliance_grants SET egress_mode = 1;
SELECT installation_id, egress_mode, max_egress_mode FROM appliance_grants;
COMMIT;
```

The final `SELECT` must match the audited row set before the new Host starts.
New databases create the non-defaulted, non-null column directly. Agent VMM has
no legacy schema reader, inferred value, or runtime backfill.

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
