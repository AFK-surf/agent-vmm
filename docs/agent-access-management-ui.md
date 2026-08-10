# Agent access management UI

- Status: Approved and implemented through delivery phase 5
- Date: 2026-08-10
- Scope: macOS app management of the local MCP adapter and the remote controller connector

## Decision

Add an **Agent Access** section to the main sidebar. It owns access setup, current connections, and access removal for both local MCP clients and the single supported remote controller. Move remote-controller management out of Settings, move operator socket and boot identity into Activity's support details, and remove the Settings section until the app has actual user preferences.

Keep Environments as the inventory of retained compute and data. Agent Access answers *who can use Agent VMM and whether they are connected*; Environments answers *what they created and what remains on disk*. An access removal must never silently remove an environment or its data.

## Current product findings

The app currently has Overview, Environments, Activity, and Settings. The existing remote controller form was added to Settings even though enrollment, connection state, capacity, drain, and revocation are operational access controls rather than preferences.

The package installs three different concerns together:

- `agent-vmm-host` is the persistent user LaunchAgent and VM control plane.
- `agent-vmm-mcp` is an on-demand stdio executable. “Install MCP” therefore means adding its command and stable `--integration` identity to an agent client's configuration, not installing another daemon.
- `agent-vmm-connect` is a background LaunchAgent. It is useful only after controller enrollment has placed an opaque credential in Keychain.

The existing UI and snapshot do not yet support a truthful connection-management page:

- Local sessions are persisted and revocable, but OperatorService cannot list them. A session also lacks an integration/client identity, connection timestamps, and live/idle state.
- The system snapshot contains the remote registration and allocations, but the Swift model drops allocation and lease details and retains only an active-lease count.
- Remote status contains registration state and connection state, but no connector health, last successful connection, current reconnect attempt, or actionable failure reason.
- Remote pool editing exposes only `max_environments`; the UI silently supplies fixed CPU, memory, PID, and disk limits instead of loading the current policy.
- `maxEnvironments` starts at `2` and is not initialized from an existing registration, so an unrelated update can overwrite capacity.
- Enrollment creates the host registration before the connector returns a credential. If enrollment fails, the user can be left with an active registration that cannot connect and blocks a clean retry.
- The installer and uninstaller are development scripts outside the signed application. The shipped app has no product API for installation health, repair, or access-safe removal.

## Information architecture

Use this top-level order:

1. **Overview** — VM state, resource summary, component health, and a compact access summary linking to Agent Access.
2. **Agent Access** — local agent integrations and the remote controller.
3. **Environments** — retained local and remote environments, workloads, data, policy, and cleanup.
4. **Activity** — operations, audit events, diagnostics, support export, and collapsed system details.

Agent Access has two tabs:

- **Local Agents** lists configured MCP client integrations and their current sessions.
- **Remote Controller** manages the single controller registration, transport state, capacity policy, allocations, and leases.

Do not add a permanent third tab for components. When the host service or connector needs setup or repair, show a page-level action banner above the affected access view. Put versions, paths, LaunchAgent labels, operator socket, boot ID, and raw failure details in Activity > System Details.

## Overview changes

Add one quiet **Agent access** row below the VM card and before resource metrics:

- `2 local agents ready · 1 in use`
- `Remote controller: Connected · Accepting work`

Each value is a link to its Agent Access tab. Show only conditions that change a decision. Do not duplicate allocations, leases, environment counts, or pool limits on Overview.

The Components section continues to report control-plane health. It should report the connector only when a controller is configured; an intentionally unconfigured connector is not degraded health.

## Local Agents

### Integration list

Each row represents a configuration managed by Agent VMM, not a process:

- agent/client name and icon;
- configuration scope, such as User or a specific workspace;
- assigned environment display name;
- integration state: **Ready**, **In use**, **Needs restart**, **Needs repair**, or **Config changed externally**;
- active session count and last connection time when known;
- actions: **Open environment**, **Manage**, and an overflow menu with **Repair configuration** and **Remove access**.

Idle on-demand MCP clients remain **Ready**, not Disconnected or Unhealthy. **In use** means at least one authenticated runtime session is live. A removed or quit client returns to Ready after its session closes.

### Add Agent flow

Use a sheet with three short steps:

1. **Choose agent** — show supported clients detected on the Mac, plus **Other MCP client**. Do not show unsupported products as disabled marketing tiles.
2. **Choose scope and environment** — select User or Workspace scope, set a readable environment name, and either create a new stable environment identity or intentionally reuse an existing local environment. Reuse requires an explicit warning because the clients will share data and policy.
3. **Review and add** — show the exact config target, command, arguments, and whether the client must restart. The primary action is **Add Agent VMM**.

After writing configuration atomically, show one of two results:

- **Ready — restart AgentName to connect** when live reload is unavailable.
- **Connected** after observing the first authenticated session.

For Other MCP client, provide **Copy configuration** and **Verify connection**. Keep the executable path and arguments visible. Never treat copying as proof of installation.

Configuration adapters must preserve unrelated client settings, create a recoverable backup, detect external edits before removal, and show a diff before overwriting a conflicting entry.

### Manage and remove

Manage shows the environment, config location, exact command, live sessions, created/last-used timestamps, and a link to environment limits and data.

**Disconnect sessions** revokes current session capabilities but leaves configuration installed; the agent may reconnect. **Remove access** removes only the Agent VMM config entry and revokes its sessions. Its confirmation says:

> AgentName will no longer be able to start Agent VMM sessions. The environment and retained data remain available in Environments.

Environment reset or discard stays exclusively in Environments > Advanced. Do not add a destructive “remove access and delete data” checkbox.

## Remote Controller

### Unconfigured state

Show one explanation and **Connect Controller…**. The enrollment sheet contains:

1. endpoint and controller display name;
2. one-time token, which is cleared from view as soon as submission begins;
3. capacity: maximum environments and per-environment limits, with additional aggregate, egress, lease, disconnect-grace, and stop-grace controls under **Advanced Capacity**;
4. a review page that states the outbound connection, Keychain storage, and maximum capacity exposed.

Submission shows determinate stages: **Creating registration**, **Storing credential**, and **Connecting**. Closing the sheet does not cancel after credential exchange begins. If credential exchange fails, the product must either roll back the new registration or retain a recoverable **Setup incomplete** state with **Retry enrollment** and **Remove registration**.

### Connected state

Keep policy and transport as separate facts:

- prominent controller identity and endpoint;
- **Connected / Connecting / Offline** transport status with last successful connection and an actionable failure reason;
- **Accepting new work / Draining / Revoked** admission state;
- capacity as `used / maximum environments`, with active lease count;
- allocations table with label, allocation state, lease state/expiry, environment link, and retained-data indicator.

The primary action follows the state: **Drain…** while accepting work, **Resume accepting work** while drained. **Edit Capacity…** is secondary. **Revoke Access…** stays in the overflow menu.

Drain confirmation distinguishes its effect: existing leases continue; no new leases are admitted. While draining, show progress such as `2 leases remaining` and allow resuming. Draining is an operator-selected state, not degraded health.

Revoke confirmation states that the connector disconnects, the Keychain credential is deleted, active remote sessions are revoked, and retained environments remain for local inspection. If retained remote environments exist, link to their filtered Environments view after revocation.

### Offline behavior

Show **Connecting** during the configured disconnect grace period. Show **Offline** only after the grace period or a terminal authentication/configuration error. Keep **Accepting new work** visible as desired policy even while transport is offline; do not merge the two into an ambiguous red status.

Offer actions based on cause: **Retry now**, **Repair connector**, **Retry enrollment**, or **View details**. Raw connector logs belong in Activity, not the access page.

## Environments changes

Keep one environment list. Add a Local/Remote source label and optional source filter only after both origins exist; do not create separate inventories. Remote environment Overview shows controller, allocation label/ID, allocation state, and lease expiry. Local environment Overview links back to the owning Agent Access integration when that relationship exists.

After an integration is removed or a lease is released, the environment remains and is labeled **No active access**. This makes retained data visible without implying that an agent is still connected.

## Activity and system details

Extend activity events with access lifecycle events: configuration added/removed/repaired, local session opened/revoked/closed, controller enrolled/connected/disconnected/drained/resumed/revoked, lease acquired/expired/released, and allocation retained/discarded.

Move the existing operator socket and VM boot ID out of Settings into a collapsed **System Details** section beside component versions and LaunchAgent state. Move the “no direct runtime socket” security explanation into Agent Access help and the enrollment review, where it informs an access decision.

## Status model

Do not reduce access to one health badge. Present these independent dimensions:

| Subject | Persistent setup | Desired policy | Live state |
| --- | --- | --- | --- |
| Local integration | Ready / needs restart / needs repair | Allowed / removed | Idle / session count |
| Remote controller | Unconfigured / enrolled / setup incomplete | Accepting / draining / revoked | Connecting / connected / offline |
| Environment | Retained / resetting / discarding | Limits and egress policy | Workload and usage health |

Use green only for verified live success, red only for an actionable failure, orange for attention, and neutral blue/gray for intentional or idle states. Pair every color with text and a symbol.

## Required product contracts

Before building the complete UI, add contracts that let it report facts instead of inferring from files:

1. A persisted `AgentIntegration` model: ID, adapter/client kind, display name, scope, config target, environment ID, desired state, config fingerprint, version, created/updated time, and last error.
2. Operator methods to list/create/update/remove integrations through product-owned adapters. Config writes remain in the signed macOS app/helper, not the guest.
3. A listable session projection: session ID, integration ID, environment ID, origin, created time, expiry, last activity, and live/revoked state. Extend CreateAgentSession with integration identity authenticated by the managed config, rather than accepting an arbitrary display label as authority.
4. Remote registration projection with the full pool policy, allocations, leases, last-connected time, reconnect phase, connector version/health, and sanitized failure reason.
5. An enrollment operation with explicit stages and rollback/retry semantics. Credential contents remain write-only and absent from snapshots, logs, and support bundles.
6. Installation-health and repair operations for the user LaunchAgents and signed helpers. Do not make the UI parse `launchctl` text as its long-term state API.
7. Activity events for all access state transitions and revision-safe mutations for integrations and remote policy.

The initial integration identity may map one managed client configuration to one local environment. Support multiple simultaneous sessions for that integration. Do not infer “Codex” or another client from a process name.

## Installation and product removal

Use precise labels:

- **Set Up Agent VMM Service** installs or repairs the background host service.
- **Add Agent VMM to AgentName** edits an MCP client configuration.
- **Connect Controller** enrolls the remote connector.
- **Remove access** removes one MCP integration.
- **Revoke controller access** revokes the remote credential.
- **Uninstall Agent VMM…** removes the product.

Product uninstall belongs in the Agent VMM application menu, not inside either access tab. Offer **Keep environments and data** by default and a separately confirmed **Delete all Agent VMM data** option. The action must stop services, revoke live sessions, remove LaunchAgents and the app, and clearly state that the application will close. A self-removing signed helper or installer owns this operation; the SwiftUI process must not delete its own bundle directly.

## Delivery sequence

1. Add backend projections for integrations, sessions, connector health, full remote policy, and recoverable enrollment.
2. Add Agent Access with read-only local and remote status plus deep links to Environments and Activity.
3. Add managed MCP client adapters and the Add/Repair/Remove flows, starting with explicitly supported clients and Manual setup.
4. Move remote enrollment and policy actions from Settings into Agent Access, then remove Settings and reorganize Activity system details.
5. Add service repair and product uninstall through a signed lifecycle helper.

Each phase must preserve the distinction between access and retained resources. UI tests should cover idle-ready local clients, concurrent sessions, external config edits, partial enrollment, reconnect grace, drain with active leases, revoke with retained environments, and uninstall with and without data removal.

The shipped lifecycle helper now reports signed-helper and LaunchAgent status, repairs both services from bundled definitions, and performs product uninstall with retained data kept by default. The host remains the authority for integrations, sessions, enrollment, admission, allocations, and leases; lifecycle operations remain usable when the host is unavailable.
