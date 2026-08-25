# Local lifecycle coordinator

`LifecycleCoordinator` is the bounded, provider-neutral seam for the local
Agent VMM lifecycle. It does not persist operations, retry in the background,
or compete with `runLifecycleOperation`, which remains the durable operation
owner.

The composite contracts are:

| Action | Delegation order | Completion rule |
| --- | --- | --- |
| `install` | `install`, `init`, `appliance-install`, `bootstrap`, `register`, `reconnect` | Every step settles and `Observe` reports the exact managed registration attached |
| `init` | `init` | The adapter settles the local initialization |
| `appliance-install` | `appliance-install` | The adapter settles appliance installation |
| `bootstrap` | `bootstrap` | The adapter settles bootstrap |
| `register` | `register` | The adapter settles registration |
| `reconnect` | `reconnect` | The adapter settles reconnection |
| `stop` | `stop` | The adapter settles stop |
| `remove` | `stop`, `remove` | Stop completes before local product removal |

Each action has an operation deadline and each delegated call has a step
deadline. The adapter must honor the supplied context. The helper adapter
reuses the existing host repair, VM start, drain, and uninstall functions;
provider-specific appliance/controller hooks are required and fail closed when
absent.

The top-level `install` command constructs `LifecycleCoordinator` with the
production local-operator adapter. It owns the durable operation record and
delegates `install`, `init`, appliance installation, bootstrap, registration,
reconnect, and final observation through that one coordinator. The narrower
`repair` command remains host-service repair plus VM start.

Fresh appliance installation consumes a bounded, one-use JSON enrollment ticket
on stdin with `install --ticket-stdin --request-id <opaque-id>`. The helper
materializes the signed manifest, publisher signature, approved grant, and
opaque bootstrap seed into a mode-0600 temporary workspace and removes it after
the operation. Ticket material never appears in argv, logs, or the operation
record. Existing installations may omit the ticket when their single local
appliance can be identified unambiguously. Missing ticket/configuration fails
closed; the helper never reports readiness or fabricates an install result.

There is no environment-based product configuration path. The only config
path is the helper-owned temporary workspace created from the ticket.

The BFT `compute-node install` command invokes this same helper, so it reaches
the same coordinator and durable operation record rather than maintaining a
second lifecycle state machine.

The helper only accepts `Agent VMM Host.app` with the fixed `Contents/MacOS`
host executable, the two approved `Contents/Helpers` binaries, the Guest
payload, and the embedded release inventory. The bundle validator also rejects
SwiftUI/window/menu/Dock metadata and debug executables before lifecycle paths
are resolved.

The helper reports local Host/appliance diagnostics and whether the exact
managed registration is attached. It never projects Salix admission or product
Node readiness; the server owns those facts from current observation.

The implementation anchors to `spec/tla/ApplianceActivationLifecycle.tla` for
install, bootstrap, current-instance fencing, stop, revoke, sealed recovery,
and revision-fenced remove. Ready, inventory, epoch, and reconnect facts are
owned by `spec/tla/ControllerAllocationLifecycle.tla`; cross-domain accounting
and isolation stay in the smaller models documented in `spec/tla/README.md`.
The mapping is intentionally limited to orchestration ordering and bounded delegation.
