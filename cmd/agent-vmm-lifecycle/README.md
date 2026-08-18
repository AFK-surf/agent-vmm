# Local lifecycle coordinator

`LifecycleCoordinator` is the bounded, provider-neutral seam for the local
Agent VMM lifecycle. It does not persist operations, retry in the background,
or compete with `runLifecycleOperation`, which remains the durable operation
owner.

The composite contracts are:

| Action | Delegation order | Completion rule |
| --- | --- | --- |
| `install` | `install`, `init`, `appliance-install`, `bootstrap`, `register`, `reconnect` | Every step settles and `Observe` reports `SalixReady` |
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

Fresh appliance installation requires `AGENT_VMM_APPLIANCE_CONFIG` to point to
an operator-owned JSON file containing absolute paths for the signed manifest,
publisher signature, approved grant, and opaque bootstrap seed, plus the
installation id. Existing installations may omit the config when their single
local appliance can be identified unambiguously. Missing configuration fails
closed; the helper never reports readiness or fabricates an install result.

The BFT `compute-node install` command invokes this same helper, so it reaches
the same coordinator and durable operation record rather than maintaining a
second lifecycle state machine.

`SalixReady` is an observed control-plane fact. Host health, appliance health,
controller activity, and inventory settlement are reported alongside it for
diagnostics but are never combined by this coordinator to manufacture
readiness.

The implementation anchors to `spec/tla/ApplianceLifecycle.tla` for install,
bootstrap, current-instance fencing, stop, and revision-fenced remove, and to
`spec/tla/ControllerDomainControl.tla` for ready, inventory, epoch, and
reconnect facts. The models and their checked configurations are unchanged by
this seam; the mapping is intentionally limited to orchestration ordering and
bounded delegation.
