---------------------- MODULE HostServiceExistingOwner ----------------------
EXTENDS Naturals

CONSTANTS RespectConfiguredOwner, UseFreshBundleResources,
          PreserveConfiguredPolicy

\* Existing-owner observation/removal is deliberately separate from the
\* first-install auto selection in HostServiceOwnership.tla.
\* Production anchor: cmd/agent-vmm-lifecycle.selectServiceModeForAction.

VARIABLES configuredOwner, configuredGeometry, configuredPolicy, action,
          agentLoaded, daemonLoaded, observedOwner, renderedGeometry,
          renderedPolicy, outcome

vars == <<configuredOwner, configuredGeometry, configuredPolicy, action,
          agentLoaded, daemonLoaded, observedOwner, renderedGeometry,
          renderedPolicy, outcome>>

Other(owner) == IF owner = "agent" THEN "daemon" ELSE "agent"

Init ==
  /\ configuredOwner \in {"agent", "daemon"}
  /\ configuredGeometry = "custom"
  /\ configuredPolicy = "restricted"
  /\ action \in {"observe", "remove", "update"}
  /\ agentLoaded = (configuredOwner = "agent")
  /\ daemonLoaded = (configuredOwner = "daemon")
  /\ observedOwner = "none"
  /\ renderedGeometry = "none"
  /\ renderedPolicy = "none"
  /\ outcome = "pending"

Observe ==
  /\ outcome = "pending"
  /\ action = "observe"
  /\ observedOwner' = IF RespectConfiguredOwner
                        THEN configuredOwner
                        ELSE Other(configuredOwner)
  /\ outcome' = "observed"
  /\ UNCHANGED <<configuredOwner, configuredGeometry, configuredPolicy, action,
                  agentLoaded, daemonLoaded, renderedGeometry, renderedPolicy>>

Remove ==
  /\ outcome = "pending"
  /\ action = "remove"
  /\ IF RespectConfiguredOwner
        THEN /\ agentLoaded' = IF configuredOwner = "agent" THEN FALSE ELSE agentLoaded
             /\ daemonLoaded' = IF configuredOwner = "daemon" THEN FALSE ELSE daemonLoaded
        ELSE /\ agentLoaded' = IF configuredOwner = "daemon" THEN FALSE ELSE agentLoaded
             /\ daemonLoaded' = IF configuredOwner = "agent" THEN FALSE ELSE daemonLoaded
  /\ outcome' = "removed"
  /\ UNCHANGED <<configuredOwner, configuredGeometry, configuredPolicy, action,
                  observedOwner, renderedGeometry, renderedPolicy>>

Update ==
  /\ outcome = "pending"
  /\ action = "update"
  /\ renderedGeometry' = IF UseFreshBundleResources
                            THEN "default"
                            ELSE configuredGeometry
  /\ renderedPolicy' = IF PreserveConfiguredPolicy
                          THEN configuredPolicy
                          ELSE "default"
  /\ outcome' = "updated"
  /\ UNCHANGED <<configuredOwner, configuredGeometry, configuredPolicy, action,
                  agentLoaded, daemonLoaded, observedOwner>>

Next == Observe \/ Remove \/ Update

TypeOK ==
  /\ configuredOwner \in {"agent", "daemon"}
  /\ configuredGeometry \in {"custom"}
  /\ configuredPolicy \in {"restricted"}
  /\ action \in {"observe", "remove", "update"}
  /\ agentLoaded \in BOOLEAN
  /\ daemonLoaded \in BOOLEAN
  /\ observedOwner \in {"none", "agent", "daemon"}
  /\ renderedGeometry \in {"none", "custom", "default"}
  /\ renderedPolicy \in {"none", "restricted", "default"}
  /\ outcome \in {"pending", "observed", "removed", "updated"}

ObservedConfiguredOwner == outcome = "observed" => observedOwner = configuredOwner
RemovedConfiguredOwner ==
  outcome = "removed" =>
    IF configuredOwner = "agent" THEN ~agentLoaded ELSE ~daemonLoaded
UpdatedResourceGeometry ==
  outcome = "updated" => renderedGeometry = "default"
UpdatedPairingPolicy ==
  outcome = "updated" => renderedPolicy = configuredPolicy

Spec == Init /\ [][Next]_vars

=============================================================================
