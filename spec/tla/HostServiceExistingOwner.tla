---------------------- MODULE HostServiceExistingOwner ----------------------
EXTENDS Naturals

CONSTANTS RespectConfiguredOwner, PreserveConfiguredResources

\* Existing-owner observation/removal is deliberately separate from the
\* first-install auto selection in HostServiceOwnership.tla.
\* Production anchor: cmd/agent-vmm-lifecycle.selectServiceModeForAction.

VARIABLES configuredOwner, configuredGeometry, action, agentLoaded, daemonLoaded,
          observedOwner, renderedGeometry, outcome

vars == <<configuredOwner, configuredGeometry, action, agentLoaded, daemonLoaded,
          observedOwner, renderedGeometry, outcome>>

Other(owner) == IF owner = "agent" THEN "daemon" ELSE "agent"

Init ==
  /\ configuredOwner \in {"agent", "daemon"}
  /\ configuredGeometry = "custom"
  /\ action \in {"observe", "remove", "update"}
  /\ agentLoaded = (configuredOwner = "agent")
  /\ daemonLoaded = (configuredOwner = "daemon")
  /\ observedOwner = "none"
  /\ renderedGeometry = "none"
  /\ outcome = "pending"

Observe ==
  /\ outcome = "pending"
  /\ action = "observe"
  /\ observedOwner' = IF RespectConfiguredOwner
                        THEN configuredOwner
                        ELSE Other(configuredOwner)
  /\ outcome' = "observed"
  /\ UNCHANGED <<configuredOwner, configuredGeometry, action, agentLoaded, daemonLoaded,
                  renderedGeometry>>

Remove ==
  /\ outcome = "pending"
  /\ action = "remove"
  /\ IF RespectConfiguredOwner
        THEN /\ agentLoaded' = IF configuredOwner = "agent" THEN FALSE ELSE agentLoaded
             /\ daemonLoaded' = IF configuredOwner = "daemon" THEN FALSE ELSE daemonLoaded
        ELSE /\ agentLoaded' = IF configuredOwner = "daemon" THEN FALSE ELSE agentLoaded
             /\ daemonLoaded' = IF configuredOwner = "agent" THEN FALSE ELSE daemonLoaded
  /\ outcome' = "removed"
  /\ UNCHANGED <<configuredOwner, configuredGeometry, action, observedOwner,
                  renderedGeometry>>

Update ==
  /\ outcome = "pending"
  /\ action = "update"
  /\ renderedGeometry' = IF PreserveConfiguredResources
                            THEN configuredGeometry
                            ELSE "default"
  /\ outcome' = "updated"
  /\ UNCHANGED <<configuredOwner, configuredGeometry, action, agentLoaded,
                  daemonLoaded, observedOwner>>

Next == Observe \/ Remove \/ Update

TypeOK ==
  /\ configuredOwner \in {"agent", "daemon"}
  /\ configuredGeometry \in {"custom"}
  /\ action \in {"observe", "remove", "update"}
  /\ agentLoaded \in BOOLEAN
  /\ daemonLoaded \in BOOLEAN
  /\ observedOwner \in {"none", "agent", "daemon"}
  /\ renderedGeometry \in {"none", "custom", "default"}
  /\ outcome \in {"pending", "observed", "removed", "updated"}

ObservedConfiguredOwner == outcome = "observed" => observedOwner = configuredOwner
RemovedConfiguredOwner ==
  outcome = "removed" =>
    IF configuredOwner = "agent" THEN ~agentLoaded ELSE ~daemonLoaded
UpdatedResourceGeometry ==
  outcome = "updated" => renderedGeometry = configuredGeometry

Spec == Init /\ [][Next]_vars

=============================================================================
