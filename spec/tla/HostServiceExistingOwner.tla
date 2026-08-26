---------------------- MODULE HostServiceExistingOwner ----------------------
EXTENDS Naturals

CONSTANT RespectConfiguredOwner

\* Existing-owner observation/removal is deliberately separate from the
\* first-install auto selection in HostServiceOwnership.tla.
\* Production anchor: cmd/agent-vmm-lifecycle.selectServiceModeForAction.

VARIABLES configuredOwner, action, agentLoaded, daemonLoaded,
          observedOwner, outcome

vars == <<configuredOwner, action, agentLoaded, daemonLoaded,
          observedOwner, outcome>>

Other(owner) == IF owner = "agent" THEN "daemon" ELSE "agent"

Init ==
  /\ configuredOwner \in {"agent", "daemon"}
  /\ action \in {"observe", "remove"}
  /\ agentLoaded = (configuredOwner = "agent")
  /\ daemonLoaded = (configuredOwner = "daemon")
  /\ observedOwner = "none"
  /\ outcome = "pending"

Observe ==
  /\ outcome = "pending"
  /\ action = "observe"
  /\ observedOwner' = IF RespectConfiguredOwner
                        THEN configuredOwner
                        ELSE Other(configuredOwner)
  /\ outcome' = "observed"
  /\ UNCHANGED <<configuredOwner, action, agentLoaded, daemonLoaded>>

Remove ==
  /\ outcome = "pending"
  /\ action = "remove"
  /\ IF RespectConfiguredOwner
        THEN /\ agentLoaded' = IF configuredOwner = "agent" THEN FALSE ELSE agentLoaded
             /\ daemonLoaded' = IF configuredOwner = "daemon" THEN FALSE ELSE daemonLoaded
        ELSE /\ agentLoaded' = IF configuredOwner = "daemon" THEN FALSE ELSE agentLoaded
             /\ daemonLoaded' = IF configuredOwner = "agent" THEN FALSE ELSE daemonLoaded
  /\ outcome' = "removed"
  /\ UNCHANGED <<configuredOwner, action, observedOwner>>

Next == Observe \/ Remove

TypeOK ==
  /\ configuredOwner \in {"agent", "daemon"}
  /\ action \in {"observe", "remove"}
  /\ agentLoaded \in BOOLEAN
  /\ daemonLoaded \in BOOLEAN
  /\ observedOwner \in {"none", "agent", "daemon"}
  /\ outcome \in {"pending", "observed", "removed"}

ObservedConfiguredOwner == outcome = "observed" => observedOwner = configuredOwner
RemovedConfiguredOwner ==
  outcome = "removed" =>
    IF configuredOwner = "agent" THEN ~agentLoaded ELSE ~daemonLoaded

Spec == Init /\ [][Next]_vars

=============================================================================
