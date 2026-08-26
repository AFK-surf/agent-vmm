------------------------ MODULE HostServiceOwnership ------------------------
EXTENDS Naturals

CONSTANTS EnforceExclusiveOwner, EnforceAutoGuard

VARIABLES requestedMode, daemonAllowed, guiAvailable,
          agentDefined, daemonDefined, agentLoaded, daemonLoaded, outcome

vars == <<requestedMode, daemonAllowed, guiAvailable,
          agentDefined, daemonDefined, agentLoaded, daemonLoaded, outcome>>

Init ==
  /\ requestedMode \in {"agent", "daemon", "auto"}
  /\ daemonAllowed \in BOOLEAN
  /\ guiAvailable \in BOOLEAN
  /\ agentDefined = FALSE
  /\ daemonDefined = FALSE
  /\ agentLoaded = FALSE
  /\ daemonLoaded = FALSE
  /\ outcome = "pending"

SelectDaemon ==
  /\ outcome = "pending"
  /\ requestedMode = "daemon" \/ requestedMode = "auto"
  /\ daemonAllowed
  /\ daemonDefined' = TRUE
  /\ agentDefined' = IF EnforceExclusiveOwner THEN FALSE ELSE agentDefined
  /\ daemonLoaded' = TRUE
  /\ agentLoaded' = IF EnforceExclusiveOwner THEN FALSE ELSE agentLoaded
  /\ outcome' = "ready"
  /\ UNCHANGED <<requestedMode, daemonAllowed, guiAvailable>>

SelectAgent ==
  /\ outcome = "pending"
  /\ requestedMode = "agent" \/ (requestedMode = "auto" /\ ~daemonAllowed)
  /\ guiAvailable
  /\ agentDefined' = TRUE
  /\ daemonDefined' = IF EnforceExclusiveOwner THEN FALSE ELSE daemonDefined
  /\ agentLoaded' = TRUE
  /\ daemonLoaded' = IF EnforceExclusiveOwner THEN FALSE ELSE daemonLoaded
  /\ outcome' = "ready"
  /\ UNCHANGED <<requestedMode, daemonAllowed, guiAvailable>>

RejectUnavailable ==
  /\ outcome = "pending"
  /\ \/ /\ requestedMode = "daemon"
         /\ ~daemonAllowed
      \/ /\ requestedMode = "agent"
         /\ ~guiAvailable
      \/ /\ requestedMode = "auto"
         /\ ~daemonAllowed
         /\ ~guiAvailable
  /\ IF EnforceAutoGuard
        THEN /\ outcome' = "action_required"
             /\ UNCHANGED <<agentDefined, daemonDefined, agentLoaded, daemonLoaded>>
        ELSE /\ outcome' = "ready"
             /\ agentDefined' = TRUE
             /\ agentLoaded' = TRUE
             /\ UNCHANGED <<daemonDefined, daemonLoaded>>
  /\ UNCHANGED <<requestedMode, daemonAllowed, guiAvailable>>

RequestAgent ==
  /\ outcome # "pending"
  /\ requestedMode' = "agent"
  /\ guiAvailable' \in BOOLEAN
  /\ outcome' = "pending"
  /\ daemonLoaded' = IF EnforceExclusiveOwner THEN FALSE ELSE daemonLoaded
  /\ agentLoaded' = FALSE
  /\ UNCHANGED <<daemonAllowed, agentDefined, daemonDefined>>

RequestDaemon ==
  /\ outcome # "pending"
  /\ requestedMode' = "daemon"
  /\ daemonAllowed' \in BOOLEAN
  /\ guiAvailable' \in BOOLEAN
  /\ outcome' = "pending"
  /\ agentLoaded' = IF EnforceExclusiveOwner THEN FALSE ELSE agentLoaded
  /\ daemonLoaded' = FALSE
  /\ UNCHANGED <<agentDefined, daemonDefined>>

Settle == SelectDaemon \/ SelectAgent \/ RejectUnavailable
Next == Settle \/ RequestAgent \/ RequestDaemon

TypeOK ==
  /\ requestedMode \in {"agent", "daemon", "auto"}
  /\ daemonAllowed \in BOOLEAN
  /\ guiAvailable \in BOOLEAN
  /\ agentDefined \in BOOLEAN
  /\ daemonDefined \in BOOLEAN
  /\ agentLoaded \in BOOLEAN
  /\ daemonLoaded \in BOOLEAN
  /\ outcome \in {"pending", "ready", "action_required"}

SingleServiceOwner == ~(agentLoaded /\ daemonLoaded)
SingleServiceDefinition == ~(agentDefined /\ daemonDefined)
LoadedHasDefinition == (agentLoaded => agentDefined) /\ (daemonLoaded => daemonDefined)
ReadyHasRealOwner == outcome = "ready" => agentLoaded \/ daemonLoaded
AgentRequiresGUI == agentLoaded => guiAvailable
RequestedEventuallySettles == [] (outcome = "pending" => <> (outcome # "pending"))

Spec == Init /\ [][Next]_vars /\ WF_vars(Settle)

=============================================================================
