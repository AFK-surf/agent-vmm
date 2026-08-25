--------------------- MODULE ApplianceActivationLifecycle --------------------
EXTENDS Naturals

CONSTANTS EnforceVerification, EnforceSingleUse, EnforceInstanceFence,
          EnforceSeedDeletionFence, EnforcePublicationFence,
          EnforceSealedRecovery, EnforceRemoveRevision

VARIABLES state, violations
vars == <<state, violations>>
NextCounter(value) == IF value = 1 THEN 2 ELSE 1
Phases == {"STAGED", "INSTALLED", "STOPPED", "STARTING", "BOOTSTRAPPING",
  "ACTIVE", "REVOKING", "REVOKED", "SEALED", "PENDING_DESTROY", "REMOVED"}

Init ==
  /\ state = [phase |-> "STAGED", artifact |-> 0, verified |-> 0,
       boot |-> 0, container |-> 0, bootstrap |-> 0, receipt |-> 0,
       health |-> 0, session |-> 0, frontend |-> 0,
       bootstrapReceipt |-> FALSE, replacementPending |-> FALSE,
       seedConsumed |-> FALSE, seedPresent |-> FALSE, seedIssued |-> FALSE,
       seedFenceCommitted |-> FALSE, fenceCommitted |-> FALSE, published |-> FALSE,
       operationPhase |-> "IDLE", revision |-> 1, plannedRevision |-> 0,
       repairPending |-> FALSE, sealedUsed |-> FALSE]
  /\ violations = {}

StageArtifact ==
  /\ state.phase = "STAGED"
  /\ state' = [state EXCEPT !.artifact = 1,
       !.verified = IF EnforceVerification THEN 1 ELSE 0, !.phase = "INSTALLED"]
  /\ UNCHANGED violations

Install ==
  /\ state.phase = "INSTALLED"
  /\ state' = [state EXCEPT !.phase = "STOPPED", !.seedPresent = TRUE,
       !.seedIssued = TRUE]
  /\ UNCHANGED violations

Start ==
  /\ state.phase = "STOPPED"
  /\ state.operationPhase # "RUNNING"
  /\ IF EnforceVerification THEN state.verified = state.artifact ELSE TRUE
  /\ state' = [state EXCEPT !.phase = "STARTING", !.session = 0,
       !.frontend = 0, !.bootstrap = 0, !.receipt = 0, !.health = 0,
       !.bootstrapReceipt = FALSE, !.boot = NextCounter(state.boot),
       !.container = NextCounter(state.container), !.replacementPending = FALSE,
       !.fenceCommitted = FALSE, !.published = FALSE, !.operationPhase = "RUNNING"]
  /\ violations' = IF EnforceVerification /\ state.verified = state.artifact
                    THEN violations ELSE violations \cup {"UNVERIFIED_START"}

CommitInstanceFence ==
  /\ state.phase = "STARTING" /\ ~state.fenceCommitted
  /\ state' = [state EXCEPT !.fenceCommitted = TRUE,
       !.seedFenceCommitted = IF ~state.seedConsumed THEN TRUE ELSE state.seedFenceCommitted]
  /\ UNCHANGED violations

BootstrapInitial ==
  /\ state.phase = "STARTING" /\ state.bootstrap = 0 /\ state.fenceCommitted
  /\ IF EnforceSingleUse THEN ~state.seedConsumed ELSE TRUE
  /\ state.seedPresent
  /\ state' = [state EXCEPT !.phase = "BOOTSTRAPPING", !.bootstrap = 1,
       !.seedConsumed = TRUE, !.bootstrapReceipt = TRUE, !.receipt = state.container]
  /\ UNCHANGED violations

Bootstrap ==
  /\ state.phase = "STARTING" /\ state.bootstrap = 0 /\ state.fenceCommitted
  /\ IF EnforceSingleUse THEN state.seedConsumed ELSE TRUE
  /\ state' = [state EXCEPT !.phase = "BOOTSTRAPPING", !.bootstrap = 1,
       !.bootstrapReceipt = TRUE, !.receipt = state.container]
  /\ UNCHANGED violations

ReplayBootstrap ==
  /\ ~EnforceSingleUse /\ state.phase = "BOOTSTRAPPING" /\ state.bootstrap > 0
  /\ state' = [state EXCEPT !.bootstrap = NextCounter(state.bootstrap)]
  /\ violations' = violations \cup {"BOOTSTRAP_REPLAY"}

DeleteSeed ==
  /\ state.phase = "BOOTSTRAPPING" /\ state.fenceCommitted
  /\ state.seedConsumed /\ state.bootstrapReceipt /\ state.seedPresent
  /\ state' = [state EXCEPT !.seedPresent = FALSE]
  /\ UNCHANGED violations

Settle ==
  /\ state.phase = "BOOTSTRAPPING" /\ ~state.replacementPending
  /\ state.fenceCommitted /\ ~state.seedPresent
  /\ state' = [state EXCEPT !.phase = "ACTIVE", !.receipt = state.container,
       !.health = state.container, !.session = state.container,
       !.frontend = state.container, !.published = TRUE, !.operationPhase = "IDLE"]
  /\ UNCHANGED violations

Stop ==
  /\ state.phase = "ACTIVE"
  /\ state' = [state EXCEPT !.phase = "STOPPED", !.session = 0,
       !.frontend = 0, !.published = FALSE, !.operationPhase = "IDLE"]
  /\ UNCHANGED violations

Restart ==
  /\ state.phase = "ACTIVE"
  /\ state.operationPhase # "RUNNING"
  /\ state' = [state EXCEPT !.phase = "STARTING", !.session = 0,
       !.frontend = 0, !.bootstrap = 0, !.receipt = 0, !.health = 0,
       !.bootstrapReceipt = FALSE, !.boot = NextCounter(state.boot),
       !.container = NextCounter(state.container), !.replacementPending = FALSE,
       !.fenceCommitted = FALSE, !.published = FALSE, !.operationPhase = "RUNNING"]
  /\ UNCHANGED violations

Crash ==
  /\ state.phase \in {"ACTIVE", "BOOTSTRAPPING", "STARTING"}
  /\ state' = [state EXCEPT !.phase = "STOPPED", !.session = 0,
       !.frontend = 0, !.replacementPending = FALSE, !.published = FALSE]
  /\ UNCHANGED violations

PayloadCrash ==
  /\ state.phase = "ACTIVE"
  /\ state' = [state EXCEPT !.phase = "STARTING",
       !.container = NextCounter(state.container), !.bootstrap = 0,
       !.receipt = 0, !.health = 0, !.session = 0, !.frontend = 0,
       !.replacementPending = TRUE, !.fenceCommitted = FALSE, !.published = FALSE]
  /\ UNCHANGED violations

AdoptReplacement ==
  /\ state.phase = "BOOTSTRAPPING" /\ state.replacementPending
  /\ state.fenceCommitted /\ ~state.seedPresent
  /\ state' = [state EXCEPT !.phase = "ACTIVE", !.receipt = state.container,
       !.health = state.container, !.session = state.container,
       !.frontend = state.container, !.replacementPending = FALSE, !.published = TRUE]
  /\ UNCHANGED violations

StaleInstance ==
  /\ ~EnforceInstanceFence /\ state.phase = "ACTIVE"
  /\ state' = [state EXCEPT !.health = state.container - 1]
  /\ violations' = violations \cup {"STALE_INSTANCE"}

StaleSession ==
  /\ ~EnforceInstanceFence /\ state.phase = "ACTIVE"
  /\ state' = [state EXCEPT !.session = state.container - 1]
  /\ violations' = violations \cup {"STALE_SESSION"}

StaleFrontend ==
  /\ ~EnforceInstanceFence /\ state.phase = "ACTIVE"
  /\ state' = [state EXCEPT !.frontend = state.container - 1]
  /\ violations' = violations \cup {"STALE_FRONTEND"}

UnsafePublishBeforeFence ==
  /\ ~EnforcePublicationFence /\ state.phase = "STARTING" /\ ~state.fenceCommitted
  /\ state' = [state EXCEPT !.phase = "ACTIVE", !.published = TRUE,
       !.receipt = state.container, !.health = state.container,
       !.session = state.container, !.frontend = state.container]
  /\ violations' = violations \cup {"PUBLISH_BEFORE_FENCE"}

UnsafeConsumeSeed ==
  /\ ~EnforceSeedDeletionFence /\ state.phase = "STARTING"
  /\ state.seedIssued /\ state.seedPresent /\ ~state.fenceCommitted
  /\ state' = [state EXCEPT !.seedPresent = FALSE]
  /\ violations' = violations \cup {"SEED_CONSUMED_BEFORE_FENCE"}

Seal ==
  /\ state.phase \in {"STARTING", "BOOTSTRAPPING", "ACTIVE"}
  /\ state' = [state EXCEPT !.phase = "SEALED", !.replacementPending = FALSE,
       !.session = 0, !.frontend = 0, !.published = FALSE,
       !.sealedUsed = TRUE, !.repairPending = FALSE]
  /\ UNCHANGED violations

Revoke ==
  /\ state.phase \in {"ACTIVE", "STOPPED", "SEALED"}
  /\ state.operationPhase # "RUNNING"
  /\ state' = [state EXCEPT
       !.phase = IF state.phase = "SEALED" THEN "SEALED" ELSE "REVOKING",
       !.session = 0, !.frontend = 0, !.published = FALSE,
       !.revision = NextCounter(state.revision),
       !.operationPhase = IF state.phase = "SEALED" THEN "IDLE" ELSE "RUNNING"]
  /\ UNCHANGED violations

CompleteRevoke ==
  /\ state.phase = "REVOKING"
  /\ state' = [state EXCEPT !.phase = "REVOKED", !.operationPhase = "IDLE"]
  /\ UNCHANGED violations

PlanSealedRepair ==
  /\ state.phase = "SEALED" /\ EnforceSealedRecovery /\ ~state.repairPending
  /\ state.operationPhase # "RUNNING"
  /\ state' = [state EXCEPT !.repairPending = TRUE, !.operationPhase = "RUNNING"]
  /\ UNCHANGED violations

RecoverSealed ==
  /\ state.phase = "SEALED" /\ EnforceSealedRecovery /\ state.repairPending
  /\ state' = [state EXCEPT !.phase = "STOPPED", !.repairPending = FALSE,
       !.sealedUsed = FALSE, !.revision = NextCounter(state.revision),
       !.operationPhase = "IDLE"]
  /\ UNCHANGED violations

RepairCrash ==
  /\ state.phase = "SEALED" /\ state.repairPending
  /\ state' = [state EXCEPT !.repairPending = FALSE, !.operationPhase = "IDLE"]
  /\ UNCHANGED violations

UnsafeReopenSealed ==
  /\ ~EnforceSealedRecovery /\ state.phase = "SEALED"
  /\ state' = [state EXCEPT !.phase = "ACTIVE"]
  /\ violations' = violations \cup {"SEALED_RECOVERY_BYPASS"}

PlanRemove ==
  /\ state.phase \in {"ACTIVE", "STOPPED", "REVOKED", "SEALED"}
  /\ state.operationPhase # "RUNNING"
  /\ state' = [state EXCEPT !.phase = "PENDING_DESTROY",
       !.plannedRevision = NextCounter(state.revision),
       !.revision = NextCounter(state.revision), !.operationPhase = "RUNNING",
       !.session = 0, !.frontend = 0, !.published = FALSE]
  /\ UNCHANGED violations

ExecuteRemove ==
  /\ state.phase = "PENDING_DESTROY"
  /\ IF EnforceRemoveRevision THEN state.plannedRevision = state.revision ELSE TRUE
  /\ state' = [state EXCEPT !.phase = "REMOVED",
       !.revision = IF EnforceRemoveRevision THEN state.revision ELSE NextCounter(state.revision),
       !.operationPhase = "IDLE"]
  /\ violations' = IF EnforceRemoveRevision THEN violations
                    ELSE violations \cup {"REMOVE_REVISION_MISMATCH"}

RecoverOperation ==
  /\ state.operationPhase = "RUNNING"
  /\ state.phase \in {"STOPPED", "REVOKING", "REVOKED", "SEALED", "PENDING_DESTROY"}
  /\ state' = [state EXCEPT !.operationPhase = "INTERRUPTED"]
  /\ UNCHANGED violations

Next == StageArtifact \/ Install \/ Start \/ CommitInstanceFence \/
  BootstrapInitial \/ Bootstrap \/ ReplayBootstrap \/ DeleteSeed \/ Settle \/
  Stop \/ Restart \/ Crash \/ PayloadCrash \/ AdoptReplacement \/
  StaleInstance \/ StaleSession \/ StaleFrontend \/ UnsafePublishBeforeFence \/
  UnsafeConsumeSeed \/ Seal \/ Revoke \/ CompleteRevoke \/ PlanSealedRepair \/
  RecoverSealed \/ RepairCrash \/ UnsafeReopenSealed \/ PlanRemove \/
  ExecuteRemove \/ RecoverOperation

TypeOK ==
  /\ state.phase \in Phases
  /\ state.artifact \in 0..1 /\ state.verified \in 0..1
  /\ state.boot \in 0..2 /\ state.container \in 0..2
  /\ state.bootstrap \in 0..2 /\ state.receipt \in 0..2 /\ state.health \in 0..2
  /\ state.session \in 0..2 /\ state.frontend \in 0..2
  /\ state.bootstrapReceipt \in BOOLEAN /\ state.replacementPending \in BOOLEAN
  /\ state.seedConsumed \in BOOLEAN /\ state.seedPresent \in BOOLEAN
  /\ state.seedIssued \in BOOLEAN /\ state.seedFenceCommitted \in BOOLEAN
  /\ state.fenceCommitted \in BOOLEAN /\ state.published \in BOOLEAN
  /\ state.operationPhase \in {"IDLE", "RUNNING", "INTERRUPTED"}
  /\ state.revision \in 1..2 /\ state.plannedRevision \in 0..2
  /\ state.repairPending \in BOOLEAN /\ state.sealedUsed \in BOOLEAN

ActiveVerifiedExact == state.phase = "ACTIVE" => state.verified = state.artifact
BootstrapSingleUse == state.bootstrap <= 1
BootstrapSeedSingleUse == state.bootstrap > 0 => state.seedConsumed
ReplacementFence == state.replacementPending => state.session = 0 /\ state.frontend = 0
CurrentInstanceFence == state.phase = "ACTIVE" =>
  state.receipt = state.container /\ state.health = state.container /\
  state.session = state.container /\ state.frontend = state.container
PublicationFence == state.phase = "ACTIVE" => state.fenceCommitted /\ state.published
SeedConsumptionFence == state.phase = "BOOTSTRAPPING" /\ state.seedConsumed => state.fenceCommitted
InitialSeedDeletionFence == state.seedIssued /\ ~state.seedPresent => state.seedFenceCommitted
SealedRecoveryIsBounded == state.sealedUsed =>
  state.phase \in {"SEALED", "PENDING_DESTROY", "REMOVED"}
RemoveRevisionExact == state.phase = "REMOVED" => state.plannedRevision = state.revision
NoActiveRuntimeDuringOperation == state.operationPhase = "RUNNING" =>
  state.phase # "ACTIVE" /\ ~state.published

BootstrapEventuallySettles ==
  [] (state.phase = "BOOTSTRAPPING" => <> (state.phase \in {"ACTIVE", "STOPPED", "SEALED"}))
ReplacementEventuallySettles == [] (state.replacementPending => <> ~state.replacementPending)
InstallEventuallySettles == [] (state.phase = "INSTALLED" => <> (state.phase # "INSTALLED"))
RemoveEventuallyTerminates == [] (state.phase = "PENDING_DESTROY" => <> (state.phase = "REMOVED"))
OperationEventuallySettles == [] (state.operationPhase = "RUNNING" =>
  <> (state.operationPhase \in {"IDLE", "INTERRUPTED"}))
RevokeEventuallySettles == [] (state.phase = "REVOKING" => <> (state.phase = "REVOKED"))

Spec == Init /\ [][Next]_vars /\ WF_vars(Install) /\ WF_vars(BootstrapInitial) /\
  WF_vars(Bootstrap) /\ WF_vars(CommitInstanceFence) /\ WF_vars(DeleteSeed) /\
  WF_vars(Settle) /\ WF_vars(AdoptReplacement) /\ WF_vars(CompleteRevoke) /\
  WF_vars(ExecuteRemove) /\ WF_vars(RecoverOperation) /\ WF_vars(RecoverSealed) /\
  WF_vars(PlanSealedRepair) /\ WF_vars(RepairCrash)

=============================================================================
