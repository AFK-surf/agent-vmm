------------------------- MODULE ApplianceLifecycle -------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Installations, EnforceVerification, EnforceSingleUse,
          EnforceInstanceFence, EnforceIsolation, EnforceSealedRecovery,
          EnforceRemoveRevision, EnforceSeedDeletionFence,
          EnforcePublicationFence

VARIABLES state, violations
vars == <<state, violations>>
Other(i) == CHOOSE j \in Installations: j # i
NextCounter(value) == IF value = 1 THEN 2 ELSE 1
UnsafeNextCounter(value) == IF value = 1 THEN 2 ELSE 1
Phases == {"STAGED", "INSTALLED", "STARTING", "BOOTSTRAPPING", "ACTIVE", "STOPPED", "REVOKING", "REVOKED", "SEALED", "PENDING_DESTROY", "REMOVED"}

InitialRecord == [phase |-> "STAGED", artifact |-> 0, verified |-> 0,
  boot |-> 0, container |-> 0, bootstrap |-> 0, receipt |-> 0, health |-> 0,
  bootstrapReceipt |-> FALSE,
  operationPhase |-> "IDLE",
  session |-> 0, frontend |-> 0, revision |-> 1, plannedRevision |-> 0,
  sealedUsed |-> FALSE, seedConsumed |-> FALSE, replacementPending |-> FALSE, repairPending |-> FALSE,
  seedPresent |-> FALSE, seedIssued |-> FALSE, seedFenceCommitted |-> FALSE,
  fenceCommitted |-> FALSE, published |-> FALSE,
  removed |-> FALSE, removeOwner |-> "NONE"]

Init == /\ state = [i \in Installations |-> InitialRecord]
        /\ violations = {}

StageArtifact(i) ==
  /\ state[i].phase = "STAGED"
  /\ state' = [state EXCEPT ![i].artifact = 1, ![i].verified = IF EnforceVerification THEN 1 ELSE 0, ![i].phase = "INSTALLED"]
  /\ UNCHANGED violations

Install(i) ==
  /\ state[i].phase = "INSTALLED"
  /\ state' = [state EXCEPT ![i].phase = "STOPPED", ![i].seedPresent = TRUE,
        ![i].seedIssued = TRUE, ![i].revision = NextCounter(state[i].revision)]
  /\ UNCHANGED violations

Start(i) ==
  /\ state[i].phase = "STOPPED"
  /\ state[i].operationPhase # "RUNNING"
  /\ IF EnforceVerification THEN state[i].verified = state[i].artifact ELSE TRUE
  /\ state' = [state EXCEPT ![i].phase = "STARTING", ![i].session = 0, ![i].frontend = 0,
        ![i].bootstrap = 0, ![i].receipt = 0, ![i].health = 0, ![i].bootstrapReceipt = FALSE,
        ![i].operationPhase = "RUNNING",
        ![i].boot = NextCounter(state[i].boot), ![i].container = NextCounter(state[i].container),
        ![i].replacementPending = FALSE, ![i].fenceCommitted = FALSE, ![i].published = FALSE]
  /\ violations' = IF EnforceVerification /\ state[i].verified = state[i].artifact
                    THEN violations ELSE violations \cup {"UNVERIFIED_START"}

Bootstrap(i) ==
  /\ state[i].phase = "STARTING"
  /\ state[i].bootstrap = 0
  /\ state[i].fenceCommitted
  /\ IF EnforceSingleUse THEN state[i].seedConsumed ELSE TRUE
  /\ state' = [state EXCEPT ![i].phase = "BOOTSTRAPPING", ![i].bootstrap = 1,
        ![i].bootstrapReceipt = TRUE, ![i].receipt = state[i].container]
  /\ UNCHANGED violations

BootstrapInitial(i) ==
  /\ state[i].phase = "STARTING"
  /\ state[i].bootstrap = 0
  /\ IF EnforceSingleUse THEN ~state[i].seedConsumed ELSE TRUE
  /\ state[i].fenceCommitted
  /\ state[i].seedPresent
  /\ state' = [state EXCEPT ![i].phase = "BOOTSTRAPPING", ![i].bootstrap = 1,
        ![i].seedConsumed = TRUE, ![i].bootstrapReceipt = TRUE, ![i].receipt = state[i].container]
  /\ UNCHANGED violations

ReplayBootstrap(i) ==
  /\ ~EnforceSingleUse /\ state[i].phase = "BOOTSTRAPPING" /\ state[i].bootstrap > 0
  /\ state' = [state EXCEPT ![i].bootstrap = NextCounter(state[i].bootstrap)]
  /\ violations' = violations \cup {"BOOTSTRAP_REPLAY"}

Settle(i) ==
  /\ state[i].phase = "BOOTSTRAPPING"
  /\ ~state[i].replacementPending
  /\ state[i].fenceCommitted
  /\ ~state[i].seedPresent
  /\ state' = [state EXCEPT ![i].phase = "ACTIVE", ![i].receipt = state[i].container,
        ![i].health = state[i].container, ![i].session = state[i].container,
        ![i].frontend = state[i].container, ![i].published = TRUE, ![i].operationPhase = "IDLE"]
  /\ UNCHANGED violations

CommitInstanceFence(i) ==
  /\ state[i].phase = "STARTING"
  /\ ~state[i].fenceCommitted
  /\ state' = [state EXCEPT ![i].fenceCommitted = TRUE,
        ![i].seedFenceCommitted = IF ~state[i].seedConsumed THEN TRUE ELSE state[i].seedFenceCommitted]
  /\ UNCHANGED violations

DeleteSeed(i) ==
  /\ state[i].phase = "BOOTSTRAPPING"
  /\ state[i].fenceCommitted
  /\ state[i].seedConsumed
  /\ state[i].bootstrapReceipt
  /\ state[i].seedPresent
  /\ state' = [state EXCEPT ![i].seedPresent = FALSE]
  /\ UNCHANGED violations

Stop(i) ==
  /\ state[i].phase = "ACTIVE"
  /\ state' = [state EXCEPT ![i].phase = "STOPPED", ![i].session = 0, ![i].frontend = 0, ![i].published = FALSE, ![i].operationPhase = "IDLE"]
  /\ UNCHANGED violations

Restart(i) ==
  /\ state[i].phase = "ACTIVE"
  /\ state[i].operationPhase # "RUNNING"
  /\ IF EnforceVerification THEN state[i].verified = state[i].artifact ELSE TRUE
  /\ state' = [state EXCEPT ![i].phase = "STARTING", ![i].session = 0, ![i].frontend = 0,
        ![i].bootstrap = 0, ![i].receipt = 0, ![i].health = 0, ![i].bootstrapReceipt = FALSE,
        ![i].operationPhase = "RUNNING",
        ![i].boot = NextCounter(state[i].boot), ![i].container = NextCounter(state[i].container),
        ![i].replacementPending = FALSE, ![i].fenceCommitted = FALSE, ![i].published = FALSE]
  /\ UNCHANGED violations

Revoke(i) ==
  /\ state[i].phase \in {"ACTIVE", "STOPPED", "SEALED"}
  /\ state[i].operationPhase # "RUNNING"
  /\ state' = [state EXCEPT ![i].phase = IF state[i].phase = "SEALED" THEN "SEALED" ELSE "REVOKING", ![i].session = 0, ![i].frontend = 0,
        ![i].revision = NextCounter(state[i].revision), ![i].published = FALSE,
        ![i].operationPhase = IF state[i].phase = "SEALED" THEN "IDLE" ELSE "RUNNING"]
  /\ UNCHANGED violations

CompleteRevoke(i) ==
  /\ state[i].phase = "REVOKING"
  /\ state' = [state EXCEPT ![i].phase = "REVOKED", ![i].operationPhase = "IDLE"]
  /\ UNCHANGED violations

Crash(i) ==
  /\ state[i].phase \in {"ACTIVE", "BOOTSTRAPPING", "STARTING"}
  /\ state' = [state EXCEPT ![i].phase = "STOPPED", ![i].session = 0, ![i].frontend = 0,
        ![i].replacementPending = FALSE, ![i].published = FALSE]
  /\ UNCHANGED violations

PayloadCrash(i) ==
  /\ state[i].phase = "ACTIVE"
  /\ state' = [state EXCEPT ![i].phase = "STARTING", ![i].container = NextCounter(state[i].container),
        ![i].bootstrap = 0, ![i].receipt = 0, ![i].health = 0, ![i].session = 0, ![i].frontend = 0,
        ![i].replacementPending = TRUE, ![i].fenceCommitted = FALSE, ![i].published = FALSE]
  /\ UNCHANGED violations

AdoptReplacement(i) ==
  /\ state[i].phase = "BOOTSTRAPPING" /\ state[i].replacementPending
  /\ state[i].fenceCommitted
  /\ ~state[i].seedPresent
  /\ state' = [state EXCEPT ![i].phase = "ACTIVE", ![i].receipt = state[i].container,
        ![i].health = state[i].container, ![i].session = state[i].container,
        ![i].frontend = state[i].container, ![i].replacementPending = FALSE, ![i].published = TRUE]
  /\ UNCHANGED violations

StaleInstance(i) ==
  /\ ~EnforceInstanceFence /\ state[i].phase = "ACTIVE"
  /\ state' = [state EXCEPT ![i].health = state[i].container - 1]
  /\ violations' = violations \cup {"STALE_INSTANCE"}

StaleSession(i) ==
  /\ ~EnforceInstanceFence /\ state[i].phase = "ACTIVE"
  /\ state' = [state EXCEPT ![i].session = state[i].container - 1]
  /\ violations' = violations \cup {"STALE_SESSION"}

StaleFrontend(i) ==
  /\ ~EnforceInstanceFence /\ state[i].phase = "ACTIVE"
  /\ state' = [state EXCEPT ![i].frontend = state[i].container - 1]
  /\ violations' = violations \cup {"STALE_FRONTEND"}

Seal(i) ==
  /\ state[i].phase \in {"STARTING", "BOOTSTRAPPING", "ACTIVE"}
  /\ state' = [state EXCEPT ![i].phase = "SEALED", ![i].sealedUsed = TRUE,
        ![i].repairPending = FALSE, ![i].replacementPending = FALSE,
        ![i].session = 0, ![i].frontend = 0, ![i].published = FALSE]
  /\ UNCHANGED violations

PlanSealedRepair(i) ==
  /\ state[i].phase = "SEALED" /\ EnforceSealedRecovery /\ ~state[i].repairPending
  /\ state[i].operationPhase # "RUNNING"
  /\ state' = [state EXCEPT ![i].repairPending = TRUE, ![i].operationPhase = "RUNNING"]
  /\ UNCHANGED violations

RecoverSealed(i) ==
  /\ state[i].phase = "SEALED" /\ EnforceSealedRecovery /\ state[i].repairPending
  /\ state' = [state EXCEPT ![i].phase = "STOPPED", ![i].repairPending = FALSE,
        ![i].sealedUsed = FALSE, ![i].revision = NextCounter(state[i].revision), ![i].operationPhase = "IDLE"]
  /\ UNCHANGED violations

RepairCrash(i) ==
  /\ state[i].phase = "SEALED" /\ state[i].repairPending
  /\ state' = [state EXCEPT ![i].repairPending = FALSE, ![i].operationPhase = "IDLE"]
  /\ UNCHANGED violations

UnsafeReopenSealed(i) ==
  /\ ~EnforceSealedRecovery /\ state[i].phase = "SEALED"
  /\ state' = [state EXCEPT ![i].phase = "ACTIVE"]
  /\ violations' = violations \cup {"SEALED_RECOVERY_BYPASS"}

UnsafePublishBeforeFence(i) ==
  /\ ~EnforcePublicationFence
  /\ state[i].phase = "STARTING" /\ ~state[i].fenceCommitted
  /\ state' = [state EXCEPT ![i].phase = "ACTIVE", ![i].published = TRUE,
        ![i].receipt = state[i].container, ![i].health = state[i].container,
        ![i].session = state[i].container, ![i].frontend = state[i].container]
  /\ violations' = violations \cup {"PUBLISH_BEFORE_FENCE"}

UnsafeConsumeSeed(i) ==
  /\ ~EnforceSeedDeletionFence
  /\ state[i].phase = "STARTING" /\ state[i].seedIssued /\ state[i].seedPresent /\ ~state[i].fenceCommitted
  /\ state' = [state EXCEPT ![i].seedPresent = FALSE]
  /\ violations' = violations \cup {"SEED_CONSUMED_BEFORE_FENCE"}

PlanRemove(i) ==
  /\ state[i].phase \in {"ACTIVE", "STOPPED", "REVOKED", "SEALED"}
  /\ state[i].operationPhase # "RUNNING"
  /\ state' = [state EXCEPT ![i].phase = "PENDING_DESTROY",
        ![i].plannedRevision = NextCounter(state[i].revision),
        ![i].revision = NextCounter(state[i].revision), ![i].operationPhase = "RUNNING"]
  /\ UNCHANGED violations

ExecuteRemove(i) ==
  /\ state[i].phase = "PENDING_DESTROY"
  /\ IF EnforceRemoveRevision THEN state[i].plannedRevision = state[i].revision ELSE TRUE
  /\ state' = [state EXCEPT ![i].phase = "REMOVED", ![i].removed = TRUE, ![i].removeOwner = i,
        ![i].revision = IF EnforceRemoveRevision THEN state[i].revision ELSE UnsafeNextCounter(state[i].revision),
        ![i].operationPhase = "IDLE"]
  /\ violations' = IF EnforceRemoveRevision THEN violations
                    ELSE violations \cup {"REMOVE_REVISION_MISMATCH"}

CrossDomainRemove(i) ==
  /\ ~EnforceIsolation /\ state[i].phase = "PENDING_DESTROY"
  /\ state' = [state EXCEPT ![Other(i)].removed = TRUE, ![Other(i)].removeOwner = i]
  /\ violations' = violations \cup {"CROSS_DOMAIN_REMOVE"}

Progress(i) == StageArtifact(i) \/ Install(i) \/ Start(i) \/ BootstrapInitial(i) \/ Bootstrap(i) \/ CommitInstanceFence(i) \/ DeleteSeed(i) \/ Settle(i)
  \/ Stop(i) \/ Revoke(i) \/ CompleteRevoke(i) \/ Crash(i) \/ PlanSealedRepair(i) \/ RecoverSealed(i)
  \/ RepairCrash(i) \/ PlanRemove(i) \/ ExecuteRemove(i)

RecoverOperation(i) ==
  /\ state[i].operationPhase = "RUNNING"
  /\ state[i].phase \in {"STOPPED", "REVOKING", "REVOKED", "SEALED", "PENDING_DESTROY"}
  /\ state' = [state EXCEPT ![i].operationPhase = "INTERRUPTED"]
  /\ UNCHANGED violations

Next == \E i \in Installations:
  Progress(i) \/ Seal(i) \/ Restart(i) \/ ReplayBootstrap(i) \/ PayloadCrash(i) \/ AdoptReplacement(i) \/ StaleInstance(i)
  \/ UnsafeReopenSealed(i) \/ UnsafePublishBeforeFence(i) \/ UnsafeConsumeSeed(i)
  \/ CrossDomainRemove(i) \/ StaleSession(i) \/ StaleFrontend(i) \/ RecoverOperation(i) \/ UNCHANGED vars

TypeOK ==
  /\ DOMAIN state = Installations
  /\ \A i \in Installations:
       /\ state[i].phase \in Phases
       /\ state[i].artifact \in 0..1 /\ state[i].verified \in 0..1
       /\ state[i].boot \in Nat /\ state[i].container \in Nat
       /\ state[i].bootstrap \in Nat /\ state[i].receipt \in Nat /\ state[i].health \in Nat
       /\ state[i].bootstrapReceipt \in BOOLEAN
       /\ state[i].operationPhase \in {"IDLE", "RUNNING", "INTERRUPTED"}
       /\ state[i].session \in Nat /\ state[i].frontend \in Nat
       /\ state[i].revision \in Nat /\ state[i].plannedRevision \in Nat
       /\ state[i].sealedUsed \in BOOLEAN /\ state[i].seedConsumed \in BOOLEAN
       /\ state[i].replacementPending \in BOOLEAN /\ state[i].repairPending \in BOOLEAN
       /\ state[i].seedPresent \in BOOLEAN /\ state[i].seedIssued \in BOOLEAN
       /\ state[i].seedFenceCommitted \in BOOLEAN
       /\ state[i].fenceCommitted \in BOOLEAN /\ state[i].published \in BOOLEAN
       /\ state[i].removed \in BOOLEAN
       /\ state[i].removeOwner \in Installations \cup {"NONE"}
  /\ violations \subseteq {"UNVERIFIED_START", "BOOTSTRAP_REPLAY", "STALE_INSTANCE", "STALE_SESSION", "STALE_FRONTEND",
       "SEALED_RECOVERY_BYPASS", "REMOVE_REVISION_MISMATCH", "CROSS_DOMAIN_REMOVE",
       "PUBLISH_BEFORE_FENCE", "SEED_CONSUMED_BEFORE_FENCE"}

ActiveVerifiedExact == \A i \in Installations: state[i].phase = "ACTIVE" => state[i].verified = state[i].artifact
BootstrapSingleUse == \A i \in Installations: state[i].bootstrap <= 1
BootstrapSeedSingleUse == \A i \in Installations: state[i].bootstrap > 0 => state[i].seedConsumed
ReplacementFence == \A i \in Installations: state[i].replacementPending => state[i].session = 0 /\ state[i].frontend = 0
CurrentInstanceFence == \A i \in Installations: state[i].phase = "ACTIVE" =>
  /\ state[i].receipt = state[i].container /\ state[i].health = state[i].container
  /\ state[i].session = state[i].container /\ state[i].frontend = state[i].container
PublicationFence == \A i \in Installations: state[i].phase = "ACTIVE" => state[i].fenceCommitted /\ state[i].published
SeedConsumptionFence == \A i \in Installations:
  state[i].phase = "BOOTSTRAPPING" /\ state[i].seedConsumed => state[i].fenceCommitted
InitialSeedDeletionFence == \A i \in Installations:
  state[i].seedIssued /\ ~state[i].seedPresent => state[i].seedFenceCommitted
InstallationNonInterference == \A i \in Installations: state[i].removed => state[i].removeOwner = i
SealedRecoveryIsBounded == \A i \in Installations: state[i].sealedUsed => state[i].phase \in {"SEALED", "STAGED", "REMOVED", "PENDING_DESTROY"}
RemoveRevisionExact == \A i \in Installations: state[i].removed => state[i].plannedRevision = state[i].revision

BootstrapEventuallySettles ==
  \A i \in Installations:
    [] (state[i].phase = "BOOTSTRAPPING" =>
      <> (state[i].phase \in {"ACTIVE", "STOPPED", "SEALED"}))

ReplacementEventuallySettles ==
  \A i \in Installations:
    [] (state[i].replacementPending => <> ~state[i].replacementPending)

InstallEventuallySettles ==
  \A i \in Installations:
    [] (state[i].phase = "INSTALLED" => <> (state[i].phase # "INSTALLED"))

RemoveEventuallyTerminates ==
  \A i \in Installations:
    [] (state[i].phase = "PENDING_DESTROY" => <> (state[i].phase = "REMOVED"))

OperationEventuallySettles ==
  \A i \in Installations:
    [] (state[i].operationPhase = "RUNNING" => <> (state[i].operationPhase \in {"IDLE", "INTERRUPTED"}))

RevokeEventuallySettles ==
  \A i \in Installations:
    [] (state[i].phase = "REVOKING" => <> (state[i].phase = "REVOKED"))

Fairness == \A i \in Installations:
  /\ WF_vars(Install(i))
  /\ WF_vars(BootstrapInitial(i))
  /\ WF_vars(Bootstrap(i))
  /\ WF_vars(CommitInstanceFence(i))
  /\ WF_vars(DeleteSeed(i))
  /\ WF_vars(Settle(i))
  /\ WF_vars(AdoptReplacement(i))
  /\ WF_vars(CompleteRevoke(i))
  /\ WF_vars(ExecuteRemove(i))
  /\ WF_vars(RecoverOperation(i))
  /\ WF_vars(RecoverSealed(i))
  /\ WF_vars(PlanSealedRepair(i))
  /\ WF_vars(RepairCrash(i))
Spec == Init /\ [][Next]_vars /\ WF_vars(Next) /\ Fairness
=============================================================================
