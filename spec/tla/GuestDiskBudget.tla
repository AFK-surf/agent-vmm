---------------------------- MODULE GuestDiskBudget ---------------------------
EXTENDS Naturals

CONSTANTS EnforceRetainedAccounting, EnforceCacheBudget, EnforceSealLinearization

D == 8
E == 1
M == 1

VARIABLES U, W, I, retained, sealed, settlement, checkedImport, postSealGrowth

vars == <<U, W, I, retained, sealed, settlement, checkedImport, postSealGrowth>>

\* U is the single physical allocation fact: workload bytes, OCI content,
\* snapshots, temp, logs, DB, and rebuildable cache are all included.
Init == /\ U = 3 /\ W = 2 /\ I = 0 /\ retained = 1
        /\ sealed = FALSE /\ settlement = "idle"
        /\ checkedImport = FALSE /\ postSealGrowth = FALSE

Safe(u, w, i) == u + w + i + E + M <= D

AdmitQuota ==
  /\ ~sealed
  /\ Safe(U, W + 1, I)
  /\ W' = W + 1
  /\ UNCHANGED <<U, I, retained, sealed, settlement, checkedImport, postSealGrowth>>

WriteQuota ==
  /\ ~sealed
  /\ W > 0
  /\ U' = U + 1
  /\ W' = W - 1
  /\ UNCHANGED <<I, retained, sealed, settlement, checkedImport, postSealGrowth>>

ReserveImport ==
  /\ ~sealed
  /\ Safe(U, W, I + 1)
  /\ I' = I + 1
  /\ UNCHANGED <<U, W, retained, sealed, settlement, checkedImport, postSealGrowth>>

CommitAbort ==
  /\ I > 0
  /\ ~checkedImport
  /\ I' = I - 1
  /\ UNCHANGED <<U, W, retained, sealed, settlement, checkedImport, postSealGrowth>>

BeginWriteImport ==
  /\ I > 0
  /\ ~sealed
  /\ ~checkedImport
  /\ checkedImport' = TRUE
  /\ UNCHANGED <<U, W, I, retained, sealed, settlement, postSealGrowth>>

WriteImport ==
  /\ checkedImport
  /\ (~EnforceSealLinearization \/ ~sealed)
  /\ U' = U + 1
  /\ I' = I - 1
  /\ checkedImport' = FALSE
  /\ postSealGrowth' = (postSealGrowth \/ sealed)
  /\ UNCHANGED <<W, retained, sealed, settlement>>

CacheGrow ==
  /\ ~sealed
  /\ ~EnforceCacheBudget \/ Safe(U + 1, W, I)
  /\ U' = U + 1
  /\ UNCHANGED <<W, I, retained, sealed, settlement, checkedImport, postSealGrowth>>

MetadataGrow ==
  /\ ~sealed
  /\ Safe(U + 1, W, I)
  /\ U' = U + 1
  /\ UNCHANGED <<W, I, retained, sealed, settlement, checkedImport, postSealGrowth>>

GC ==
  /\ U > 0
  /\ U' = U - 1
  /\ UNCHANGED <<W, I, retained, sealed, settlement, checkedImport, postSealGrowth>>

SettlementFailure ==
  /\ ~sealed
  /\ settlement = "idle"
  /\ settlement' = "retry"
  /\ UNCHANGED <<U, W, I, retained, sealed, checkedImport, postSealGrowth>>

RetrySettlement ==
  /\ settlement = "retry"
  /\ settlement' = "failed"
  /\ UNCHANGED <<U, W, I, retained, sealed, checkedImport, postSealGrowth>>

Seal ==
  /\ settlement = "failed"
  /\ (~EnforceSealLinearization \/ ~checkedImport)
  /\ sealed' = TRUE
  /\ settlement' = "sealed"
  /\ UNCHANGED <<U, W, I, retained, checkedImport, postSealGrowth>>

RestartExactInit ==
  /\ sealed
  /\ ~checkedImport
  /\ sealed' = FALSE
  /\ settlement' = "idle"
  /\ UNCHANGED <<U, W, I, retained, checkedImport, postSealGrowth>>

RetainRelease ==
  /\ retained > 0
  /\ IF EnforceRetainedAccounting
        THEN UNCHANGED <<U, W>>
        ELSE /\ W' = W - 1
             \* The retained writer still consumes the quota that the unsafe
             \* release stopped accounting for.
             /\ U' = U + 3
  /\ UNCHANGED <<I, retained, sealed, settlement, checkedImport, postSealGrowth>>

DeleteRetained ==
  /\ retained > 0 /\ W > 0
  /\ retained' = retained - 1
  /\ W' = W - 1
  /\ U' = IF U > 0 THEN U - 1 ELSE U
  /\ UNCHANGED <<I, sealed, settlement, checkedImport, postSealGrowth>>

Next == AdmitQuota \/ WriteQuota \/ ReserveImport \/ BeginWriteImport \/ WriteImport
        \/ CommitAbort \/ CacheGrow \/ MetadataGrow \/ GC
        \/ SettlementFailure \/ RetrySettlement \/ Seal \/ RestartExactInit
        \/ RetainRelease \/ DeleteRetained

TypeOK == /\ U \in Nat /\ W \in Nat /\ I \in Nat /\ retained \in 0..1
          /\ sealed \in BOOLEAN /\ settlement \in {"idle", "retry", "failed", "sealed"}
          /\ checkedImport \in BOOLEAN /\ postSealGrowth \in BOOLEAN
DiskInvariant == Safe(U, W, I)
NoGrowthAfterSeal == ~postSealGrowth

Spec == Init /\ [][Next]_vars

=============================================================================
