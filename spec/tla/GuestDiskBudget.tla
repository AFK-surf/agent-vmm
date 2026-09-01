---------------------------- MODULE GuestDiskBudget ---------------------------
EXTENDS Naturals

CONSTANTS EnforceRetainedAccounting, EnforceCacheBudget

D == 8
E == 1
M == 1

VARIABLES U, W, I, retained

vars == <<U, W, I, retained>>

\* U is the single physical allocation fact: workload bytes, OCI content,
\* snapshots, temp, logs, DB, and rebuildable cache are all included.
Init == /\ U = 3 /\ W = 2 /\ I = 0 /\ retained = 1

Safe(u, w, i) == u + w + i + E + M <= D

AdmitQuota ==
  /\ Safe(U, W + 1, I)
  /\ W' = W + 1
  /\ UNCHANGED <<U, I, retained>>

WriteQuota ==
  /\ W > 0
  /\ U' = U + 1
  /\ W' = W - 1
  /\ UNCHANGED <<I, retained>>

ReserveImport ==
  /\ Safe(U, W, I + 1)
  /\ I' = I + 1
  /\ UNCHANGED <<U, W, retained>>

CommitAbort ==
  /\ I > 0
  /\ I' = I - 1
  /\ UNCHANGED <<U, W, retained>>

WriteImport ==
  /\ I > 0
  /\ U' = U + 1
  /\ I' = I - 1
  /\ UNCHANGED <<W, retained>>

CacheGrow ==
  /\ ~EnforceCacheBudget \/ Safe(U + 1, W, I)
  /\ U' = U + 1
  /\ UNCHANGED <<W, I, retained>>

GC ==
  /\ U > 0
  /\ U' = U - 1
  /\ UNCHANGED <<W, I, retained>>

RetainRelease ==
  /\ retained > 0
  /\ IF EnforceRetainedAccounting
        THEN UNCHANGED <<U, W>>
        ELSE /\ W' = W - 1
             \* The retained writer still consumes the quota that the unsafe
             \* release stopped accounting for.
             /\ U' = U + 3
  /\ UNCHANGED <<I, retained>>

DeleteRetained ==
  /\ retained > 0 /\ W > 0
  /\ retained' = retained - 1
  /\ W' = W - 1
  /\ U' = IF U > 0 THEN U - 1 ELSE U
  /\ UNCHANGED I

Next == AdmitQuota \/ WriteQuota \/ ReserveImport \/ WriteImport \/ CommitAbort \/ CacheGrow \/ GC \/ RetainRelease \/ DeleteRetained

TypeOK == /\ U \in Nat /\ W \in Nat /\ I \in Nat /\ retained \in 0..1
DiskInvariant == Safe(U, W, I)

Spec == Init /\ [][Next]_vars

=============================================================================
