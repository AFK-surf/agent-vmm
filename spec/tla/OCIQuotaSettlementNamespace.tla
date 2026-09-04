--------------------- MODULE OCIQuotaSettlementNamespace --------------------
EXTENDS Naturals

CONSTANT EnforceSharedMutationLock

VARIABLES namespaceExists, settlement, mutationLock, recreatedBucket

\* Implementation anchors:
\* - internal/guest/control/oci_budget_linux.go:settleOCIProjectQuota
\* - internal/guest/control/namespace_linux.go:deleteNamespaceLocked
vars == <<namespaceExists, settlement, mutationLock, recreatedBucket>>

Init ==
  /\ namespaceExists = TRUE
  /\ settlement = "idle"
  /\ mutationLock = "free"
  /\ recreatedBucket = FALSE

BeginSettlement ==
  /\ settlement = "idle"
  /\ namespaceExists
  /\ mutationLock = "free"
  /\ settlement' = "selected"
  /\ mutationLock' = "settlement"
  /\ UNCHANGED <<namespaceExists, recreatedBucket>>

DeleteNamespace ==
  /\ namespaceExists
  /\ (~EnforceSharedMutationLock \/ mutationLock = "free")
  /\ namespaceExists' = FALSE
  /\ UNCHANGED <<settlement, mutationLock, recreatedBucket>>

RunSynchronousBarrier ==
  /\ settlement = "selected"
  /\ recreatedBucket' = (~namespaceExists \/ recreatedBucket)
  /\ namespaceExists' = TRUE
  /\ settlement' = "barrier"
  /\ UNCHANGED mutationLock

FinishSettlement ==
  /\ settlement = "barrier"
  /\ settlement' = "done"
  /\ mutationLock' = "free"
  /\ UNCHANGED <<namespaceExists, recreatedBucket>>

DeleteAfterSettlement ==
  /\ settlement = "done"
  /\ namespaceExists
  /\ mutationLock = "free"
  /\ namespaceExists' = FALSE
  /\ UNCHANGED <<settlement, mutationLock, recreatedBucket>>

Next == BeginSettlement \/ DeleteNamespace \/ RunSynchronousBarrier \/
        FinishSettlement \/ DeleteAfterSettlement

TypeOK ==
  /\ namespaceExists \in BOOLEAN
  /\ settlement \in {"idle", "selected", "barrier", "done"}
  /\ mutationLock \in {"free", "settlement"}
  /\ recreatedBucket \in BOOLEAN

SettlementNeverRecreatesDeletedNamespace == ~recreatedBucket

Spec == Init /\ [][Next]_vars

=============================================================================
