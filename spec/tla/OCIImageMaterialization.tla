----------------------- MODULE OCIImageMaterialization ---------------------
EXTENDS Naturals

CONSTANTS EnforceProjectionFence, EnforceCreateFence, EnforceDigestFence

\* Production anchors: internal/guest/control.importImage,
\* Runtime.ListImages, and Runtime.createContainer.

VARIABLES recordDigest, contentReady, snapshotReady, repairPending,
          projectedStale, createdFromStale, replacedByConflict

vars == <<recordDigest, contentReady, snapshotReady, repairPending,
          projectedStale, createdFromStale, replacedByConflict>>

Init ==
  /\ recordDigest = 1
  /\ contentReady \in BOOLEAN
  /\ snapshotReady = FALSE
  /\ repairPending = FALSE
  /\ projectedStale = FALSE
  /\ createdFromStale = FALSE
  /\ replacedByConflict = FALSE

ObserveImage ==
  /\ ~EnforceProjectionFence \/ (contentReady /\ snapshotReady)
  /\ projectedStale' = (projectedStale \/ ~contentReady \/ ~snapshotReady)
  /\ UNCHANGED <<recordDigest, contentReady, snapshotReady, repairPending,
                  createdFromStale, replacedByConflict>>

RetryExactArchive ==
  /\ recordDigest = 1
  /\ ~snapshotReady
  /\ repairPending' = TRUE
  /\ UNCHANGED <<recordDigest, contentReady, snapshotReady, projectedStale,
                  createdFromStale, replacedByConflict>>

CommitExactArchive ==
  /\ repairPending
  /\ contentReady' = TRUE
  /\ snapshotReady' = TRUE
  /\ repairPending' = FALSE
  /\ UNCHANGED <<recordDigest, projectedStale, createdFromStale,
                  replacedByConflict>>

RetryConflictingArchive ==
  /\ IF EnforceDigestFence
        THEN UNCHANGED <<recordDigest, contentReady, snapshotReady,
                         repairPending, replacedByConflict>>
        ELSE /\ recordDigest' = 2
             /\ contentReady' = TRUE
             /\ snapshotReady' = TRUE
             /\ repairPending' = FALSE
             /\ replacedByConflict' = TRUE
  /\ UNCHANGED <<projectedStale, createdFromStale>>

CreateContainer ==
  /\ ~EnforceCreateFence \/ (recordDigest = 1 /\ contentReady /\ snapshotReady)
  /\ createdFromStale' = (createdFromStale \/
       ~(recordDigest = 1 /\ contentReady /\ snapshotReady))
  /\ UNCHANGED <<recordDigest, contentReady, snapshotReady, repairPending,
                  projectedStale, replacedByConflict>>

Next == ObserveImage \/ RetryExactArchive \/ CommitExactArchive \/
        RetryConflictingArchive \/ CreateContainer

TypeOK ==
  /\ recordDigest \in 1..2
  /\ contentReady \in BOOLEAN
  /\ snapshotReady \in BOOLEAN
  /\ repairPending \in BOOLEAN
  /\ projectedStale \in BOOLEAN
  /\ createdFromStale \in BOOLEAN
  /\ replacedByConflict \in BOOLEAN

ReadySnapshotHasContent == snapshotReady => contentReady
UnavailableImageIsNotProjected == ~projectedStale
CreateRequiresExactMaterial == ~createdFromStale
ConflictingArchiveDoesNotReplace == ~replacedByConflict /\ recordDigest = 1
RecoveryEventuallyMaterializes == <> (contentReady /\ snapshotReady)

Spec == Init /\ [][Next]_vars /\ WF_vars(RetryExactArchive) /\
        WF_vars(CommitExactArchive)

=============================================================================
