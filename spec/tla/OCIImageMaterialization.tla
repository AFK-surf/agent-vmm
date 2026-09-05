----------------------- MODULE OCIImageMaterialization ---------------------
EXTENDS Naturals

CONSTANTS EnforceProjectionFence, EnforceCreateFence, EnforceDigestFence,
          EnforceBuildImport, EnforceBuildCommit

\* Production anchors: internal/guest/control.importImage,
\* Runtime.ListImages, Runtime.createContainer, and Runtime.BuildImage.
\* BuildKit emits an archive; only the runtime importer publishes its material.
\* Build failures/restarts may abandon a candidate. No build progress is claimed.

VARIABLES recordDigest, contentReady, snapshotReady, repairPending,
          projectedStale, createdFromStale, replacedByConflict,
          expectedDigest, buildStage, buildContent, buildSnapshot,
          invalidBuildUnpack, invalidBuildPublish

vars == <<recordDigest, contentReady, snapshotReady, repairPending,
                  projectedStale, createdFromStale, replacedByConflict, expectedDigest,
                  buildStage, buildContent, buildSnapshot, invalidBuildUnpack,
                  invalidBuildPublish>>

Init ==
  /\ recordDigest = 1
  /\ expectedDigest = 1
  /\ buildStage = "idle"
  /\ buildContent = FALSE /\ buildSnapshot = FALSE
  /\ invalidBuildUnpack = FALSE /\ invalidBuildPublish = FALSE
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
                  createdFromStale, replacedByConflict, expectedDigest, buildStage,
                  buildContent, buildSnapshot, invalidBuildUnpack, invalidBuildPublish>>

RetryExactArchive ==
  /\ recordDigest = 1
  /\ ~snapshotReady
  /\ repairPending' = TRUE
  /\ UNCHANGED <<recordDigest, contentReady, snapshotReady, projectedStale,
                  createdFromStale, replacedByConflict, expectedDigest, buildStage,
                  buildContent, buildSnapshot, invalidBuildUnpack, invalidBuildPublish>>

CommitExactArchive ==
  /\ repairPending
  /\ contentReady' = TRUE
  /\ snapshotReady' = TRUE
  /\ repairPending' = FALSE
  /\ UNCHANGED <<recordDigest, projectedStale, createdFromStale, replacedByConflict,
                  expectedDigest, buildStage, buildContent, buildSnapshot,
                  invalidBuildUnpack, invalidBuildPublish>>

RetryConflictingArchive ==
  /\ IF EnforceDigestFence
        THEN UNCHANGED <<recordDigest, contentReady, snapshotReady, repairPending,
                  replacedByConflict, expectedDigest, buildStage, buildContent,
                  buildSnapshot, invalidBuildUnpack, invalidBuildPublish>>
        ELSE /\ recordDigest' = 2
             /\ contentReady' = TRUE
             /\ snapshotReady' = TRUE
             /\ repairPending' = FALSE
             /\ replacedByConflict' = TRUE
  /\ UNCHANGED <<projectedStale, createdFromStale, expectedDigest, buildStage,
                  buildContent, buildSnapshot, invalidBuildUnpack, invalidBuildPublish>>

CreateContainer ==
  /\ ~EnforceCreateFence \/ (recordDigest = expectedDigest /\ contentReady /\ snapshotReady)
  /\ createdFromStale' = (createdFromStale \/
       ~(recordDigest = expectedDigest /\ contentReady /\ snapshotReady))
  /\ UNCHANGED <<recordDigest, contentReady, snapshotReady, repairPending,
                  projectedStale, replacedByConflict, expectedDigest, buildStage,
                  buildContent, buildSnapshot, invalidBuildUnpack, invalidBuildPublish>>

BeginBuild ==
  /\ buildStage = "idle"
  /\ buildStage' = "built"
  /\ buildContent' = FALSE /\ buildSnapshot' = FALSE
  /\ UNCHANGED <<recordDigest, contentReady, snapshotReady, repairPending,
                  projectedStale, createdFromStale, replacedByConflict, expectedDigest,
                  invalidBuildUnpack, invalidBuildPublish>>

ImportBuild ==
  /\ buildStage = "built"
  /\ buildStage' = "imported"
  /\ buildContent' = EnforceBuildImport
  /\ UNCHANGED <<recordDigest, contentReady, snapshotReady, repairPending,
                  projectedStale, createdFromStale, replacedByConflict, expectedDigest,
                  buildSnapshot, invalidBuildUnpack, invalidBuildPublish>>

UnpackBuild ==
  /\ buildStage = "imported"
  /\ buildStage' = IF buildContent THEN "unpacked" ELSE "failed"
  /\ buildSnapshot' = buildContent
  /\ invalidBuildUnpack' = (invalidBuildUnpack \/ ~buildContent)
  /\ UNCHANGED <<recordDigest, contentReady, snapshotReady, repairPending,
                  projectedStale, createdFromStale, replacedByConflict, expectedDigest,
                  buildContent, invalidBuildPublish>>

PublishBuild ==
  /\ buildStage \in {"built", "imported", "unpacked"}
  /\ ~EnforceBuildCommit \/ buildStage = "unpacked"
  /\ recordDigest' = 2 /\ expectedDigest' = 2
  /\ contentReady' = buildContent /\ snapshotReady' = buildSnapshot
  /\ repairPending' = FALSE /\ buildStage' = "idle"
  /\ invalidBuildPublish' = (invalidBuildPublish \/ ~(buildContent /\ buildSnapshot))
  /\ UNCHANGED <<projectedStale, createdFromStale, replacedByConflict, buildContent,
                  buildSnapshot, invalidBuildUnpack>>

AbandonBuild ==
  /\ buildStage # "idle"
  /\ buildStage' = "idle"
  /\ buildContent' = FALSE /\ buildSnapshot' = FALSE
  /\ UNCHANGED <<recordDigest, contentReady, snapshotReady, repairPending,
                  projectedStale, createdFromStale, replacedByConflict, expectedDigest,
                  invalidBuildUnpack, invalidBuildPublish>>

Next == BeginBuild \/ ImportBuild \/ UnpackBuild \/ PublishBuild \/ AbandonBuild \/
        ObserveImage \/ RetryExactArchive \/ CommitExactArchive \/
        RetryConflictingArchive \/ CreateContainer

TypeOK ==
  /\ recordDigest \in 1..2
  /\ contentReady \in BOOLEAN
  /\ snapshotReady \in BOOLEAN
  /\ repairPending \in BOOLEAN
  /\ projectedStale \in BOOLEAN
  /\ createdFromStale \in BOOLEAN
  /\ replacedByConflict \in BOOLEAN
  /\ expectedDigest \in 1..2
  /\ buildStage \in {"idle", "built", "imported", "unpacked", "failed"}
  /\ buildContent \in BOOLEAN /\ buildSnapshot \in BOOLEAN
  /\ invalidBuildUnpack \in BOOLEAN /\ invalidBuildPublish \in BOOLEAN

ReadySnapshotHasContent == snapshotReady => contentReady
UnavailableImageIsNotProjected == ~projectedStale
CreateRequiresExactMaterial == ~createdFromStale
ConflictingArchiveDoesNotReplace == ~replacedByConflict /\ recordDigest = expectedDigest
BuildUnpackUsesRuntimeContent == ~invalidBuildUnpack
BuildPublicationRequiresMaterial == ~invalidBuildPublish
RecoveryEventuallyMaterializes == <> (contentReady /\ snapshotReady)

Spec == Init /\ [][Next]_vars /\ WF_vars(RetryExactArchive) /\
        WF_vars(CommitExactArchive)

=============================================================================
