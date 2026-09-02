-------------------------- MODULE OCIContentLifecycle -------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS EnforceReferenceFence, EnforceGlobalSlot, EnforceCriticalGate,
          EnforceSealLinearization

Digests == {"a", "b"}
Namespaces == {"one", "two"}
Refs == Namespaces \X Digests

VARIABLES materializing, content, snapshots, activeRefs, retainedRefs,
          reservations, temp, timedOut, deletedContent, deletedSnapshots,
          pressure, lastAdmissionPressure, sealed, settlement, checkedDownload,
          postSealGrowth, cacheMetadata

vars == <<materializing, content, snapshots, activeRefs, retainedRefs,
          reservations, temp, timedOut, deletedContent, deletedSnapshots,
          pressure, lastAdmissionPressure, sealed, settlement, checkedDownload,
          postSealGrowth, cacheMetadata>>

ReferencedDigests == {d \in Digests : \E n \in Namespaces: <<n,d>> \in activeRefs \cup retainedRefs}

Init ==
  /\ materializing = {}
  /\ content = {"a"} /\ snapshots = {"a"}
  /\ activeRefs = {<<"one","a">>} /\ retainedRefs = {}
  /\ reservations = {} /\ temp = {} /\ timedOut = {}
  /\ deletedContent = {} /\ deletedSnapshots = {}
  /\ pressure = 0 /\ lastAdmissionPressure = 0
  /\ sealed = FALSE /\ settlement = "idle" /\ checkedDownload = {}
  /\ postSealGrowth = FALSE /\ cacheMetadata = 0

ReserveImport(d) ==
  /\ d \in Digests
  /\ ~sealed
  /\ (~EnforceCriticalGate \/ pressure < 2)
  /\ (~EnforceGlobalSlot \/ materializing = {})
  /\ materializing' = materializing \cup {d}
  /\ reservations' = reservations \cup {d}
  /\ lastAdmissionPressure' = pressure
  /\ UNCHANGED <<content, snapshots, activeRefs, retainedRefs, temp, timedOut,
                  deletedContent, deletedSnapshots, pressure, sealed, settlement,
                  checkedDownload, postSealGrowth, cacheMetadata>>

BeginDownload(d) ==
  /\ d \in materializing \cap reservations
  /\ ~sealed
  /\ d \notin checkedDownload
  /\ checkedDownload' = checkedDownload \cup {d}
  /\ UNCHANGED <<materializing, content, snapshots, activeRefs, retainedRefs,
                  reservations, temp, timedOut, deletedContent, deletedSnapshots,
                  pressure, lastAdmissionPressure, sealed, settlement,
                  postSealGrowth, cacheMetadata>>

Download(d) ==
  /\ d \in checkedDownload
  /\ (~EnforceSealLinearization \/ ~sealed)
  /\ temp' = temp \cup {d}
  /\ checkedDownload' = checkedDownload \ {d}
  /\ postSealGrowth' = (postSealGrowth \/ sealed)
  /\ UNCHANGED <<materializing, content, snapshots, activeRefs, retainedRefs,
                  reservations, timedOut, deletedContent, deletedSnapshots,
                  pressure, lastAdmissionPressure, sealed, settlement, cacheMetadata>>

Unpack(d) ==
  /\ d \in materializing \cap reservations \cap temp
  /\ ~sealed
  /\ content' = content \cup {d}
  /\ snapshots' = snapshots \cup {d}
  /\ deletedContent' = deletedContent \ {d}
  /\ deletedSnapshots' = deletedSnapshots \ {d}
  /\ UNCHANGED <<materializing, activeRefs, retainedRefs, reservations, temp,
                  timedOut, pressure, lastAdmissionPressure, sealed, settlement,
                  checkedDownload, postSealGrowth, cacheMetadata>>

Commit(d) ==
  /\ d \in materializing \cap content \cap snapshots
  /\ materializing' = materializing \ {d}
  /\ reservations' = reservations \ {d}
  /\ temp' = temp \ {d}
  /\ timedOut' = timedOut \ {d}
  /\ UNCHANGED <<content, snapshots, activeRefs, retainedRefs, pressure,
                  lastAdmissionPressure, deletedContent, deletedSnapshots,
                  sealed, settlement, checkedDownload, postSealGrowth, cacheMetadata>>

AbortOrCrash(d) ==
  /\ d \in materializing
  /\ materializing' = materializing \ {d}
  /\ reservations' = reservations \ {d}
  /\ temp' = temp \ {d}
  /\ UNCHANGED <<content, snapshots, activeRefs, retainedRefs, timedOut,
                  deletedContent, deletedSnapshots, pressure,
                  lastAdmissionPressure, sealed, settlement, checkedDownload,
                  postSealGrowth, cacheMetadata>>

Timeout(d) ==
  /\ d \in materializing
  /\ timedOut' = timedOut \cup {d}
  /\ materializing' = materializing \ {d}
  /\ reservations' = reservations \ {d}
  /\ temp' = temp \ {d}
  /\ UNCHANGED <<content, snapshots, activeRefs, retainedRefs,
                  deletedContent, deletedSnapshots, pressure,
                  lastAdmissionPressure, sealed, settlement, checkedDownload,
                  postSealGrowth, cacheMetadata>>

AddActiveRef(n,d) ==
  /\ <<n,d>> \in Refs /\ d \in content \cap snapshots
  /\ activeRefs' = activeRefs \cup {<<n,d>>}
  /\ UNCHANGED <<materializing, content, snapshots, retainedRefs, reservations,
                  temp, timedOut, deletedContent, deletedSnapshots, pressure,
                  lastAdmissionPressure, sealed, settlement, checkedDownload,
                  postSealGrowth, cacheMetadata>>

RetainRef(n,d) ==
  /\ <<n,d>> \in activeRefs
  /\ activeRefs' = activeRefs \ {<<n,d>>}
  /\ retainedRefs' = retainedRefs \cup {<<n,d>>}
  /\ UNCHANGED <<materializing, content, snapshots, reservations, temp,
                  timedOut, deletedContent, deletedSnapshots, pressure,
                  lastAdmissionPressure, sealed, settlement, checkedDownload,
                  postSealGrowth, cacheMetadata>>

ReleaseRef(n,d) ==
  /\ <<n,d>> \in activeRefs \cup retainedRefs
  /\ activeRefs' = activeRefs \ {<<n,d>>}
  /\ retainedRefs' = retainedRefs \ {<<n,d>>}
  /\ UNCHANGED <<materializing, content, snapshots, reservations, temp,
                  timedOut, deletedContent, deletedSnapshots, pressure,
                  lastAdmissionPressure, sealed, settlement, checkedDownload,
                  postSealGrowth, cacheMetadata>>

GC(d) ==
  /\ d \in content \cup snapshots
  /\ ~EnforceReferenceFence \/ (d \notin ReferencedDigests /\ d \notin materializing)
  /\ content' = content \ {d}
  /\ snapshots' = snapshots \ {d}
  /\ deletedContent' = deletedContent \cup {d}
  /\ deletedSnapshots' = deletedSnapshots \cup {d}
  /\ UNCHANGED <<materializing, activeRefs, retainedRefs, reservations, temp,
                  timedOut, pressure, lastAdmissionPressure, sealed, settlement,
                  checkedDownload, postSealGrowth, cacheMetadata>>

IncreasePressure ==
  /\ pressure < 3
  /\ pressure' = pressure + 1
  /\ UNCHANGED <<materializing, content, snapshots, activeRefs, retainedRefs,
                  reservations, temp, timedOut, deletedContent,
                  deletedSnapshots, lastAdmissionPressure, sealed, settlement,
                  checkedDownload, postSealGrowth, cacheMetadata>>

CacheOrMetadataGrow ==
  /\ ~sealed
  /\ cacheMetadata < 2
  /\ cacheMetadata' = cacheMetadata + 1
  /\ UNCHANGED <<materializing, content, snapshots, activeRefs, retainedRefs,
                  reservations, temp, timedOut, deletedContent, deletedSnapshots,
                  pressure, lastAdmissionPressure, sealed, settlement,
                  checkedDownload, postSealGrowth>>

SettlementFailure ==
  /\ ~sealed /\ settlement = "idle"
  /\ settlement' = "retry"
  /\ UNCHANGED <<materializing, content, snapshots, activeRefs, retainedRefs,
                  reservations, temp, timedOut, deletedContent, deletedSnapshots,
                  pressure, lastAdmissionPressure, sealed, checkedDownload,
                  postSealGrowth, cacheMetadata>>

RetrySettlement ==
  /\ settlement = "retry"
  /\ settlement' = "failed"
  /\ UNCHANGED <<materializing, content, snapshots, activeRefs, retainedRefs,
                  reservations, temp, timedOut, deletedContent, deletedSnapshots,
                  pressure, lastAdmissionPressure, sealed, checkedDownload,
                  postSealGrowth, cacheMetadata>>

Seal ==
  /\ settlement = "failed"
  /\ (~EnforceSealLinearization \/ checkedDownload = {})
  /\ sealed' = TRUE /\ settlement' = "sealed"
  /\ UNCHANGED <<materializing, content, snapshots, activeRefs, retainedRefs,
                  reservations, temp, timedOut, deletedContent, deletedSnapshots,
                  pressure, lastAdmissionPressure, checkedDownload,
                  postSealGrowth, cacheMetadata>>

RestartExactInit ==
  /\ sealed /\ checkedDownload = {}
  /\ sealed' = FALSE /\ settlement' = "idle"
  /\ UNCHANGED <<materializing, content, snapshots, activeRefs, retainedRefs,
                  reservations, temp, timedOut, deletedContent, deletedSnapshots,
                  pressure, lastAdmissionPressure, checkedDownload,
                  postSealGrowth, cacheMetadata>>

Next == \/ \E d \in Digests: ReserveImport(d) \/ BeginDownload(d) \/ Download(d) \/ Unpack(d) \/ Commit(d) \/ AbortOrCrash(d) \/ Timeout(d) \/ GC(d)
        \/ \E n \in Namespaces, d \in Digests: AddActiveRef(n,d) \/ RetainRef(n,d) \/ ReleaseRef(n,d)
        \/ IncreasePressure \/ CacheOrMetadataGrow \/ SettlementFailure
        \/ RetrySettlement \/ Seal \/ RestartExactInit

TypeOK ==
  /\ materializing \subseteq Digests /\ content \subseteq Digests /\ snapshots \subseteq Digests
  /\ activeRefs \subseteq Refs /\ retainedRefs \subseteq Refs
  /\ reservations \subseteq Digests /\ temp \subseteq Digests /\ timedOut \subseteq Digests
  /\ deletedContent \subseteq Digests /\ deletedSnapshots \subseteq Digests
  /\ pressure \in 0..3 /\ lastAdmissionPressure \in 0..3
  /\ sealed \in BOOLEAN /\ settlement \in {"idle", "retry", "failed", "sealed"}
  /\ checkedDownload \subseteq Digests /\ postSealGrowth \in BOOLEAN
  /\ cacheMetadata \in 0..2

SingleGlobalImportOwner == Cardinality(materializing) <= 1
MaterializationHasReservation == materializing \subseteq reservations
ReferencedContentNotDeleted == deletedContent \cap ReferencedDigests = {}
ReferencedSnapshotNotDeleted == deletedSnapshots \cap ReferencedDigests = {}
CriticalWatermarkRejectsAdmission == lastAdmissionPressure < 2
CommittedMaterialHasBothFacts == \A d \in ReferencedDigests: d \in content \cap snapshots
NoGrowthAfterSeal == ~postSealGrowth

Spec == Init /\ [][Next]_vars

=============================================================================
