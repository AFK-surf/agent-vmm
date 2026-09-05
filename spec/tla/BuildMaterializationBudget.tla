---------------------- MODULE BuildMaterializationBudget ----------------------
EXTENDS Naturals, FiniteSets

CONSTANTS AtomicLocalReservation, WaitForBuildSlot, FenceCanceledWaiter

\* Existing two build slots cover native execution AND output consumption.
\* This model isolates their local budget/slot protocol, not OCI content or
\* streaming upload state. GuestDiskBudget owns full physical accounting.
\* Anchors: Workspace.RunArchive; Manager.LockBuild; guardOCIPhysicalGrowth;
\* Transaction.ReserveImportHeadroom; guardOCIProjectGrowth.
\* Finite native work and metadata each grow one unit, with sufficient real
\* capacity. There is no promise under actual capacity exhaustion. A failed
\* settlement seals growth; restart clears volatile waiters and orphan holds.
\* Cancellation/timeout/disconnect share Cancel; retry is a NEW request, never
\* an implicit repeat of materialization. No network-progress/liveness claim.

Workers == {"a", "b"}
None == "none"
D == 12
W == 2
E == 1
M == 1
Terminal == {"completed", "canceled", "failed"}

VARIABLES phase, owner, transaction, used, reserved, canceled,
          metadataDone, falseMetadataRejection, rejectedBuild, staleRun, sealed
vars == <<phase, owner, transaction, used, reserved, canceled,
          metadataDone, falseMetadataRejection, rejectedBuild, staleRun, sealed>>

Init ==
 /\ phase = [b \in Workers |-> "building"]
 /\ owner = None /\ transaction = FALSE
 /\ used = 3 /\ reserved = 0 /\ canceled = {}
 /\ metadataDone = FALSE /\ falseMetadataRejection = FALSE
 /\ rejectedBuild = FALSE /\ staleRun = FALSE /\ sealed = FALSE

FinishNative(b) ==
 /\ phase[b] = "building"
 /\ phase' = [phase EXCEPT ![b] = "waiting"]
 /\ UNCHANGED <<owner, transaction, used, reserved, canceled, metadataDone,
                 falseMetadataRejection, rejectedBuild, staleRun, sealed>>

Cancel(b) ==
 /\ phase[b] \notin Terminal
 /\ b \notin canceled
 /\ canceled' = canceled \cup {b}
 /\ phase' = IF phase[b] = "building" THEN [phase EXCEPT ![b] = "canceled"] ELSE phase
 /\ UNCHANGED <<owner, transaction, used, reserved, metadataDone,
                 falseMetadataRejection, rejectedBuild, staleRun, sealed>>

SettleCanceledWaiter(b) ==
 /\ phase[b] = "waiting" /\ b \in canceled
 /\ phase' = [phase EXCEPT ![b] = "canceled"]
 /\ UNCHANGED <<owner, transaction, used, reserved, canceled, metadataDone,
                 falseMetadataRejection, rejectedBuild, staleRun, sealed>>

Acquire(b) ==
 /\ phase[b] = "waiting" /\ owner = None /\ ~sealed
 /\ (~FenceCanceledWaiter \/ b \notin canceled)
 /\ owner' = b /\ phase' = [phase EXCEPT ![b] = "reserved"]
 /\ transaction' = AtomicLocalReservation
 /\ reserved' = D - used - W - E - M
 /\ staleRun' = (staleRun \/ b \in canceled)
 /\ UNCHANGED <<used, canceled, metadataDone, falseMetadataRejection, rejectedBuild, sealed>>

RejectOccupied(b) ==
 /\ ~WaitForBuildSlot /\ phase[b] = "waiting" /\ owner # None
 /\ rejectedBuild' = TRUE /\ phase' = [phase EXCEPT ![b] = "failed"]
 /\ UNCHANGED <<owner, transaction, used, reserved, canceled, metadataDone,
                 falseMetadataRejection, staleRun, sealed>>

Convert(b) ==
 /\ owner = b /\ phase[b] = "reserved"
 /\ transaction' = TRUE /\ reserved' = 0
 /\ phase' = [phase EXCEPT ![b] = "materializing"]
 /\ UNCHANGED <<owner, used, canceled, metadataDone, falseMetadataRejection,
                 rejectedBuild, staleRun, sealed>>

Settle(b, failed) ==
 /\ owner = b /\ phase[b] = "materializing"
 /\ used' = used + 1
 /\ owner' = None /\ transaction' = FALSE
 /\ phase' = [phase EXCEPT ![b] = IF failed THEN "failed" ELSE "completed"]
 /\ sealed' = failed
 /\ UNCHANGED <<reserved, canceled, metadataDone, falseMetadataRejection, rejectedBuild, staleRun>>

Metadata ==
 /\ ~transaction /\ ~sealed /\ ~metadataDone
 /\ metadataDone' = TRUE
 /\ IF used + reserved + W + E + M < D
       THEN /\ used' = used + 1 /\ UNCHANGED falseMetadataRejection
       ELSE /\ falseMetadataRejection' = TRUE /\ UNCHANGED used
 /\ UNCHANGED <<phase, owner, transaction, reserved, canceled, rejectedBuild, staleRun, sealed>>

Restart ==
 /\ owner # None \/ sealed
 /\ phase' = [b \in Workers |-> IF phase[b] \in Terminal THEN phase[b] ELSE "failed"]
 /\ owner' = None /\ transaction' = FALSE /\ reserved' = 0 /\ sealed' = FALSE
 /\ UNCHANGED <<used, canceled, metadataDone, falseMetadataRejection, rejectedBuild, staleRun>>

Next == Metadata \/ Restart \/ \E b \in Workers:
 FinishNative(b) \/ Cancel(b) \/ SettleCanceledWaiter(b) \/ Acquire(b) \/
 RejectOccupied(b) \/ Convert(b) \/ Settle(b, FALSE) \/ Settle(b, TRUE)

TypeOK ==
 /\ phase \in [Workers -> {"building", "waiting", "reserved", "materializing"} \cup Terminal]
 /\ owner \in Workers \cup {None} /\ transaction \in BOOLEAN
 /\ used \in Nat /\ reserved \in Nat /\ canceled \subseteq Workers
 /\ metadataDone \in BOOLEAN /\ falseMetadataRejection \in BOOLEAN
 /\ rejectedBuild \in BOOLEAN /\ staleRun \in BOOLEAN /\ sealed \in BOOLEAN
DiskInvariant == used + reserved + W + E + M <= D
NoSpuriousMetadataRejection == ~falseMetadataRejection
AdmittedBuildWaits == ~rejectedBuild
CanceledWaiterDoesNotRun == ~staleRun
SingleMaterializer == Cardinality({b \in Workers: phase[b] \in {"reserved", "materializing"}}) <= 1

Spec == Init /\ [][Next]_vars
=============================================================================
