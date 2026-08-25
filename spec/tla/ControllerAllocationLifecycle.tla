--------------------- MODULE ControllerAllocationLifecycle -------------------
EXTENDS Naturals

CONSTANTS MaxRevision, EnforceReady, EnforceWatermark, EnforceCommandFence,
          EnforceAdmission, EnforceSessionReplacement, EnforceRevoke,
          EnforceLeaseDeleteFence

VARIABLES phase, epoch, watermark, acknowledgedWatermark, sequence,
          capacityAvailable, allocated, lease, capabilities,
          unsafeReadyMutation, staleSettlement, unsafeSequenceMutation,
          unsafeAdmission, unsafeLeaseDelete, postRevokeAdmission

vars == <<phase, epoch, watermark, acknowledgedWatermark, sequence,
          capacityAvailable, allocated, lease, capabilities,
          unsafeReadyMutation, staleSettlement, unsafeSequenceMutation,
          unsafeAdmission, unsafeLeaseDelete, postRevokeAdmission>>

Init ==
  /\ phase = "RECONCILING" /\ epoch = 1
  /\ watermark = 1 /\ acknowledgedWatermark = 0 /\ sequence = 0
  /\ capacityAvailable = TRUE /\ allocated = FALSE /\ lease = FALSE
  /\ capabilities = 0 /\ unsafeReadyMutation = FALSE
  /\ staleSettlement = FALSE /\ unsafeSequenceMutation = FALSE
  /\ unsafeAdmission = FALSE /\ unsafeLeaseDelete = FALSE
  /\ postRevokeAdmission = FALSE

ControllerReady ==
  /\ phase = "RECONCILING"
  /\ acknowledgedWatermark' = IF EnforceWatermark THEN watermark ELSE 0
  /\ staleSettlement' = (staleSettlement \/ acknowledgedWatermark' # watermark)
  /\ phase' = "READY"
  /\ UNCHANGED <<epoch, watermark, sequence, capacityAvailable, allocated,
       lease, capabilities, unsafeReadyMutation, unsafeSequenceMutation,
       unsafeAdmission, unsafeLeaseDelete, postRevokeAdmission>>

SetCapacityAvailable(value) ==
  /\ capacityAvailable # value
  /\ capacityAvailable' = value
  /\ UNCHANGED <<phase, epoch, watermark, acknowledgedWatermark, sequence,
       allocated, lease, capabilities, unsafeReadyMutation, staleSettlement,
       unsafeSequenceMutation, unsafeAdmission, unsafeLeaseDelete,
       postRevokeAdmission>>

Allocate(expectedEpoch, expectedSequence) ==
  /\ IF phase = "REVOKED" THEN ~EnforceRevoke
        ELSE IF EnforceReady THEN phase = "READY"
        ELSE phase \in {"RECONCILING", "READY"}
  /\ ~allocated
  /\ IF EnforceCommandFence
        THEN expectedEpoch = epoch /\ expectedSequence = sequence
        ELSE TRUE
  /\ IF phase = "READY" /\ EnforceWatermark
        THEN acknowledgedWatermark = watermark
        ELSE TRUE
  /\ IF EnforceAdmission THEN capacityAvailable ELSE TRUE
  /\ watermark < MaxRevision
  /\ allocated' = TRUE /\ watermark' = watermark + 1 /\ sequence' = sequence + 1
  /\ unsafeReadyMutation' = (unsafeReadyMutation \/ phase # "READY")
  /\ unsafeSequenceMutation' = (unsafeSequenceMutation \/
       (~EnforceCommandFence /\ (expectedEpoch # epoch \/ expectedSequence # sequence)))
  /\ unsafeAdmission' = (unsafeAdmission \/ ~capacityAvailable)
  /\ postRevokeAdmission' = (postRevokeAdmission \/ phase = "REVOKED")
  /\ UNCHANGED <<phase, epoch, acknowledgedWatermark, capacityAvailable,
       lease, capabilities, staleSettlement, unsafeLeaseDelete>>

AcquireLease ==
  /\ phase = "READY" /\ allocated /\ ~lease
  /\ watermark < MaxRevision
  /\ lease' = TRUE /\ watermark' = watermark + 1 /\ sequence' = sequence + 1
  /\ UNCHANGED <<phase, epoch, acknowledgedWatermark, capacityAvailable,
       allocated, capabilities, unsafeReadyMutation, staleSettlement,
       unsafeSequenceMutation, unsafeAdmission, unsafeLeaseDelete,
       postRevokeAdmission>>

RenewLease ==
  /\ phase = "READY" /\ lease /\ watermark < MaxRevision
  /\ watermark' = watermark + 1 /\ sequence' = sequence + 1
  /\ UNCHANGED <<phase, epoch, acknowledgedWatermark, capacityAvailable,
       allocated, lease, capabilities, unsafeReadyMutation, staleSettlement,
       unsafeSequenceMutation, unsafeAdmission, unsafeLeaseDelete,
       postRevokeAdmission>>

ReleaseLease ==
  /\ phase = "READY" /\ lease /\ watermark < MaxRevision
  /\ lease' = FALSE /\ capabilities' = 0
  /\ watermark' = watermark + 1 /\ sequence' = sequence + 1
  /\ UNCHANGED <<phase, epoch, acknowledgedWatermark, capacityAvailable,
       allocated, unsafeReadyMutation, staleSettlement, unsafeSequenceMutation,
       unsafeAdmission, unsafeLeaseDelete, postRevokeAdmission>>

OpenSession ==
  /\ phase = "READY" /\ lease
  /\ capabilities' = IF EnforceSessionReplacement THEN 1 ELSE capabilities + 1
  /\ UNCHANGED <<phase, epoch, watermark, acknowledgedWatermark, sequence,
       capacityAvailable, allocated, lease, unsafeReadyMutation,
       staleSettlement, unsafeSequenceMutation, unsafeAdmission,
       unsafeLeaseDelete, postRevokeAdmission>>

DeleteAllocation ==
  /\ phase = "READY" /\ allocated
  /\ IF EnforceLeaseDeleteFence THEN ~lease ELSE TRUE
  /\ watermark < MaxRevision
  /\ allocated' = FALSE /\ lease' = FALSE /\ capabilities' = 0
  /\ watermark' = watermark + 1 /\ sequence' = sequence + 1
  /\ unsafeLeaseDelete' = (unsafeLeaseDelete \/ lease)
  /\ UNCHANGED <<phase, epoch, acknowledgedWatermark, capacityAvailable,
       unsafeReadyMutation, staleSettlement, unsafeSequenceMutation,
       unsafeAdmission, postRevokeAdmission>>

InventoryMutation ==
  /\ phase = "READY" /\ watermark < MaxRevision
  /\ watermark' = watermark + 1 /\ sequence' = sequence + 1
  /\ lease' = FALSE /\ capabilities' = 0
  /\ UNCHANGED <<phase, epoch, acknowledgedWatermark, capacityAvailable,
       allocated, unsafeReadyMutation, staleSettlement, unsafeSequenceMutation,
       unsafeAdmission, unsafeLeaseDelete, postRevokeAdmission>>

ControllerResettle ==
  /\ phase = "READY" /\ acknowledgedWatermark # watermark
  /\ acknowledgedWatermark' = watermark
  /\ UNCHANGED <<phase, epoch, watermark, sequence, capacityAvailable,
       allocated, lease, capabilities, unsafeReadyMutation, staleSettlement,
       unsafeSequenceMutation, unsafeAdmission, unsafeLeaseDelete,
       postRevokeAdmission>>

Disconnect ==
  /\ phase = "READY" /\ epoch < 2
  /\ phase' = "RECONCILING" /\ epoch' = epoch + 1
  /\ capabilities' = 0 /\ acknowledgedWatermark' = 0
  /\ UNCHANGED <<watermark, sequence, capacityAvailable, allocated, lease,
       unsafeReadyMutation, staleSettlement, unsafeSequenceMutation,
       unsafeAdmission, unsafeLeaseDelete, postRevokeAdmission>>

Revoke ==
  /\ phase \in {"READY", "RECONCILING"}
  /\ phase' = "DRAINING" /\ lease' = FALSE /\ capabilities' = 0
  /\ UNCHANGED <<epoch, watermark, acknowledgedWatermark, sequence,
       capacityAvailable, allocated, unsafeReadyMutation, staleSettlement,
       unsafeSequenceMutation, unsafeAdmission, unsafeLeaseDelete,
       postRevokeAdmission>>

CompleteRevoke ==
  /\ phase = "DRAINING"
  /\ phase' = "REVOKED"
  /\ UNCHANGED <<epoch, watermark, acknowledgedWatermark, sequence,
       capacityAvailable, allocated, lease, capabilities, unsafeReadyMutation,
       staleSettlement, unsafeSequenceMutation, unsafeAdmission,
       unsafeLeaseDelete, postRevokeAdmission>>

Next == ControllerReady \/ ControllerResettle \/ InventoryMutation \/
  AcquireLease \/ RenewLease \/ ReleaseLease \/ OpenSession \/
  DeleteAllocation \/ Disconnect \/ Revoke \/ CompleteRevoke \/
  (\E value \in BOOLEAN: SetCapacityAvailable(value)) \/
  (\E expectedEpoch \in 0..2: \E expectedSequence \in 0..MaxRevision:
    Allocate(expectedEpoch, expectedSequence))

TypeOK ==
  /\ phase \in {"RECONCILING", "READY", "DRAINING", "REVOKED"}
  /\ epoch \in 1..2 /\ watermark \in 1..MaxRevision
  /\ acknowledgedWatermark \in 0..MaxRevision /\ sequence \in 0..MaxRevision
  /\ capacityAvailable \in BOOLEAN /\ allocated \in BOOLEAN /\ lease \in BOOLEAN
  /\ capabilities \in Nat

NoCommandBeforeReady == ~unsafeReadyMutation
ExactInventorySettlement == ~staleSettlement
InventoryRevisionTracksMutations == watermark = sequence + 1
CommandFenceExact == ~unsafeSequenceMutation
NoAllocationWithoutAdmission == ~unsafeAdmission
LeaseBlocksDelete == ~unsafeLeaseDelete
SingleCurrentCapability == capabilities <= 1
CapabilityRequiresCurrentLease == capabilities > 0 => phase = "READY" /\ lease
RevokeIsTerminal == ~postRevokeAdmission

EventuallySettled == phase = "RECONCILING" ~> phase \in {"READY", "REVOKED"}
InventoryEventuallyResettled ==
  (phase = "READY" /\ acknowledgedWatermark # watermark)
    ~> (phase # "READY" \/ acknowledgedWatermark = watermark)
RevokeEventuallySettled == phase = "DRAINING" ~> phase = "REVOKED"

Spec == Init /\ [][Next]_vars /\ WF_vars(ControllerReady) /\
  WF_vars(ControllerResettle) /\ WF_vars(Revoke) /\ WF_vars(CompleteRevoke)

=============================================================================
