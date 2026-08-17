----------------------- MODULE ControllerDomainControl -----------------------
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Domains, MaxResource,
          EnforceReady, EnforceIdentity, EnforceWatermark, EnforceAdmission,
          EnforceSessionReplacement, EnforceRevoke, EnforceIsolation,
          EnforceSystemSeparation, EnforceCommandFence

VARIABLES phase, epoch, watermark, acknowledgedWatermark, sequence,
          reservation, lease, capabilities, mutationOwner,
          unsafeReadyMutation, staleSettlement, postRevokeAdmission,
          crossDomainMutation, systemVisible, unsafeSequenceMutation

vars == <<phase, epoch, watermark, acknowledgedWatermark, sequence,
          reservation, lease, capabilities, mutationOwner,
          unsafeReadyMutation, staleSettlement, postRevokeAdmission,
          crossDomainMutation, systemVisible, unsafeSequenceMutation>>

Other(d) == CHOOSE other \in Domains: other # d

RECURSIVE DomainSum(_,_)
DomainSum(domains, values) ==
  IF domains = {} THEN 0
  ELSE LET d == CHOOSE item \in domains: TRUE IN
    values[d] + DomainSum(domains \ {d}, values)

SumReservations == DomainSum(Domains, reservation)

Init ==
  /\ Cardinality(Domains) >= 2
  /\ phase = [d \in Domains |-> "RECONCILING"]
  /\ epoch = [d \in Domains |-> 1]
  /\ watermark = [d \in Domains |-> 1]
  /\ acknowledgedWatermark = [d \in Domains |-> 0]
  /\ sequence = [d \in Domains |-> 0]
  /\ reservation = [d \in Domains |-> 0]
  /\ lease = [d \in Domains |-> FALSE]
  /\ capabilities = [d \in Domains |-> 0]
  /\ mutationOwner = [d \in Domains |-> d]
  /\ unsafeReadyMutation = [d \in Domains |-> FALSE]
  /\ staleSettlement = [d \in Domains |-> FALSE]
  /\ postRevokeAdmission = [d \in Domains |-> FALSE]
  /\ crossDomainMutation = FALSE
  /\ systemVisible = FALSE
  /\ unsafeSequenceMutation = [d \in Domains |-> FALSE]

ControllerReady(d) ==
  /\ phase[d] = "RECONCILING"
  /\ IF EnforceWatermark
        THEN acknowledgedWatermark' = [acknowledgedWatermark EXCEPT ![d] = watermark[d]]
        ELSE acknowledgedWatermark' = [acknowledgedWatermark EXCEPT ![d] = 0]
  /\ staleSettlement' = [staleSettlement EXCEPT ![d] = (@ \/ acknowledgedWatermark'[d] # watermark[d])]
  /\ phase' = [phase EXCEPT ![d] = "READY"]
  /\ UNCHANGED <<epoch, watermark, sequence, reservation, lease, capabilities,
                 mutationOwner, unsafeReadyMutation, postRevokeAdmission,
                 crossDomainMutation, systemVisible, unsafeSequenceMutation>>

Allocate(d, expectedEpoch, expectedSequence) ==
  /\ IF phase[d] = "REVOKED"
        THEN ~EnforceRevoke
        ELSE IF phase[d] = "DRAINING" THEN FALSE
        ELSE IF EnforceReady THEN phase[d] = "READY" ELSE phase[d] \in {"RECONCILING", "READY"}
  /\ IF EnforceRevoke THEN phase[d] # "REVOKED" ELSE TRUE
	/\ IF EnforceCommandFence
	      THEN expectedEpoch = epoch[d] /\ expectedSequence = sequence[d]
	      ELSE TRUE
	/\ IF EnforceWatermark THEN acknowledgedWatermark[d] = watermark[d] ELSE TRUE
  /\ IF EnforceAdmission THEN Cardinality(Domains) + SumReservations < MaxResource ELSE TRUE
  /\ watermark[d] < MaxResource
  /\ reservation' = [reservation EXCEPT ![d] = @ + 1]
  /\ sequence' = [sequence EXCEPT ![d] = @ + 1]
  /\ watermark' = [watermark EXCEPT ![d] = @ + 1]
  /\ unsafeReadyMutation' = [unsafeReadyMutation EXCEPT ![d] = (@ \/ phase[d] # "READY")]
  /\ postRevokeAdmission' = [postRevokeAdmission EXCEPT ![d] = (@ \/ phase[d] = "REVOKED")]
  /\ unsafeSequenceMutation' = [unsafeSequenceMutation EXCEPT ![d] =
        (@ \/ (~EnforceCommandFence /\ (expectedEpoch # epoch[d] \/ expectedSequence # sequence[d])))]
  /\ UNCHANGED <<phase, epoch, acknowledgedWatermark, lease, capabilities,
                 mutationOwner, staleSettlement, crossDomainMutation, systemVisible>>

AcquireLease(d) ==
  /\ phase[d] = "READY"
  /\ reservation[d] > 0
  /\ ~lease[d]
  /\ watermark[d] < MaxResource
  /\ lease' = [lease EXCEPT ![d] = TRUE]
  /\ sequence' = [sequence EXCEPT ![d] = @ + 1]
  /\ watermark' = [watermark EXCEPT ![d] = @ + 1]
  /\ UNCHANGED <<phase, epoch, acknowledgedWatermark, reservation,
                 capabilities, mutationOwner, unsafeReadyMutation,
                 staleSettlement, postRevokeAdmission, crossDomainMutation, systemVisible, unsafeSequenceMutation>>

RenewLease(d) ==
  /\ phase[d] = "READY"
  /\ lease[d]
  /\ watermark[d] < MaxResource
  /\ sequence' = [sequence EXCEPT ![d] = @ + 1]
  /\ watermark' = [watermark EXCEPT ![d] = @ + 1]
  /\ UNCHANGED <<phase, epoch, acknowledgedWatermark, reservation, lease,
                 capabilities, mutationOwner, unsafeReadyMutation,
                 staleSettlement, postRevokeAdmission, crossDomainMutation, systemVisible, unsafeSequenceMutation>>

ReleaseLease(d) ==
  /\ phase[d] = "READY"
  /\ lease[d]
  /\ watermark[d] < MaxResource
  /\ lease' = [lease EXCEPT ![d] = FALSE]
  /\ capabilities' = [capabilities EXCEPT ![d] = 0]
  /\ sequence' = [sequence EXCEPT ![d] = @ + 1]
  /\ watermark' = [watermark EXCEPT ![d] = @ + 1]
  /\ UNCHANGED <<phase, epoch, acknowledgedWatermark, reservation,
                 mutationOwner, unsafeReadyMutation, staleSettlement,
                 postRevokeAdmission, crossDomainMutation, systemVisible, unsafeSequenceMutation>>

OpenSession(d) ==
  /\ phase[d] = "READY" /\ lease[d]
  /\ IF EnforceSessionReplacement
        THEN capabilities' = [capabilities EXCEPT ![d] = 1]
        ELSE capabilities' = [capabilities EXCEPT ![d] = @ + 1]
  /\ UNCHANGED <<phase, epoch, watermark, acknowledgedWatermark, sequence,
                 reservation, lease, mutationOwner, unsafeReadyMutation,
                 staleSettlement, postRevokeAdmission, crossDomainMutation, systemVisible, unsafeSequenceMutation>>

Disconnect(d) ==
  /\ phase[d] = "READY" /\ epoch[d] < 2
  /\ phase' = [phase EXCEPT ![d] = "RECONCILING"]
  /\ epoch' = [epoch EXCEPT ![d] = @ + 1]
  /\ capabilities' = [capabilities EXCEPT ![d] = 0]
  /\ acknowledgedWatermark' = [acknowledgedWatermark EXCEPT ![d] = 0]
  /\ UNCHANGED <<watermark, sequence, reservation, lease, mutationOwner,
                 unsafeReadyMutation, staleSettlement, postRevokeAdmission,
                 crossDomainMutation, systemVisible, unsafeSequenceMutation>>

DeleteAllocation(d) ==
  /\ phase[d] = "READY"
  /\ reservation[d] > 0
  /\ ~lease[d]
  /\ watermark[d] < MaxResource
  /\ reservation' = [reservation EXCEPT ![d] = @ - 1]
  /\ sequence' = [sequence EXCEPT ![d] = @ + 1]
  /\ watermark' = [watermark EXCEPT ![d] = @ + 1]
	/\ UNCHANGED <<phase, epoch, acknowledgedWatermark, lease, capabilities,
	               mutationOwner, unsafeReadyMutation, staleSettlement,
	               postRevokeAdmission, crossDomainMutation, systemVisible, unsafeSequenceMutation>>

\* A replacement connection can revoke the old leases and advance the
\* registration inventory while the controller is still in the READY phase.
\* Admission must therefore require a fresh ControllerReady acknowledgement.
InventoryMutation(d) ==
	/\ phase[d] = "READY"
	/\ watermark[d] = 1
	/\ watermark' = [watermark EXCEPT ![d] = @ + 1]
	/\ sequence' = [sequence EXCEPT ![d] = @ + 1]
	/\ lease' = [lease EXCEPT ![d] = FALSE]
	/\ capabilities' = [capabilities EXCEPT ![d] = 0]
	/\ UNCHANGED <<phase, epoch, acknowledgedWatermark, reservation,
	               mutationOwner, unsafeReadyMutation, staleSettlement,
	               postRevokeAdmission, crossDomainMutation, systemVisible, unsafeSequenceMutation>>

ControllerResettle(d) ==
	/\ phase[d] = "READY"
	/\ acknowledgedWatermark[d] # watermark[d]
	/\ acknowledgedWatermark' = [acknowledgedWatermark EXCEPT ![d] = watermark[d]]
	/\ UNCHANGED <<phase, epoch, watermark, sequence, reservation, lease, capabilities,
	               mutationOwner, unsafeReadyMutation, staleSettlement,
	               postRevokeAdmission, crossDomainMutation, systemVisible, unsafeSequenceMutation>>

Revoke(d) ==
  /\ phase[d] # "REVOKED"
  /\ phase' = [phase EXCEPT ![d] = "DRAINING"]
  /\ lease' = [lease EXCEPT ![d] = FALSE]
  /\ capabilities' = [capabilities EXCEPT ![d] = 0]
  /\ UNCHANGED <<epoch, watermark, acknowledgedWatermark, sequence, reservation,
                 mutationOwner, unsafeReadyMutation, staleSettlement,
                 postRevokeAdmission, crossDomainMutation, systemVisible, unsafeSequenceMutation>>

CompleteRevoke(d) ==
  /\ phase[d] = "DRAINING"
  /\ phase' = [phase EXCEPT ![d] = "REVOKED"]
  /\ UNCHANGED <<epoch, watermark, acknowledgedWatermark, sequence, reservation,
                 lease, capabilities, mutationOwner, unsafeReadyMutation, staleSettlement,
                 postRevokeAdmission, crossDomainMutation, systemVisible, unsafeSequenceMutation>>

CrossDomainCommand(d) ==
  /\ phase[d] = "READY"
  /\ ~EnforceIdentity
  /\ mutationOwner' = [mutationOwner EXCEPT ![d] = Other(d)]
  /\ UNCHANGED <<phase, epoch, watermark, acknowledgedWatermark, sequence,
                 reservation, lease, capabilities, unsafeReadyMutation,
                 staleSettlement, postRevokeAdmission, crossDomainMutation, systemVisible, unsafeSequenceMutation>>

CrossDomainFailure(d) ==
  /\ ~EnforceIsolation
  /\ reservation' = [reservation EXCEPT ![d] = @ + 1, ![Other(d)] = @ + 1]
  /\ crossDomainMutation' = TRUE
  /\ UNCHANGED <<phase, epoch, watermark, acknowledgedWatermark, sequence,
                 lease, capabilities, mutationOwner, unsafeReadyMutation,
                 staleSettlement, postRevokeAdmission, systemVisible, unsafeSequenceMutation>>

ExposeSystem ==
  /\ ~EnforceSystemSeparation
  /\ systemVisible' = TRUE
  /\ UNCHANGED <<phase, epoch, watermark, acknowledgedWatermark, sequence,
                 reservation, lease, capabilities, mutationOwner,
                 unsafeReadyMutation, staleSettlement, postRevokeAdmission,
                 crossDomainMutation, unsafeSequenceMutation>>

Next == \E d \in Domains:
  ControllerReady(d) \/
  (\E expectedEpoch \in 0..2: \E expectedSequence \in 0..MaxResource: Allocate(d, expectedEpoch, expectedSequence)) \/
  InventoryMutation(d) \/ ControllerResettle(d) \/ DeleteAllocation(d) \/ AcquireLease(d) \/ RenewLease(d) \/ ReleaseLease(d) \/ OpenSession(d) \/
  Disconnect(d) \/ Revoke(d) \/ CompleteRevoke(d) \/ CrossDomainCommand(d) \/ CrossDomainFailure(d)
  \/ ExposeSystem

TypeOK ==
  /\ phase \in [Domains -> {"RECONCILING", "READY", "DRAINING", "REVOKED"}]
  /\ epoch \in [Domains -> Nat]
  /\ watermark \in [Domains -> Nat]
  /\ reservation \in [Domains -> Nat]
  /\ capabilities \in [Domains -> Nat]
  /\ unsafeSequenceMutation \in [Domains -> BOOLEAN]

NoCommandBeforeReady == \A d \in Domains: ~unsafeReadyMutation[d]
ExactRegistrationIdentity == \A d \in Domains: mutationOwner[d] = d
ExactInventorySettlement == \A d \in Domains: ~staleSettlement[d]
AggregateNoOvercommit == Cardinality(Domains) + SumReservations <= MaxResource
SingleCurrentCapability == \A d \in Domains: capabilities[d] <= 1
RevokeIsTerminal == \A d \in Domains: ~postRevokeAdmission[d]
DomainNonInterference == ~crossDomainMutation
SystemExcludedFromInventory == ~systemVisible
InventoryRevisionTracksMutations == \A d \in Domains: watermark[d] = sequence[d] + 1
CommandFenceExact == \A d \in Domains: ~unsafeSequenceMutation[d]

EventuallySettled == \A d \in Domains: phase[d] = "RECONCILING" ~> phase[d] \in {"READY", "REVOKED"}

RevokeEventuallySettled == \A d \in Domains: phase[d] = "DRAINING" ~> phase[d] = "REVOKED"

InventoryEventuallyResettled ==
  \A d \in Domains: (phase[d] = "READY" /\ acknowledgedWatermark[d] # watermark[d])
    ~> (phase[d] # "READY" \/ acknowledgedWatermark[d] = watermark[d])

Spec == Init /\ [][Next]_vars /\ \A d \in Domains:
  WF_vars(ControllerReady(d)) /\ WF_vars(ControllerResettle(d)) /\
  WF_vars(Revoke(d)) /\ WF_vars(CompleteRevoke(d))

=============================================================================
