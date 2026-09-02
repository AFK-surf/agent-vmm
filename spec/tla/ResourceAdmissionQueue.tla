------------------------ MODULE ResourceAdmissionQueue -----------------------
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS EnforceExactRelease, EnforceCurrentFence, EnforceReleaseConfirmation,
          EnforceTerminalCompensation, EnforceDeadlineRevalidation

Tasks == {1, 2}
Generations == {1, 2}
Epochs == {1, 2}
Identities == {<<t, g, e>> : t \in Tasks, g \in Generations, e \in Epochs}
Current(t) == <<t, 2, 2>>
Stale(t) == <<t, 1, 1>>

CPUGuaranteeBudget == 2
CPUMaxOversellBudget == 4
MemoryBudget == 2
PIDBudget == 2
WritableBudget == 2

VARIABLES queue, deadlines, now, hostRows, charged, guestReserved, receipts,
          admitted, releasing, guestTombstones, pendingConfirm, hostConfirmed,
          stopped, revoked, expired, reserveResponseLost, releaseResponseLost,
          confirmResponseLost, lastClaimedTask, lastRequestedRelease, lastFreed

VARIABLES leaseProjections, leaseReady,
          lastExpiredTask, lastExpiredDeadline

vars == <<queue, deadlines, now, hostRows, charged, guestReserved, receipts,
          admitted, releasing, guestTombstones, pendingConfirm, hostConfirmed,
          stopped, revoked, expired, reserveResponseLost, releaseResponseLost,
          confirmResponseLost, lastClaimedTask, lastRequestedRelease, lastFreed>>

allVars == <<vars, leaseProjections, leaseReady,
             lastExpiredTask, lastExpiredDeadline>>
LeaseUnchanged == UNCHANGED <<leaseProjections, leaseReady,
                              lastExpiredTask, lastExpiredDeadline>>

Fits(ids) ==
  /\ Cardinality(ids) <= CPUGuaranteeBudget
  /\ 2 * Cardinality(ids) <= CPUMaxOversellBudget
  /\ Cardinality(ids) <= MemoryBudget
  /\ Cardinality(ids) <= PIDBudget
  /\ Cardinality(ids) <= WritableBudget

Init ==
  /\ queue = <<1, 2>>
  /\ deadlines = [t \in Tasks |-> 3]
  /\ now = 0
  /\ hostRows = {} /\ charged = {} /\ guestReserved = {} /\ receipts = {}
  /\ admitted = {} /\ releasing = {} /\ guestTombstones = {}
  /\ pendingConfirm = {} /\ hostConfirmed = {}
  /\ stopped = {} /\ revoked = {} /\ expired = {}
  /\ reserveResponseLost = {} /\ releaseResponseLost = {} /\ confirmResponseLost = {}
  /\ lastClaimedTask = 0 /\ lastRequestedRelease = <<0, 0, 0>> /\ lastFreed = <<0, 0, 0>>
  /\ leaseProjections = {} /\ leaseReady = {}
  /\ lastExpiredTask = 0 /\ lastExpiredDeadline = 0

ClaimHead ==
  /\ Len(queue) > 0
  /\ LET task == Head(queue) IN
     LET identity == IF EnforceCurrentFence THEN Current(task) ELSE Stale(task) IN
       /\ task \notin stopped \cup revoked \cup expired
       /\ now < deadlines[task]
       /\ Fits(charged \cup {identity})
       /\ queue' = Tail(queue)
       /\ hostRows' = hostRows \cup {identity}
       /\ charged' = charged \cup {identity}
       /\ lastClaimedTask' = task
  /\ UNCHANGED <<deadlines, now, guestReserved, receipts, admitted, releasing,
                  guestTombstones, pendingConfirm, hostConfirmed, stopped, revoked,
                  expired, reserveResponseLost, releaseResponseLost,
                  confirmResponseLost, lastRequestedRelease, lastFreed>>
  /\ LeaseUnchanged

StartLease ==
  /\ \E i \in admitted:
       /\ ~\E p \in leaseProjections: p[1] = i[1]
       /\ i[1] \notin leaseReady \cup stopped \cup revoked
       /\ now < deadlines[i[1]]
       /\ leaseProjections' = leaseProjections \cup {i}
  /\ UNCHANGED <<vars, leaseReady,
                  lastExpiredTask, lastExpiredDeadline>>

RenewLease ==
  /\ \E i \in leaseProjections:
       /\ now < deadlines[i[1]]
       /\ deadlines' = [deadlines EXCEPT ![i[1]] = 4]
  /\ UNCHANGED <<queue, now, hostRows, charged, guestReserved, receipts,
                  admitted, releasing, guestTombstones, pendingConfirm,
                  hostConfirmed, stopped, revoked, expired,
                  reserveResponseLost, releaseResponseLost,
                  confirmResponseLost, lastClaimedTask, lastRequestedRelease,
                  lastFreed, leaseProjections, leaseReady,
                  lastExpiredTask, lastExpiredDeadline>>

ExplicitReleaseLease ==
  /\ \E i \in leaseProjections:
       /\ leaseProjections' = leaseProjections \ {i}
       /\ leaseReady' = leaseReady \cup {i[1]}
  /\ UNCHANGED <<vars, lastExpiredTask, lastExpiredDeadline>>

ExpireLease ==
  /\ \E i \in leaseProjections:
       /\ (~EnforceDeadlineRevalidation \/ deadlines[i[1]] <= now)
       /\ leaseProjections' = leaseProjections \ {i}
       /\ leaseReady' = leaseReady \cup {i[1]}
       /\ lastExpiredTask' = i[1]
       /\ lastExpiredDeadline' = deadlines[i[1]]
  /\ UNCHANGED <<queue, deadlines, now, hostRows, charged, guestReserved,
                  receipts, admitted, releasing, guestTombstones,
                  pendingConfirm, hostConfirmed, stopped, revoked, expired,
                  reserveResponseLost, releaseResponseLost,
                  confirmResponseLost, lastClaimedTask, lastRequestedRelease,
                  lastFreed>>

GuestReserve ==
  /\ \E i \in hostRows \ guestReserved:
       /\ i[1] \notin stopped \cup revoked
       /\ i \notin guestTombstones \cup pendingConfirm \cup hostConfirmed
       /\ (~EnforceCurrentFence \/ i = Current(i[1]))
       /\ guestReserved' = guestReserved \cup {i}
       /\ UNCHANGED <<queue, deadlines, now, hostRows, charged, receipts, admitted,
                       releasing, guestTombstones, pendingConfirm, hostConfirmed,
                       stopped, revoked, expired, reserveResponseLost,
                       releaseResponseLost, confirmResponseLost, lastClaimedTask,
                       lastRequestedRelease, lastFreed>>
       /\ LeaseUnchanged

LoseReserveResponse ==
  /\ \E i \in guestReserved \ receipts:
       /\ reserveResponseLost' = reserveResponseLost \cup {i}
       /\ UNCHANGED <<queue, deadlines, now, hostRows, charged, guestReserved,
                       receipts, admitted, releasing, guestTombstones,
                       pendingConfirm, hostConfirmed, stopped, revoked, expired,
                       releaseResponseLost, confirmResponseLost, lastClaimedTask,
                       lastRequestedRelease, lastFreed>>
       /\ LeaseUnchanged

ReconcileReserve ==
  /\ \E i \in hostRows \cap guestReserved:
       /\ i[1] \notin stopped \cup revoked
       /\ (~EnforceCurrentFence \/ i = Current(i[1]))
       /\ receipts' = receipts \cup {i}
       /\ admitted' = admitted \cup {i}
       /\ reserveResponseLost' = reserveResponseLost \ {i}
       /\ UNCHANGED <<queue, deadlines, now, hostRows, charged, guestReserved,
                       releasing, guestTombstones, pendingConfirm, hostConfirmed,
                       stopped, revoked, expired, releaseResponseLost,
                       confirmResponseLost, lastClaimedTask,
                       lastRequestedRelease, lastFreed>>
       /\ LeaseUnchanged

\* Stop is explicit allocation retirement. Automatic lease expiry and
\* disconnect revocation only remove leaseProjections and preserve admission.
Stop ==
  /\ \E t \in Tasks \ stopped:
       /\ ~\E p \in leaseProjections: p[1] = t
       /\ stopped' = stopped \cup {t}
       /\ admitted' = {i \in admitted : i[1] # t}
       /\ releasing' = releasing \cup
             (IF EnforceTerminalCompensation
                 THEN {i \in hostRows : i[1] = t}
                 ELSE {i \in admitted : i[1] = t})
       /\ UNCHANGED <<queue, deadlines, now, hostRows, charged, guestReserved,
                       receipts, guestTombstones,
                       pendingConfirm, hostConfirmed, revoked, expired,
                       reserveResponseLost, releaseResponseLost,
                       confirmResponseLost, lastClaimedTask,
                       lastRequestedRelease, lastFreed>>
       /\ LeaseUnchanged

Revoke ==
  /\ \E t \in Tasks \ revoked:
       /\ ~\E p \in leaseProjections: p[1] = t
       /\ revoked' = revoked \cup {t}
       /\ admitted' = {i \in admitted : i[1] # t}
       /\ releasing' = releasing \cup
             (IF EnforceTerminalCompensation
                 THEN {i \in hostRows : i[1] = t}
                 ELSE {i \in admitted : i[1] = t})
       /\ UNCHANGED <<queue, deadlines, now, hostRows, charged, guestReserved,
                       receipts, guestTombstones,
                       pendingConfirm, hostConfirmed, stopped, expired,
                       reserveResponseLost, releaseResponseLost,
                       confirmResponseLost, lastClaimedTask,
                       lastRequestedRelease, lastFreed>>
       /\ LeaseUnchanged

Tick ==
  /\ now < 4
  /\ now' = now + 1
  /\ expired' = expired \cup {t \in Tasks : deadlines[t] <= now'}
  /\ UNCHANGED <<queue, deadlines, hostRows, charged, guestReserved, receipts,
                  admitted, releasing, guestTombstones, pendingConfirm,
                  hostConfirmed, stopped, revoked, reserveResponseLost,
                  releaseResponseLost, confirmResponseLost, lastClaimedTask,
                  lastRequestedRelease, lastFreed>>
  /\ LeaseUnchanged

BeginRelease ==
  /\ \E i \in admitted:
       /\ i[1] \in stopped \cup revoked
       /\ releasing' = releasing \cup {i}
       /\ admitted' = admitted \ {i}
       /\ UNCHANGED <<queue, deadlines, now, hostRows, charged, guestReserved,
                       receipts, guestTombstones, pendingConfirm, hostConfirmed,
                       stopped, revoked, expired, reserveResponseLost,
                       releaseResponseLost, confirmResponseLost, lastClaimedTask,
                       lastRequestedRelease, lastFreed>>
       /\ LeaseUnchanged

\* hostRows is the durable exact reserve request. This transition is delivery
\* (or redelivery after response loss) of that persisted request to Guest; it
\* is not an invented Guest reservation. The result goes directly to the
\* terminal release obligation and never re-enters admitted.
DeliverTerminalReserveRetry(i) ==
  /\ i \in releasing \cap hostRows
  /\ i[1] \in stopped \cup revoked
  /\ i \notin guestTombstones \cup pendingConfirm \cup hostConfirmed
  /\ (~EnforceCurrentFence \/ i = Current(i[1]))
  /\ guestReserved' = guestReserved \cup {i}
  /\ receipts' = receipts \cup {i}
  /\ reserveResponseLost' = reserveResponseLost \ {i}
  /\ UNCHANGED <<queue, deadlines, now, hostRows, charged, admitted,
                  releasing, guestTombstones, pendingConfirm, hostConfirmed,
                  stopped, revoked, expired, releaseResponseLost,
                  confirmResponseLost, lastClaimedTask,
                  lastRequestedRelease, lastFreed>>
  /\ LeaseUnchanged

ReleaseGuest(requested) ==
  /\ requested \in Identities
  /\ lastRequestedRelease' = requested
  /\ LET exact == IF requested \in releasing THEN requested
                   ELSE IF EnforceExactRelease THEN <<0, 0, 0>> ELSE Current(requested[1]) IN
       /\ IF exact \in guestReserved
             THEN /\ guestReserved' = guestReserved \ {exact}
                  /\ receipts' = receipts \ {exact}
                  /\ charged' = charged \ {exact}
                  /\ guestTombstones' = guestTombstones \cup {exact}
                  /\ lastFreed' = exact
             ELSE /\ UNCHANGED <<guestReserved, receipts, charged, guestTombstones>>
                  /\ lastFreed' = <<0, 0, 0>>
  /\ UNCHANGED <<queue, deadlines, now, hostRows, admitted, releasing,
                  pendingConfirm, hostConfirmed, stopped, revoked, expired,
                  reserveResponseLost, releaseResponseLost, confirmResponseLost,
                  lastClaimedTask>>
  /\ LeaseUnchanged

CurrentRelease(i) == i \in releasing \cap guestReserved /\ ReleaseGuest(i)
StaleGenerationRelease == ReleaseGuest(Stale(1))

LoseReleaseResponse ==
  /\ \E i \in guestTombstones \cap releasing:
       /\ releaseResponseLost' = releaseResponseLost \cup {i}
       /\ UNCHANGED <<queue, deadlines, now, hostRows, charged, guestReserved,
                       receipts, admitted, releasing, guestTombstones,
                       pendingConfirm, hostConfirmed, stopped, revoked, expired,
                       reserveResponseLost, confirmResponseLost, lastClaimedTask,
                       lastRequestedRelease, lastFreed>>
       /\ LeaseUnchanged

CommitGuestRelease(i) ==
  /\ i \in guestTombstones \cap releasing
  /\ releasing' = releasing \ {i}
  /\ pendingConfirm' = pendingConfirm \cup {i}
  /\ releaseResponseLost' = releaseResponseLost \ {i}
  /\ UNCHANGED <<queue, deadlines, now, hostRows, charged, guestReserved,
                  receipts, admitted, guestTombstones, hostConfirmed,
                  stopped, revoked, expired, reserveResponseLost,
                  confirmResponseLost, lastClaimedTask,
                  lastRequestedRelease, lastFreed>>
  /\ LeaseUnchanged

LoseConfirmResponse ==
  /\ \E i \in pendingConfirm \cap guestTombstones:
       /\ guestTombstones' = guestTombstones \ {i}
       /\ confirmResponseLost' = confirmResponseLost \cup {i}
       /\ UNCHANGED <<queue, deadlines, now, hostRows, charged, guestReserved,
                       receipts, admitted, releasing, pendingConfirm,
                       hostConfirmed, stopped, revoked, expired,
                       reserveResponseLost, releaseResponseLost, lastClaimedTask,
                       lastRequestedRelease, lastFreed>>
       /\ LeaseUnchanged

ConfirmGuestRelease(i) ==
  /\ i \in pendingConfirm
  /\ guestTombstones' = guestTombstones \ {i}
  /\ hostConfirmed' = hostConfirmed \cup {i}
  /\ confirmResponseLost' = confirmResponseLost \ {i}
  /\ UNCHANGED <<queue, deadlines, now, hostRows, charged, guestReserved,
                  receipts, admitted, releasing, pendingConfirm, stopped,
                  revoked, expired, reserveResponseLost,
                  releaseResponseLost, lastClaimedTask,
                  lastRequestedRelease, lastFreed>>
  /\ LeaseUnchanged

FinalizeHostRelease(i) ==
  /\ i \in pendingConfirm
  /\ (~EnforceReleaseConfirmation \/ i \in hostConfirmed)
  /\ hostRows' = hostRows \ {i}
  /\ pendingConfirm' = pendingConfirm \ {i}
  /\ hostConfirmed' = hostConfirmed \ {i}
  /\ UNCHANGED <<queue, deadlines, now, charged, guestReserved, receipts,
                  admitted, releasing, guestTombstones, stopped, revoked,
                  expired, reserveResponseLost, releaseResponseLost,
                  confirmResponseLost, lastClaimedTask,
                  lastRequestedRelease, lastFreed>>
  /\ LeaseUnchanged

\* Restart discards transient leases and transport-loss markers while
\* preserving every durable Host row, Guest reservation, receipt, tombstone,
\* and admission phase. It never manufactures a release obligation.
HostRestart ==
  /\ LET recoveredTasks == {i[1] : i \in leaseProjections} IN
       /\ leaseReady' = leaseReady \cup recoveredTasks
  /\ leaseProjections' = {}
  /\ reserveResponseLost' = {} /\ releaseResponseLost' = {} /\ confirmResponseLost' = {}
  /\ UNCHANGED <<queue, deadlines, now, hostRows, charged, guestReserved,
                  receipts, admitted, releasing, guestTombstones,
                  pendingConfirm, hostConfirmed, stopped, revoked, expired,
                  lastClaimedTask, lastRequestedRelease, lastFreed,
                  lastExpiredTask, lastExpiredDeadline>>
GuestRestart ==
  /\ reserveResponseLost' = {} /\ releaseResponseLost' = {} /\ confirmResponseLost' = {}
  /\ UNCHANGED <<queue, deadlines, now, hostRows, charged, guestReserved,
                  receipts, admitted, releasing, guestTombstones,
                  pendingConfirm, hostConfirmed, stopped, revoked, expired,
                  lastClaimedTask, lastRequestedRelease, lastFreed,
                  leaseProjections, leaseReady,
                  lastExpiredTask, lastExpiredDeadline>>

Next == \/ ClaimHead \/ GuestReserve \/ LoseReserveResponse \/ ReconcileReserve
        \/ StartLease \/ RenewLease \/ ExplicitReleaseLease \/ ExpireLease
        \/ (\E i \in Identities: DeliverTerminalReserveRetry(i))
        \/ Stop \/ Revoke \/ Tick \/ BeginRelease \/ (\E i \in Identities: CurrentRelease(i))
        \/ StaleGenerationRelease \/ LoseReleaseResponse
        \/ (\E i \in Identities: CommitGuestRelease(i))
        \/ LoseConfirmResponse \/ (\E i \in Identities: ConfirmGuestRelease(i))
        \/ (\E i \in Identities: FinalizeHostRelease(i))
        \/ HostRestart \/ GuestRestart

TypeOK ==
  /\ queue \in Seq(Tasks) /\ now \in 0..4
  /\ hostRows \subseteq Identities /\ charged \subseteq Identities
  /\ guestReserved \subseteq Identities /\ receipts \subseteq Identities
  /\ admitted \subseteq Identities /\ releasing \subseteq Identities
  /\ guestTombstones \subseteq Identities /\ pendingConfirm \subseteq Identities
  /\ hostConfirmed \subseteq Identities
  /\ stopped \subseteq Tasks /\ revoked \subseteq Tasks /\ expired \subseteq Tasks
  /\ lastClaimedTask \in 0..2 /\ lastRequestedRelease \in Identities \cup {<<0,0,0>>}
  /\ lastFreed \in Identities \cup {<<0,0,0>>}
  /\ leaseProjections \subseteq Identities
  /\ leaseReady \subseteq Tasks
  /\ lastExpiredTask \in 0..2 /\ lastExpiredDeadline \in 0..4

HardBudgetsHold == Fits(charged)
AdmittedHasBothReservations == admitted \subseteq (hostRows \cap guestReserved \cap receipts)
AdmittedIsCurrent == \A i \in admitted: i = Current(i[1])
StoppedRevokedCannotGainAdmission == \A i \in admitted: i[1] \notin stopped \cup revoked
ExactReleaseOnly == lastFreed = <<0,0,0>> \/ lastFreed = lastRequestedRelease
QueueTasks == {queue[index] : index \in 1..Len(queue)}
FIFOClaim == lastClaimedTask = 0 \/ lastClaimedTask \notin QueueTasks
GuestTombstoneHasHostObligation == guestTombstones \subseteq hostRows
HostDeletesOnlyAfterConfirm == (Identities \ hostRows) \cap pendingConfirm = {}
TerminalGuestHasReleaseObligation ==
  \A i \in guestReserved:
    i[1] \in stopped \cup revoked => i \in releasing \cup pendingConfirm
LeaseExpiryDeadlineExact == lastExpiredTask = 0 \/ lastExpiredDeadline <= now

Spec == Init /\ [][Next]_allVars
        /\ WF_vars(ClaimHead) /\ WF_vars(GuestReserve) /\ WF_vars(ReconcileReserve)
        /\ WF_vars(BeginRelease)
        /\ \A i \in Identities:
             WF_vars(DeliverTerminalReserveRetry(i)) /\ WF_vars(CurrentRelease(i))
             /\ WF_vars(CommitGuestRelease(i)) /\ WF_vars(ConfirmGuestRelease(i))
             /\ WF_vars(FinalizeHostRelease(i))
        /\ WF_allVars(ExpireLease)

QueueEventuallyAdvances == <>(Len(queue) = 0 \/ Head(queue) \in expired \cup stopped \cup revoked)
TerminalReservationsEventuallyFreed ==
  []((\E i \in charged: i[1] \in stopped \cup revoked)
       => <>(\A i \in charged: i[1] \notin stopped \cup revoked))

=============================================================================
