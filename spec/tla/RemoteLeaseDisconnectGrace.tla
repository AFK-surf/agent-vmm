---------------------- MODULE RemoteLeaseDisconnectGrace --------------------
EXTENDS Naturals

CONSTANTS MaxFence, EnforceDisconnectFence

VARIABLES connected, disconnectFence, pendingTimers, leaseActive,
          staleTimerRevoked, allocationReady, reservationAdmitted

vars == <<connected, disconnectFence, pendingTimers, leaseActive,
          staleTimerRevoked, allocationReady, reservationAdmitted>>

Init ==
  /\ connected = TRUE
  /\ disconnectFence = 0
  /\ pendingTimers = {}
  /\ leaseActive = TRUE
  /\ staleTimerRevoked = FALSE
  /\ allocationReady = TRUE
  /\ reservationAdmitted = TRUE

Disconnect ==
  /\ connected
  /\ disconnectFence < MaxFence
  /\ connected' = FALSE
  /\ disconnectFence' = disconnectFence + 1
  /\ pendingTimers' = pendingTimers \cup {disconnectFence + 1}
  /\ UNCHANGED <<leaseActive, staleTimerRevoked, allocationReady,
                  reservationAdmitted>>

Reconnect ==
  /\ ~connected
  /\ connected' = TRUE
  /\ UNCHANGED <<disconnectFence, pendingTimers, leaseActive,
                  staleTimerRevoked, allocationReady, reservationAdmitted>>

GraceExpires(fence) ==
  /\ fence \in pendingTimers
  /\ pendingTimers' = pendingTimers \ {fence}
  /\ IF leaseActive /\ ~connected /\
        (~EnforceDisconnectFence \/ fence = disconnectFence)
       THEN /\ leaseActive' = FALSE
            /\ staleTimerRevoked' =
                 (staleTimerRevoked \/ fence # disconnectFence)
       ELSE UNCHANGED <<leaseActive, staleTimerRevoked>>
  /\ UNCHANGED <<connected, disconnectFence, allocationReady,
                  reservationAdmitted>>

ExpireCurrentGrace == GraceExpires(disconnectFence)

Next == Disconnect \/ Reconnect \/
  (\E fence \in 1..MaxFence: GraceExpires(fence))

TypeOK ==
  /\ connected \in BOOLEAN
  /\ disconnectFence \in 0..MaxFence
  /\ pendingTimers \subseteq 1..MaxFence
  /\ leaseActive \in BOOLEAN
  /\ staleTimerRevoked \in BOOLEAN
  /\ allocationReady \in BOOLEAN
  /\ reservationAdmitted \in BOOLEAN

NoStaleTimerRevocation == ~staleTimerRevoked
DisconnectPreservesAllocation == allocationReady /\ reservationAdmitted

CurrentDisconnectEventuallySettles ==
  (~connected /\ leaseActive) ~> (connected \/ ~leaseActive)

Spec == Init /\ [][Next]_vars /\ WF_vars(ExpireCurrentGrace)

=============================================================================
