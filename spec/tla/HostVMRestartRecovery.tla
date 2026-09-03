----------------------- MODULE HostVMRestartRecovery -----------------------
EXTENDS Naturals

CONSTANTS EnforceInstalledStateGuard, StartSucceeds, RecoverySucceeds

\* VMLifecycleOwner is represented by the scalar operation/phase pair. The
\* model owns serialization, shutdown exclusion, and admission ordering. Disk
\* identity and filesystem contents remain ordinary Go configuration concerns.
VARIABLES installedState, initializingState, hostRunning, vmState, boot,
          operation, phase, capabilitiesValid, policiesReady, connectorsReady,
          accepting, leaseActive, recoveryUsed, shutdownRequested, actionRequired,
          admissionEpoch, commitEpoch, guestQuiesced, vzTerminal, diskReleased

vars == <<installedState, initializingState, hostRunning, vmState, boot,
          operation, phase, capabilitiesValid, policiesReady, connectorsReady,
          accepting, leaseActive, recoveryUsed, shutdownRequested, actionRequired,
          admissionEpoch, commitEpoch, guestQuiesced, vzTerminal, diskReleased>>

Init ==
  /\ installedState \in BOOLEAN /\ initializingState \in BOOLEAN
  /\ ~(installedState /\ initializingState)
  /\ hostRunning = FALSE /\ vmState = "stopped" /\ boot = 0
  /\ operation = "none" /\ phase = "idle"
  /\ capabilitiesValid = FALSE /\ policiesReady = FALSE
  /\ connectorsReady = FALSE /\ accepting = FALSE /\ leaseActive = FALSE
  /\ recoveryUsed = FALSE /\ shutdownRequested = FALSE /\ actionRequired = FALSE
  /\ admissionEpoch = 0 /\ commitEpoch = 0
  /\ guestQuiesced = TRUE /\ vzTerminal = TRUE /\ diskReleased = TRUE

StartHost ==
  /\ ~hostRunning /\ ~shutdownRequested /\ diskReleased
  /\ (~EnforceInstalledStateGuard \/ (installedState /\ ~initializingState))
  /\ hostRunning' = TRUE
  /\ guestQuiesced' = FALSE /\ vzTerminal' = FALSE /\ diskReleased' = FALSE
  /\ operation' = "start"
  /\ phase' = "execute"
  /\ UNCHANGED <<installedState, initializingState, vmState, boot,
                  capabilitiesValid, policiesReady, connectorsReady, accepting,
                  leaseActive, recoveryUsed, shutdownRequested, actionRequired,
                  admissionEpoch, commitEpoch>>

ResumeSuccess ==
  /\ operation = "start" /\ phase = "execute" /\ StartSucceeds
  /\ vmState' = "running" /\ boot' = boot + 1
  /\ operation' = "none" /\ phase' = "idle"
  /\ capabilitiesValid' = TRUE /\ policiesReady' = TRUE
  /\ connectorsReady' = TRUE /\ accepting' = TRUE
  /\ UNCHANGED <<installedState, initializingState, hostRunning, leaseActive,
                  recoveryUsed, shutdownRequested, actionRequired, admissionEpoch, commitEpoch,
                  guestQuiesced, vzTerminal, diskReleased>>

ResumeFailure ==
  /\ operation = "start" /\ phase = "execute" /\ ~StartSucceeds
  /\ vmState' = "failed" /\ operation' = "none" /\ phase' = "idle"
  /\ actionRequired' = TRUE
  /\ UNCHANGED <<installedState, initializingState, hostRunning, boot,
                  capabilitiesValid, policiesReady, connectorsReady, accepting,
                  leaseActive, recoveryUsed, shutdownRequested, admissionEpoch, commitEpoch,
                  guestQuiesced, vzTerminal, diskReleased>>

AcquireLease ==
  /\ accepting /\ ~leaseActive /\ operation = "none" /\ ~shutdownRequested
  /\ leaseActive' = TRUE
  /\ UNCHANGED <<installedState, initializingState, hostRunning, vmState, boot,
                  operation, phase, capabilitiesValid, policiesReady,
                  connectorsReady, accepting, recoveryUsed, shutdownRequested,
                  actionRequired, admissionEpoch, commitEpoch, guestQuiesced,
                  vzTerminal, diskReleased>>

UnexpectedStop ==
  /\ vmState = "running" /\ operation = "none" /\ ~recoveryUsed /\ ~shutdownRequested
  /\ vmState' = "stopped" /\ operation' = "recover" /\ phase' = "execute"
  /\ recoveryUsed' = TRUE /\ capabilitiesValid' = FALSE
  /\ policiesReady' = FALSE /\ connectorsReady' = FALSE
  /\ accepting' = FALSE /\ leaseActive' = FALSE
  /\ admissionEpoch' = admissionEpoch + 1
  /\ vzTerminal' = TRUE /\ diskReleased' = TRUE
  /\ UNCHANGED <<installedState, initializingState, hostRunning, boot,
                  shutdownRequested, actionRequired, commitEpoch, guestQuiesced>>

RecoverySuccess ==
  /\ operation = "recover" /\ phase = "execute" /\ RecoverySucceeds /\ ~leaseActive
  /\ vmState' = "running" /\ boot' = boot + 1 /\ phase' = "commit"
  /\ capabilitiesValid' = TRUE /\ policiesReady' = TRUE /\ connectorsReady' = TRUE
  /\ commitEpoch' = admissionEpoch
  /\ guestQuiesced' = FALSE /\ vzTerminal' = FALSE /\ diskReleased' = FALSE
  /\ UNCHANGED <<installedState, initializingState, hostRunning, operation,
                  accepting, leaseActive, recoveryUsed, shutdownRequested, actionRequired,
                  admissionEpoch>>

RecoveryCommit ==
  /\ operation = "recover" /\ phase = "commit"
  /\ commitEpoch = admissionEpoch
  /\ accepting' = TRUE /\ operation' = "none" /\ phase' = "idle"
  /\ UNCHANGED <<installedState, initializingState, hostRunning, vmState, boot,
                  capabilitiesValid, policiesReady, connectorsReady, leaseActive,
                  recoveryUsed, shutdownRequested, actionRequired, admissionEpoch, commitEpoch,
                  guestQuiesced, vzTerminal, diskReleased>>

TerminalDuringCommit ==
  /\ operation = "recover" /\ phase = "commit"
  /\ vmState' = "failed" /\ operation' = "none" /\ phase' = "idle"
  /\ accepting' = FALSE /\ capabilitiesValid' = FALSE /\ policiesReady' = FALSE
  /\ connectorsReady' = FALSE /\ leaseActive' = FALSE
  /\ admissionEpoch' = admissionEpoch + 1 /\ actionRequired' = TRUE
  /\ UNCHANGED <<installedState, initializingState, hostRunning, boot,
                  recoveryUsed, shutdownRequested, commitEpoch, guestQuiesced,
                  vzTerminal, diskReleased>>

RecoveryFailure ==
  /\ operation = "recover" /\ phase = "execute" /\ ~RecoverySucceeds
  /\ vmState' = "failed" /\ operation' = "none" /\ phase' = "idle"
  /\ actionRequired' = TRUE
  /\ UNCHANGED <<installedState, initializingState, hostRunning, boot,
                  capabilitiesValid, policiesReady, connectorsReady, accepting,
                  leaseActive, recoveryUsed, shutdownRequested, admissionEpoch, commitEpoch,
                  guestQuiesced, vzTerminal, diskReleased>>

BeginShutdown ==
  /\ hostRunning /\ operation = "none"
  /\ operation' = "shutdown" /\ phase' = "quiesce" /\ shutdownRequested' = TRUE
  /\ capabilitiesValid' = FALSE /\ policiesReady' = FALSE
  /\ connectorsReady' = FALSE /\ accepting' = FALSE /\ leaseActive' = FALSE
  /\ admissionEpoch' = admissionEpoch + 1
  /\ UNCHANGED <<installedState, initializingState, hostRunning, vmState, boot,
                  recoveryUsed, actionRequired, commitEpoch, guestQuiesced,
                  vzTerminal, diskReleased>>

GuestQuiesceSuccess ==
  /\ operation = "shutdown" /\ phase = "quiesce"
  /\ guestQuiesced' = TRUE /\ phase' = "terminal"
  /\ UNCHANGED <<installedState, initializingState, hostRunning, vmState, boot,
                  operation, capabilitiesValid, policiesReady, connectorsReady,
                  accepting, leaseActive, recoveryUsed, shutdownRequested,
                  actionRequired, admissionEpoch, commitEpoch, vzTerminal,
                  diskReleased>>

GuestQuiesceFailure ==
  /\ operation = "shutdown" /\ phase = "quiesce"
  /\ actionRequired' = TRUE /\ phase' = "terminal"
  /\ UNCHANGED <<installedState, initializingState, hostRunning, vmState, boot,
                  operation, capabilitiesValid, policiesReady, connectorsReady,
                  accepting, leaseActive, recoveryUsed, shutdownRequested,
                  admissionEpoch, commitEpoch, guestQuiesced, vzTerminal,
                  diskReleased>>

VZTerminal ==
  /\ operation = "shutdown" /\ phase = "terminal"
  /\ vmState' = "stopped" /\ vzTerminal' = TRUE /\ phase' = "release"
  /\ UNCHANGED <<installedState, initializingState, hostRunning, boot, operation,
                  capabilitiesValid, policiesReady, connectorsReady, accepting,
                  leaseActive, recoveryUsed, shutdownRequested, actionRequired,
                  admissionEpoch, commitEpoch, guestQuiesced, diskReleased>>

ReleaseDisk ==
  /\ operation = "shutdown" /\ phase = "release" /\ vzTerminal
  /\ diskReleased' = TRUE /\ phase' = "exit"
  /\ UNCHANGED <<installedState, initializingState, hostRunning, vmState, boot,
                  operation, capabilitiesValid, policiesReady, connectorsReady,
                  accepting, leaseActive, recoveryUsed, shutdownRequested,
                  actionRequired, admissionEpoch, commitEpoch, guestQuiesced,
                  vzTerminal>>

ExitHost ==
  /\ operation = "shutdown" /\ phase = "exit" /\ diskReleased
  /\ hostRunning' = FALSE /\ operation' = "none" /\ phase' = "idle"
  /\ UNCHANGED <<installedState, initializingState, vmState, boot,
                  capabilitiesValid, policiesReady, connectorsReady, accepting,
                  leaseActive, recoveryUsed, shutdownRequested, actionRequired,
                  admissionEpoch, commitEpoch, guestQuiesced, vzTerminal,
                  diskReleased>>

Next == StartHost \/ ResumeSuccess \/ ResumeFailure \/ AcquireLease \/
        UnexpectedStop \/ RecoverySuccess \/ RecoveryCommit \/ TerminalDuringCommit \/ RecoveryFailure \/
        BeginShutdown \/ GuestQuiesceSuccess \/ GuestQuiesceFailure \/
        VZTerminal \/ ReleaseDisk \/ ExitHost

TypeOK ==
  /\ installedState \in BOOLEAN /\ initializingState \in BOOLEAN
  /\ hostRunning \in BOOLEAN /\ vmState \in {"stopped", "running", "failed"}
  /\ boot \in Nat /\ operation \in {"none", "start", "recover", "shutdown"}
  /\ phase \in {"idle", "execute", "commit", "quiesce", "terminal", "release", "exit"}
  /\ capabilitiesValid \in BOOLEAN /\ policiesReady \in BOOLEAN
  /\ connectorsReady \in BOOLEAN /\ accepting \in BOOLEAN /\ leaseActive \in BOOLEAN
  /\ recoveryUsed \in BOOLEAN /\ shutdownRequested \in BOOLEAN /\ actionRequired \in BOOLEAN
  /\ admissionEpoch \in Nat /\ commitEpoch \in Nat
  /\ guestQuiesced \in BOOLEAN /\ vzTerminal \in BOOLEAN /\ diskReleased \in BOOLEAN

NoFreshVMStart == vmState = "running" => installedState /\ ~initializingState
AcceptingRequiresHealthyRuntime == accepting => vmState = "running" /\ capabilitiesValid /\ policiesReady /\ connectorsReady
RecoveryRevokesAdmission == operation = "recover" /\ phase = "execute" => ~accepting /\ ~capabilitiesValid
RecoveryClearsLease == operation = "recover" => ~leaseActive
AtMostOneRecoveryOwner == operation = "recover" => recoveryUsed
ShutdownExcludesRecoveryOwner == shutdownRequested => operation # "recover"
ExhaustedRecoveryFailsClosed == actionRequired => ~accepting
DiskReleaseRequiresVZTerminal == diskReleased => vzTerminal
HostExitRequiresDiskRelease == ~hostRunning => diskReleased /\ vzTerminal
CleanShutdownRequiresGuestQuiesce == operation = "shutdown" /\ vzTerminal /\ ~actionRequired => guestQuiesced
InstalledEventuallyRunning == StartSucceeds => [] (hostRunning /\ installedState /\ ~recoveryUsed /\ ~actionRequired /\ ~shutdownRequested => <> (vmState = "running"))

Spec == Init /\ [][Next]_vars /\ WF_vars(StartHost) /\ WF_vars(ResumeSuccess) /\
        WF_vars(ResumeFailure) /\ WF_vars(RecoverySuccess) /\ WF_vars(RecoveryCommit) /\ WF_vars(RecoveryFailure)
=============================================================================
