----------------------- MODULE HostVMRestartRecovery -----------------------
EXTENDS Naturals

\* Shutdown and disk-release actions apply to the macOS VZ Host.
\* Linux Host exit closes control authority and preserves the independent VMM.

CONSTANTS EnforceInstalledStateGuard, StartSucceeds, RecoverySucceeds, MaxRecoveryAttempts

\* recoveryAttempts counts automatic restart attempts across unexpected stops.
\* The Go owner resets it after a recovered VM runs for ten minutes; that timing
\* reset is not modeled, so the model checks the stricter process-lifetime bound.

\* VMLifecycleOwner is represented by the scalar operation/phase pair. The
\* model owns serialization, shutdown exclusion, and admission ordering. Disk
\* identity and filesystem contents remain ordinary Go configuration concerns.
VARIABLES installedState, initializingState, hostRunning, vmState, boot,
          operation, phase, capabilitiesValid, policiesReady, connectorsReady,
          accepting, sessionActive, recoveryAttempts, shutdownRequested, actionRequired,
          admissionEpoch, commitEpoch, guestQuiesced, vzTerminal, diskReleased

vars == <<installedState, initializingState, hostRunning, vmState, boot,
          operation, phase, capabilitiesValid, policiesReady, connectorsReady,
          accepting, sessionActive, recoveryAttempts, shutdownRequested, actionRequired,
          admissionEpoch, commitEpoch, guestQuiesced, vzTerminal, diskReleased>>

Init ==
  /\ installedState \in BOOLEAN /\ initializingState \in BOOLEAN
  /\ ~(installedState /\ initializingState)
  /\ hostRunning = FALSE /\ vmState = "stopped" /\ boot = 0
  /\ operation = "none" /\ phase = "idle"
  /\ capabilitiesValid = FALSE /\ policiesReady = FALSE
  /\ connectorsReady = FALSE /\ accepting = FALSE /\ sessionActive = FALSE
  /\ recoveryAttempts = 0 /\ shutdownRequested = FALSE /\ actionRequired = FALSE
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
                  sessionActive, recoveryAttempts, shutdownRequested, actionRequired,
                  admissionEpoch, commitEpoch>>

ResumeSuccess ==
  /\ operation = "start" /\ phase = "execute" /\ StartSucceeds
  /\ vmState' = "running" /\ boot' = boot + 1
  /\ operation' = "none" /\ phase' = "idle"
  /\ capabilitiesValid' = TRUE /\ policiesReady' = TRUE
  /\ connectorsReady' = TRUE /\ accepting' = TRUE
  /\ UNCHANGED <<installedState, initializingState, hostRunning, sessionActive,
                  recoveryAttempts, shutdownRequested, actionRequired, admissionEpoch, commitEpoch,
                  guestQuiesced, vzTerminal, diskReleased>>

ResumeFailure ==
  /\ operation = "start" /\ phase = "execute" /\ ~StartSucceeds
  /\ vmState' = "failed" /\ operation' = "none" /\ phase' = "idle"
  /\ actionRequired' = TRUE
  /\ UNCHANGED <<installedState, initializingState, hostRunning, boot,
                  capabilitiesValid, policiesReady, connectorsReady, accepting,
                  sessionActive, recoveryAttempts, shutdownRequested, admissionEpoch, commitEpoch,
                  guestQuiesced, vzTerminal, diskReleased>>

OpenSession ==
  /\ accepting /\ ~sessionActive /\ operation = "none" /\ ~shutdownRequested
  /\ sessionActive' = TRUE
  /\ UNCHANGED <<installedState, initializingState, hostRunning, vmState, boot,
                  operation, phase, capabilitiesValid, policiesReady,
                  connectorsReady, accepting, recoveryAttempts, shutdownRequested,
                  actionRequired, admissionEpoch, commitEpoch, guestQuiesced,
                  vzTerminal, diskReleased>>

UnexpectedStop ==
  /\ vmState = "running" /\ operation = "none" /\ recoveryAttempts < MaxRecoveryAttempts /\ ~shutdownRequested
  /\ vmState' = "stopped" /\ operation' = "recover" /\ phase' = "execute"
  /\ recoveryAttempts' = recoveryAttempts + 1 /\ capabilitiesValid' = FALSE
  /\ policiesReady' = FALSE /\ connectorsReady' = FALSE
  /\ accepting' = FALSE /\ sessionActive' = FALSE
  /\ admissionEpoch' = admissionEpoch + 1
  /\ vzTerminal' = TRUE /\ diskReleased' = TRUE
  /\ UNCHANGED <<installedState, initializingState, hostRunning, boot,
                  shutdownRequested, actionRequired, commitEpoch, guestQuiesced>>

RecoverySuccess ==
  /\ operation = "recover" /\ phase = "execute" /\ RecoverySucceeds /\ ~sessionActive
  /\ vmState' = "running" /\ boot' = boot + 1 /\ phase' = "commit"
  /\ capabilitiesValid' = TRUE /\ policiesReady' = TRUE /\ connectorsReady' = TRUE
  /\ commitEpoch' = admissionEpoch
  /\ guestQuiesced' = FALSE /\ vzTerminal' = FALSE /\ diskReleased' = FALSE
  /\ UNCHANGED <<installedState, initializingState, hostRunning, operation,
                  accepting, sessionActive, recoveryAttempts, shutdownRequested, actionRequired,
                  admissionEpoch>>

RecoveryCommit ==
  /\ operation = "recover" /\ phase = "commit"
  /\ commitEpoch = admissionEpoch
  /\ accepting' = TRUE /\ operation' = "none" /\ phase' = "idle"
  /\ UNCHANGED <<installedState, initializingState, hostRunning, vmState, boot,
                  capabilitiesValid, policiesReady, connectorsReady, sessionActive,
                  recoveryAttempts, shutdownRequested, actionRequired, admissionEpoch, commitEpoch,
                  guestQuiesced, vzTerminal, diskReleased>>

TerminalDuringCommit ==
  /\ operation = "recover" /\ phase = "commit"
  /\ vmState' = "failed" /\ operation' = "none" /\ phase' = "idle"
  /\ accepting' = FALSE /\ capabilitiesValid' = FALSE /\ policiesReady' = FALSE
  /\ connectorsReady' = FALSE /\ sessionActive' = FALSE
  /\ admissionEpoch' = admissionEpoch + 1 /\ actionRequired' = TRUE
  /\ UNCHANGED <<installedState, initializingState, hostRunning, boot,
                  recoveryAttempts, shutdownRequested, commitEpoch, guestQuiesced,
                  vzTerminal, diskReleased>>

\* A failed attempt keeps the same recovery owner and retries after a backoff.
RecoveryRetry ==
  /\ operation = "recover" /\ phase = "execute" /\ ~RecoverySucceeds
  /\ recoveryAttempts < MaxRecoveryAttempts
  /\ recoveryAttempts' = recoveryAttempts + 1
  /\ UNCHANGED <<installedState, initializingState, hostRunning, vmState, boot,
                  operation, phase, capabilitiesValid, policiesReady, connectorsReady,
                  accepting, sessionActive, shutdownRequested, actionRequired,
                  admissionEpoch, commitEpoch, guestQuiesced, vzTerminal, diskReleased>>

RecoveryFailure ==
  /\ operation = "recover" /\ phase = "execute" /\ ~RecoverySucceeds
  /\ recoveryAttempts = MaxRecoveryAttempts
  /\ vmState' = "failed" /\ operation' = "none" /\ phase' = "idle"
  /\ actionRequired' = TRUE
  /\ UNCHANGED <<installedState, initializingState, hostRunning, boot,
                  capabilitiesValid, policiesReady, connectorsReady, accepting,
                  sessionActive, recoveryAttempts, shutdownRequested, admissionEpoch, commitEpoch,
                  guestQuiesced, vzTerminal, diskReleased>>

BeginShutdown ==
  /\ hostRunning /\ operation = "none"
  /\ operation' = "shutdown" /\ phase' = "quiesce" /\ shutdownRequested' = TRUE
  /\ capabilitiesValid' = FALSE /\ policiesReady' = FALSE
  /\ connectorsReady' = FALSE /\ accepting' = FALSE /\ sessionActive' = FALSE
  /\ admissionEpoch' = admissionEpoch + 1
  /\ UNCHANGED <<installedState, initializingState, hostRunning, vmState, boot,
                  recoveryAttempts, actionRequired, commitEpoch, guestQuiesced,
                  vzTerminal, diskReleased>>

GuestQuiesceSuccess ==
  /\ operation = "shutdown" /\ phase = "quiesce"
  /\ guestQuiesced' = TRUE /\ phase' = "terminal"
  /\ UNCHANGED <<installedState, initializingState, hostRunning, vmState, boot,
                  operation, capabilitiesValid, policiesReady, connectorsReady,
                  accepting, sessionActive, recoveryAttempts, shutdownRequested,
                  actionRequired, admissionEpoch, commitEpoch, vzTerminal,
                  diskReleased>>

GuestQuiesceFailure ==
  /\ operation = "shutdown" /\ phase = "quiesce"
  /\ actionRequired' = TRUE /\ phase' = "terminal"
  /\ UNCHANGED <<installedState, initializingState, hostRunning, vmState, boot,
                  operation, capabilitiesValid, policiesReady, connectorsReady,
                  accepting, sessionActive, recoveryAttempts, shutdownRequested,
                  admissionEpoch, commitEpoch, guestQuiesced, vzTerminal,
                  diskReleased>>

VZTerminal ==
  /\ operation = "shutdown" /\ phase = "terminal"
  /\ vmState' = "stopped" /\ vzTerminal' = TRUE /\ phase' = "release"
  /\ UNCHANGED <<installedState, initializingState, hostRunning, boot, operation,
                  capabilitiesValid, policiesReady, connectorsReady, accepting,
                  sessionActive, recoveryAttempts, shutdownRequested, actionRequired,
                  admissionEpoch, commitEpoch, guestQuiesced, diskReleased>>

ReleaseDisk ==
  /\ operation = "shutdown" /\ phase = "release" /\ vzTerminal
  /\ diskReleased' = TRUE /\ phase' = "exit"
  /\ UNCHANGED <<installedState, initializingState, hostRunning, vmState, boot,
                  operation, capabilitiesValid, policiesReady, connectorsReady,
                  accepting, sessionActive, recoveryAttempts, shutdownRequested,
                  actionRequired, admissionEpoch, commitEpoch, guestQuiesced,
                  vzTerminal>>

ExitHost ==
  /\ operation = "shutdown" /\ phase = "exit" /\ diskReleased
  /\ hostRunning' = FALSE /\ operation' = "none" /\ phase' = "idle"
  /\ UNCHANGED <<installedState, initializingState, vmState, boot,
                  capabilitiesValid, policiesReady, connectorsReady, accepting,
                  sessionActive, recoveryAttempts, shutdownRequested, actionRequired,
                  admissionEpoch, commitEpoch, guestQuiesced, vzTerminal,
                  diskReleased>>

Terminated == ~hostRunning /\ diskReleased /\ (shutdownRequested \/ (EnforceInstalledStateGuard /\ (~installedState \/ initializingState))) /\ UNCHANGED vars

Next == Terminated \/ StartHost \/ ResumeSuccess \/ ResumeFailure \/ OpenSession \/
        UnexpectedStop \/ RecoverySuccess \/ RecoveryCommit \/ TerminalDuringCommit \/ RecoveryRetry \/ RecoveryFailure \/
        BeginShutdown \/ GuestQuiesceSuccess \/ GuestQuiesceFailure \/
        VZTerminal \/ ReleaseDisk \/ ExitHost

TypeOK ==
  /\ installedState \in BOOLEAN /\ initializingState \in BOOLEAN
  /\ hostRunning \in BOOLEAN /\ vmState \in {"stopped", "running", "failed"}
  /\ boot \in Nat /\ operation \in {"none", "start", "recover", "shutdown"}
  /\ phase \in {"idle", "execute", "commit", "quiesce", "terminal", "release", "exit"}
  /\ capabilitiesValid \in BOOLEAN /\ policiesReady \in BOOLEAN
  /\ connectorsReady \in BOOLEAN /\ accepting \in BOOLEAN /\ sessionActive \in BOOLEAN
  /\ recoveryAttempts \in 0..MaxRecoveryAttempts /\ shutdownRequested \in BOOLEAN /\ actionRequired \in BOOLEAN
  /\ admissionEpoch \in Nat /\ commitEpoch \in Nat
  /\ guestQuiesced \in BOOLEAN /\ vzTerminal \in BOOLEAN /\ diskReleased \in BOOLEAN

NoFreshVMStart == vmState = "running" => installedState /\ ~initializingState
AcceptingRequiresHealthyRuntime == accepting => vmState = "running" /\ capabilitiesValid /\ policiesReady /\ connectorsReady
RecoveryRevokesAdmission == operation = "recover" /\ phase = "execute" => ~accepting /\ ~capabilitiesValid
RecoveryClearsSession == operation = "recover" => ~sessionActive
AtMostOneRecoveryOwner == operation = "recover" => recoveryAttempts > 0
ShutdownExcludesRecoveryOwner == shutdownRequested => operation # "recover"
ExhaustedRecoveryFailsClosed == actionRequired => ~accepting
DiskReleaseRequiresVZTerminal == diskReleased => vzTerminal
HostExitRequiresDiskRelease == ~hostRunning => diskReleased /\ vzTerminal
CleanShutdownRequiresGuestQuiesce == operation = "shutdown" /\ vzTerminal /\ ~actionRequired => guestQuiesced
InstalledEventuallyRunning == StartSucceeds => [] (hostRunning /\ installedState /\ recoveryAttempts = 0 /\ ~actionRequired /\ ~shutdownRequested => <> (vmState = "running"))

Spec == Init /\ [][Next]_vars /\ WF_vars(StartHost) /\ WF_vars(ResumeSuccess) /\
        WF_vars(ResumeFailure) /\ WF_vars(RecoverySuccess) /\ WF_vars(RecoveryCommit) /\ WF_vars(RecoveryRetry) /\ WF_vars(RecoveryFailure)
=============================================================================
