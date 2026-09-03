----------------------- MODULE HostVMRestartRecovery -----------------------
EXTENDS Naturals

CONSTANTS EnforceInstalledStateGuard, StartSucceeds, RecoverySucceeds

\* VMLifecycleOwner is represented by the scalar operation/phase pair. The
\* model owns serialization, shutdown exclusion, and admission ordering. Disk
\* identity and filesystem contents remain ordinary Go configuration concerns.
VARIABLES installedState, initializingState, hostRunning, vmState, boot,
          operation, phase, capabilitiesValid, policiesReady, connectorsReady,
          accepting, leaseActive, recoveryUsed, shutdownRequested, actionRequired,
          admissionEpoch, commitEpoch

vars == <<installedState, initializingState, hostRunning, vmState, boot,
          operation, phase, capabilitiesValid, policiesReady, connectorsReady,
          accepting, leaseActive, recoveryUsed, shutdownRequested, actionRequired,
          admissionEpoch, commitEpoch>>

Init ==
  /\ installedState \in BOOLEAN /\ initializingState \in BOOLEAN
  /\ ~(installedState /\ initializingState)
  /\ hostRunning = FALSE /\ vmState = "stopped" /\ boot = 0
  /\ operation = "none" /\ phase = "idle"
  /\ capabilitiesValid = FALSE /\ policiesReady = FALSE
  /\ connectorsReady = FALSE /\ accepting = FALSE /\ leaseActive = FALSE
  /\ recoveryUsed = FALSE /\ shutdownRequested = FALSE /\ actionRequired = FALSE
  /\ admissionEpoch = 0 /\ commitEpoch = 0

StartHost ==
  /\ ~hostRunning /\ ~shutdownRequested
  /\ (~EnforceInstalledStateGuard \/ (installedState /\ ~initializingState))
  /\ hostRunning' = TRUE
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
                  recoveryUsed, shutdownRequested, actionRequired, admissionEpoch, commitEpoch>>

ResumeFailure ==
  /\ operation = "start" /\ phase = "execute" /\ ~StartSucceeds
  /\ vmState' = "failed" /\ operation' = "none" /\ phase' = "idle"
  /\ actionRequired' = TRUE
  /\ UNCHANGED <<installedState, initializingState, hostRunning, boot,
                  capabilitiesValid, policiesReady, connectorsReady, accepting,
                  leaseActive, recoveryUsed, shutdownRequested, admissionEpoch, commitEpoch>>

AcquireLease ==
  /\ accepting /\ ~leaseActive /\ operation = "none" /\ ~shutdownRequested
  /\ leaseActive' = TRUE
  /\ UNCHANGED <<installedState, initializingState, hostRunning, vmState, boot,
                  operation, phase, capabilitiesValid, policiesReady,
                  connectorsReady, accepting, recoveryUsed, shutdownRequested,
                  actionRequired, admissionEpoch, commitEpoch>>

UnexpectedStop ==
  /\ vmState = "running" /\ operation = "none" /\ ~recoveryUsed /\ ~shutdownRequested
  /\ vmState' = "stopped" /\ operation' = "recover" /\ phase' = "execute"
  /\ recoveryUsed' = TRUE /\ capabilitiesValid' = FALSE
  /\ policiesReady' = FALSE /\ connectorsReady' = FALSE
  /\ accepting' = FALSE /\ leaseActive' = FALSE
  /\ admissionEpoch' = admissionEpoch + 1
  /\ UNCHANGED <<installedState, initializingState, hostRunning, boot,
                  shutdownRequested, actionRequired, commitEpoch>>

RecoverySuccess ==
  /\ operation = "recover" /\ phase = "execute" /\ RecoverySucceeds /\ ~leaseActive
  /\ vmState' = "running" /\ boot' = boot + 1 /\ phase' = "commit"
  /\ capabilitiesValid' = TRUE /\ policiesReady' = TRUE /\ connectorsReady' = TRUE
  /\ commitEpoch' = admissionEpoch
  /\ UNCHANGED <<installedState, initializingState, hostRunning, operation,
                  accepting, leaseActive, recoveryUsed, shutdownRequested, actionRequired,
                  admissionEpoch>>

RecoveryCommit ==
  /\ operation = "recover" /\ phase = "commit"
  /\ commitEpoch = admissionEpoch
  /\ accepting' = TRUE /\ operation' = "none" /\ phase' = "idle"
  /\ UNCHANGED <<installedState, initializingState, hostRunning, vmState, boot,
                  capabilitiesValid, policiesReady, connectorsReady, leaseActive,
                  recoveryUsed, shutdownRequested, actionRequired, admissionEpoch, commitEpoch>>

TerminalDuringCommit ==
  /\ operation = "recover" /\ phase = "commit"
  /\ vmState' = "failed" /\ operation' = "none" /\ phase' = "idle"
  /\ accepting' = FALSE /\ capabilitiesValid' = FALSE /\ policiesReady' = FALSE
  /\ connectorsReady' = FALSE /\ leaseActive' = FALSE
  /\ admissionEpoch' = admissionEpoch + 1 /\ actionRequired' = TRUE
  /\ UNCHANGED <<installedState, initializingState, hostRunning, boot,
                  recoveryUsed, shutdownRequested, commitEpoch>>

RecoveryFailure ==
  /\ operation = "recover" /\ phase = "execute" /\ ~RecoverySucceeds
  /\ vmState' = "failed" /\ operation' = "none" /\ phase' = "idle"
  /\ actionRequired' = TRUE
  /\ UNCHANGED <<installedState, initializingState, hostRunning, boot,
                  capabilitiesValid, policiesReady, connectorsReady, accepting,
                  leaseActive, recoveryUsed, shutdownRequested, admissionEpoch, commitEpoch>>

Shutdown ==
  /\ hostRunning /\ operation = "none"
  /\ hostRunning' = FALSE /\ vmState' = "stopped"
  /\ operation' = "shutdown" /\ phase' = "idle" /\ shutdownRequested' = TRUE
  /\ capabilitiesValid' = FALSE /\ policiesReady' = FALSE
  /\ connectorsReady' = FALSE /\ accepting' = FALSE /\ leaseActive' = FALSE
  /\ admissionEpoch' = admissionEpoch + 1
  /\ UNCHANGED <<installedState, initializingState, boot, recoveryUsed, actionRequired, commitEpoch>>

Next == StartHost \/ ResumeSuccess \/ ResumeFailure \/ AcquireLease \/
        UnexpectedStop \/ RecoverySuccess \/ RecoveryCommit \/ TerminalDuringCommit \/ RecoveryFailure \/ Shutdown

TypeOK ==
  /\ installedState \in BOOLEAN /\ initializingState \in BOOLEAN
  /\ hostRunning \in BOOLEAN /\ vmState \in {"stopped", "running", "failed"}
  /\ boot \in Nat /\ operation \in {"none", "start", "recover", "shutdown"}
  /\ phase \in {"idle", "execute", "commit"}
  /\ capabilitiesValid \in BOOLEAN /\ policiesReady \in BOOLEAN
  /\ connectorsReady \in BOOLEAN /\ accepting \in BOOLEAN /\ leaseActive \in BOOLEAN
  /\ recoveryUsed \in BOOLEAN /\ shutdownRequested \in BOOLEAN /\ actionRequired \in BOOLEAN
  /\ admissionEpoch \in Nat /\ commitEpoch \in Nat

NoFreshVMStart == vmState = "running" => installedState /\ ~initializingState
AcceptingRequiresHealthyRuntime == accepting => vmState = "running" /\ capabilitiesValid /\ policiesReady /\ connectorsReady
RecoveryRevokesAdmission == operation = "recover" /\ phase = "execute" => ~accepting /\ ~capabilitiesValid
RecoveryClearsLease == operation = "recover" => ~leaseActive
AtMostOneRecoveryOwner == operation = "recover" => recoveryUsed
ShutdownExcludesRecoveryOwner == shutdownRequested => operation # "recover"
ExhaustedRecoveryFailsClosed == actionRequired => ~accepting
InstalledEventuallyRunning == StartSucceeds => [] (hostRunning /\ installedState /\ ~recoveryUsed /\ ~actionRequired => <> (vmState = "running"))

Spec == Init /\ [][Next]_vars /\ WF_vars(StartHost) /\ WF_vars(ResumeSuccess) /\
        WF_vars(ResumeFailure) /\ WF_vars(RecoverySuccess) /\ WF_vars(RecoveryCommit) /\ WF_vars(RecoveryFailure)
=============================================================================
