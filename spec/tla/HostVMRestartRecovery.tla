----------------------- MODULE HostVMRestartRecovery -----------------------
EXTENDS Naturals

CONSTANTS EnforceInstalledStateGuard, StartSucceeds

\* Production anchors: cmd/agent-vmm-host.resumeInstalledVM and
\* internal/host.Controller.StartVM.

VARIABLES installedState, initializingState, hostRunning, vmRunning,
          resumePending, diagnostic

vars == <<installedState, initializingState, hostRunning, vmRunning,
          resumePending, diagnostic>>

Init ==
  /\ installedState \in BOOLEAN
  /\ initializingState \in BOOLEAN
  /\ ~(installedState /\ initializingState)
  /\ hostRunning = FALSE
  /\ vmRunning = FALSE
  /\ resumePending = FALSE
  /\ diagnostic = FALSE

StartHost ==
  /\ ~hostRunning
  /\ hostRunning' = TRUE
  /\ resumePending' = IF EnforceInstalledStateGuard
                       THEN installedState /\ ~initializingState
                       ELSE TRUE
  /\ diagnostic' = FALSE
  /\ UNCHANGED <<installedState, initializingState, vmRunning>>

ResumeSuccess ==
  /\ hostRunning
  /\ resumePending
  /\ StartSucceeds
  /\ vmRunning' = TRUE
  /\ resumePending' = FALSE
  /\ UNCHANGED <<installedState, initializingState, hostRunning, diagnostic>>

ResumeFailure ==
  /\ hostRunning
  /\ resumePending
  /\ ~StartSucceeds
  /\ vmRunning' = FALSE
  /\ resumePending' = FALSE
  /\ diagnostic' = TRUE
  /\ UNCHANGED <<installedState, initializingState, hostRunning>>

Next == StartHost \/ ResumeSuccess \/ ResumeFailure

TypeOK ==
  /\ installedState \in BOOLEAN
  /\ initializingState \in BOOLEAN
  /\ ~(installedState /\ initializingState)
  /\ hostRunning \in BOOLEAN
  /\ vmRunning \in BOOLEAN
  /\ resumePending \in BOOLEAN
  /\ diagnostic \in BOOLEAN

NoFreshVMStart == vmRunning => installedState /\ ~initializingState
FailedResumeIsDiagnosable ==
  hostRunning /\ installedState /\ ~StartSucceeds /\ ~resumePending => diagnostic
InstalledEventuallyRunning ==
  StartSucceeds => [] (hostRunning /\ installedState => <> vmRunning)

Spec == Init /\ [][Next]_vars /\ WF_vars(StartHost) /\
        WF_vars(ResumeSuccess) /\ WF_vars(ResumeFailure)

=============================================================================
