------------------------- MODULE WorkloadRunLifecycle ------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS EnforceAtomicAdmission, EnforceExecutionInstanceFence,
          EnforceQuiesceFence, EnforceStopInstanceFence

Tasks == {1, 2}
Instances == {1, 2}
Executions == {1, 2}
QuiesceCommands == {1, 2}
States == {"stopped", "starting", "running", "stopping", "unknown"}
Capacity == 1

VARIABLES state, instance, execution, executionInstance, admissionOpen,
          quiesce, pending, checked, lastStoppedTask, lastStoppedInstance,
          lastStoppedExpected

vars == <<state, instance, execution, executionInstance, admissionOpen,
          quiesce, pending, checked, lastStoppedTask, lastStoppedInstance,
          lastStoppedExpected>>

Active == {t \in Tasks : state[t] # "stopped"}

Init ==
  /\ state = [t \in Tasks |-> "stopped"]
  /\ instance = [t \in Tasks |-> 0]
  /\ execution = [t \in Tasks |-> 0]
  /\ executionInstance = [t \in Tasks |-> 0]
  /\ admissionOpen = [t \in Tasks |-> FALSE]
  /\ quiesce = [t \in Tasks |-> 0]
  /\ pending = [t \in Tasks |-> "none"]
  /\ checked = {}
  /\ lastStoppedTask = 0
  /\ lastStoppedInstance = 0
  /\ lastStoppedExpected = 0

CheckStart(t) ==
  /\ ~EnforceAtomicAdmission
  /\ t \notin Active
  /\ Cardinality(Active) < Capacity
  /\ checked' = checked \cup {t}
  /\ UNCHANGED <<state, instance, execution, executionInstance,
                  admissionOpen, quiesce, pending, lastStoppedTask,
                  lastStoppedInstance, lastStoppedExpected>>

BeginStart(t) ==
  /\ state[t] = "stopped"
  /\ IF EnforceAtomicAdmission
        THEN Cardinality(Active) < Capacity
        ELSE t \in checked
  /\ state' = [state EXCEPT ![t] = "starting"]
  /\ admissionOpen' = [admissionOpen EXCEPT ![t] = FALSE]
  /\ pending' = [pending EXCEPT ![t] = "start"]
  /\ checked' = checked \ {t}
  /\ UNCHANGED <<instance, execution, executionInstance, quiesce,
                  lastStoppedTask, lastStoppedInstance, lastStoppedExpected>>

CommitRunning(t, i) ==
  /\ state[t] \in {"starting", "unknown"}
  /\ pending[t] = "start"
  /\ i \in Instances
  /\ i # instance[t]
  /\ state' = [state EXCEPT ![t] = "running"]
  /\ instance' = [instance EXCEPT ![t] = i]
  /\ admissionOpen' = [admissionOpen EXCEPT ![t] = TRUE]
  /\ pending' = [pending EXCEPT ![t] = "none"]
  /\ UNCHANGED <<execution, executionInstance, quiesce, checked,
                  lastStoppedTask, lastStoppedInstance, lastStoppedExpected>>

LoseStartResponse(t) ==
  /\ state[t] = "starting"
  /\ state' = [state EXCEPT ![t] = "unknown"]
  /\ UNCHANGED <<instance, execution, executionInstance, admissionOpen,
                  quiesce, pending, checked, lastStoppedTask, lastStoppedInstance,
                  lastStoppedExpected>>

Acquire(t, e, observedInstance) ==
  /\ state[t] = "running"
  /\ execution[t] = 0
  /\ e \in Executions
  /\ observedInstance \in Instances
  /\ (~EnforceQuiesceFence \/ admissionOpen[t])
  /\ (~EnforceExecutionInstanceFence \/ observedInstance = instance[t])
  /\ execution' = [execution EXCEPT ![t] = e]
  /\ executionInstance' = [executionInstance EXCEPT ![t] = observedInstance]
  /\ UNCHANGED <<state, instance, admissionOpen, quiesce, pending, checked,
                  lastStoppedTask, lastStoppedInstance, lastStoppedExpected>>

Release(t, e, observedInstance) ==
  /\ execution[t] = e
  /\ executionInstance[t] = observedInstance
  /\ observedInstance = instance[t]
  /\ execution' = [execution EXCEPT ![t] = 0]
  /\ executionInstance' = [executionInstance EXCEPT ![t] = 0]
  /\ UNCHANGED <<state, instance, admissionOpen, quiesce, pending, checked,
                  lastStoppedTask, lastStoppedInstance, lastStoppedExpected>>

BeginQuiesce(t, q) ==
  /\ state[t] = "running"
  /\ execution[t] = 0
  /\ q \in QuiesceCommands
  /\ quiesce[t] = 0
  /\ admissionOpen' = [admissionOpen EXCEPT ![t] = FALSE]
  /\ quiesce' = [quiesce EXCEPT ![t] = q]
  /\ UNCHANGED <<state, instance, execution, executionInstance, pending, checked,
                  lastStoppedTask, lastStoppedInstance, lastStoppedExpected>>

CancelQuiesce(t, q) ==
  /\ state[t] = "running"
  /\ quiesce[t] = q
  /\ execution[t] = 0
  /\ admissionOpen' = [admissionOpen EXCEPT ![t] = TRUE]
  /\ quiesce' = [quiesce EXCEPT ![t] = 0]
  /\ UNCHANGED <<state, instance, execution, executionInstance, pending, checked,
                  lastStoppedTask, lastStoppedInstance, lastStoppedExpected>>

BeginStop(t, q) ==
  /\ state[t] = "running"
  /\ q \in QuiesceCommands
  /\ IF EnforceQuiesceFence
        THEN /\ quiesce[t] = q
             /\ ~admissionOpen[t]
             /\ execution[t] = 0
        ELSE TRUE
  /\ state' = [state EXCEPT ![t] = "stopping"]
  /\ admissionOpen' = [admissionOpen EXCEPT ![t] = FALSE]
  /\ pending' = [pending EXCEPT ![t] = "stop"]
  /\ UNCHANGED <<instance, execution, executionInstance, quiesce, checked,
                  lastStoppedTask, lastStoppedInstance, lastStoppedExpected>>

LoseStopResponse(t) ==
  /\ state[t] = "stopping"
  /\ state' = [state EXCEPT ![t] = "unknown"]
  /\ UNCHANGED <<instance, execution, executionInstance, admissionOpen,
                  quiesce, pending, checked, lastStoppedTask, lastStoppedInstance,
                  lastStoppedExpected>>

ObserveStopped(t, observedInstance) ==
  /\ state[t] \in {"stopping", "unknown"}
  /\ pending[t] = "stop"
  /\ observedInstance \in Instances
  /\ (~EnforceStopInstanceFence \/ observedInstance = instance[t])
  /\ state' = [state EXCEPT ![t] = "stopped"]
  /\ admissionOpen' = [admissionOpen EXCEPT ![t] = FALSE]
  /\ quiesce' = [quiesce EXCEPT ![t] = 0]
  /\ pending' = [pending EXCEPT ![t] = "none"]
  /\ lastStoppedTask' = t
  /\ lastStoppedInstance' = observedInstance
  /\ lastStoppedExpected' = instance[t]
  /\ UNCHANGED <<instance, execution, executionInstance, checked>>

ObserveRunning(t, observedInstance) ==
  /\ state[t] = "unknown"
  /\ pending[t] = "none"
  /\ observedInstance = instance[t]
  /\ state' = [state EXCEPT ![t] = "running"]
  /\ admissionOpen' = [admissionOpen EXCEPT ![t] = TRUE]
  /\ UNCHANGED <<instance, execution, executionInstance, quiesce, pending,
                  checked, lastStoppedTask, lastStoppedInstance,
                  lastStoppedExpected>>

HostRestart ==
  /\ \E t \in Active:
       /\ state' = [x \in Tasks |-> IF x \in Active THEN "unknown" ELSE state[x]]
       /\ execution' = [x \in Tasks |-> 0]
       /\ executionInstance' = [x \in Tasks |-> 0]
       /\ admissionOpen' = [x \in Tasks |-> FALSE]
       /\ quiesce' = [x \in Tasks |-> 0]
       /\ pending' = [x \in Tasks |-> IF x \in Active THEN "none" ELSE pending[x]]
       /\ UNCHANGED <<instance, checked, lastStoppedTask,
                       lastStoppedInstance, lastStoppedExpected>>

Next ==
  \/ \E t \in Tasks: CheckStart(t) \/ BeginStart(t) \/ LoseStartResponse(t)
  \/ \E t \in Tasks, i \in Instances: CommitRunning(t, i)
  \/ \E t \in Tasks, e \in Executions, i \in Instances: Acquire(t, e, i) \/ Release(t, e, i)
  \/ \E t \in Tasks, q \in QuiesceCommands: BeginQuiesce(t, q) \/ CancelQuiesce(t, q) \/ BeginStop(t, q)
  \/ \E t \in Tasks: LoseStopResponse(t)
  \/ \E t \in Tasks, i \in Instances: ObserveStopped(t, i)
  \/ \E t \in Tasks, i \in Instances: ObserveRunning(t, i)
  \/ HostRestart

TypeOK ==
  /\ state \in [Tasks -> States]
  /\ instance \in [Tasks -> Instances \cup {0}]
  /\ execution \in [Tasks -> Executions \cup {0}]
  /\ executionInstance \in [Tasks -> Instances \cup {0}]
  /\ admissionOpen \in [Tasks -> BOOLEAN]
  /\ quiesce \in [Tasks -> QuiesceCommands \cup {0}]
  /\ pending \in [Tasks -> {"none", "start", "stop"}]
  /\ checked \subseteq Tasks
  /\ lastStoppedTask \in Tasks \cup {0}
  /\ lastStoppedInstance \in Instances \cup {0}
  /\ lastStoppedExpected \in Instances \cup {0}

ActiveBound == Cardinality(Active) <= Capacity

ExecutionRequiresCurrentInstance ==
  \A t \in Tasks:
    execution[t] # 0 =>
      /\ state[t] = "running"
      /\ executionInstance[t] = instance[t]
      /\ admissionOpen[t]

QuiesceExcludesExecution ==
  \A t \in Tasks: quiesce[t] # 0 => ~admissionOpen[t] /\ execution[t] = 0

ExactStopObservation ==
  lastStoppedTask = 0 \/ lastStoppedInstance = lastStoppedExpected

RestartPreservesActiveSlots ==
  Cardinality(Active) <= Capacity

Spec == Init /\ [][Next]_vars
=============================================================================
