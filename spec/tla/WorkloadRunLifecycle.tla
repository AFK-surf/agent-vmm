------------------------- MODULE WorkloadRunLifecycle ------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS EnforceLifecycleFence, EnforceActivityInstanceFence,
          EnforceExactActivityRelease, EnforceQuiesceFence,
          EnforceStopInstanceFence

Tasks == {1, 2}
Instances == {1, 2}
ActivityIDs == {1, 2}
QuiesceCommands == {1, 2}
States == {"stopped", "starting", "running", "stopping", "unknown"}

VARIABLES state, instance, activities, activityInstance, admissionOpen,
          quiesce, pending, lastReleasedTask, lastReleasedActivity,
          lastReleaseBefore, lastReleaseAfter, lastStoppedTask,
          lastStoppedInstance, lastStoppedExpected

vars == <<state, instance, activities, activityInstance, admissionOpen,
          quiesce, pending, lastReleasedTask, lastReleasedActivity,
          lastReleaseBefore, lastReleaseAfter, lastStoppedTask,
          lastStoppedInstance, lastStoppedExpected>>

Active == {t \in Tasks : state[t] # "stopped"}

Init ==
  /\ state = [t \in Tasks |-> "stopped"]
  /\ instance = [t \in Tasks |-> 0]
  /\ activities = [t \in Tasks |-> {}]
  /\ activityInstance = [t \in Tasks |-> [a \in ActivityIDs |-> 0]]
  /\ admissionOpen = [t \in Tasks |-> FALSE]
  /\ quiesce = [t \in Tasks |-> 0]
  /\ pending = [t \in Tasks |-> "none"]
  /\ lastReleasedTask = 0
  /\ lastReleasedActivity = 0
  /\ lastReleaseBefore = {}
  /\ lastReleaseAfter = {}
  /\ lastStoppedTask = 0
  /\ lastStoppedInstance = 0
  /\ lastStoppedExpected = 0

BeginStart(t) ==
  /\ (~EnforceLifecycleFence \/ state[t] = "stopped")
  /\ state' = [state EXCEPT ![t] = "starting"]
  /\ admissionOpen' = [admissionOpen EXCEPT ![t] = FALSE]
  /\ pending' = [pending EXCEPT ![t] = "start"]
  /\ UNCHANGED <<instance, activities, activityInstance, quiesce,
                  lastReleasedTask, lastReleasedActivity, lastReleaseBefore,
                  lastReleaseAfter, lastStoppedTask, lastStoppedInstance,
                  lastStoppedExpected>>

CommitRunning(t, i) ==
  /\ state[t] \in {"starting", "unknown"}
  /\ pending[t] = "start"
  /\ i \in Instances
  /\ i # instance[t]
  /\ state' = [state EXCEPT ![t] = "running"]
  /\ instance' = [instance EXCEPT ![t] = i]
  /\ admissionOpen' = [admissionOpen EXCEPT ![t] = TRUE]
  /\ pending' = [pending EXCEPT ![t] = "none"]
  /\ UNCHANGED <<activities, activityInstance, quiesce, lastReleasedTask,
                  lastReleasedActivity, lastReleaseBefore, lastReleaseAfter,
                  lastStoppedTask, lastStoppedInstance, lastStoppedExpected>>

LoseStartResponse(t) ==
  /\ state[t] = "starting"
  /\ state' = [state EXCEPT ![t] = "unknown"]
  /\ UNCHANGED <<instance, activities, activityInstance, admissionOpen,
                  quiesce, pending, lastReleasedTask, lastReleasedActivity,
                  lastReleaseBefore, lastReleaseAfter, lastStoppedTask,
                  lastStoppedInstance, lastStoppedExpected>>

Acquire(t, a, observedInstance) ==
  /\ state[t] = "running"
  /\ a \in ActivityIDs
  /\ a \notin activities[t]
  /\ observedInstance \in Instances
  /\ (~EnforceQuiesceFence \/ admissionOpen[t])
  /\ (~EnforceActivityInstanceFence \/ observedInstance = instance[t])
  /\ activities' = [activities EXCEPT ![t] = @ \cup {a}]
  /\ activityInstance' = [activityInstance EXCEPT ![t][a] = observedInstance]
  /\ UNCHANGED <<state, instance, admissionOpen, quiesce, pending,
                  lastReleasedTask, lastReleasedActivity, lastReleaseBefore,
                  lastReleaseAfter, lastStoppedTask, lastStoppedInstance,
                  lastStoppedExpected>>

RetryAcquire(t, a, observedInstance) ==
  /\ state[t] = "running"
  /\ a \in activities[t]
  /\ activityInstance[t][a] = observedInstance
  /\ observedInstance = instance[t]
  /\ UNCHANGED vars

Release(t, a, observedInstance) ==
  /\ a \in activities[t]
  /\ activityInstance[t][a] = observedInstance
  /\ observedInstance = instance[t]
  /\ activities' = [activities EXCEPT
       ![t] = IF EnforceExactActivityRelease THEN @ \ {a} ELSE {}]
  /\ activityInstance' = [activityInstance EXCEPT
       ![t] = IF EnforceExactActivityRelease
               THEN [@ EXCEPT ![a] = 0]
               ELSE [x \in ActivityIDs |-> 0]]
  /\ lastReleasedTask' = t
  /\ lastReleasedActivity' = a
  /\ lastReleaseBefore' = activities[t]
  /\ lastReleaseAfter' = activities'[t]
  /\ UNCHANGED <<state, instance, admissionOpen, quiesce, pending,
                  lastStoppedTask, lastStoppedInstance, lastStoppedExpected>>

BeginQuiesce(t, q) ==
  /\ state[t] = "running"
  /\ activities[t] = {}
  /\ q \in QuiesceCommands
  /\ quiesce[t] = 0
  /\ admissionOpen' = [admissionOpen EXCEPT ![t] = FALSE]
  /\ quiesce' = [quiesce EXCEPT ![t] = q]
  /\ UNCHANGED <<state, instance, activities, activityInstance, pending,
                  lastReleasedTask, lastReleasedActivity, lastReleaseBefore,
                  lastReleaseAfter, lastStoppedTask, lastStoppedInstance,
                  lastStoppedExpected>>

CancelQuiesce(t, q) ==
  /\ state[t] = "running"
  /\ quiesce[t] = q
  /\ activities[t] = {}
  /\ admissionOpen' = [admissionOpen EXCEPT ![t] = TRUE]
  /\ quiesce' = [quiesce EXCEPT ![t] = 0]
  /\ UNCHANGED <<state, instance, activities, activityInstance, pending,
                  lastReleasedTask, lastReleasedActivity, lastReleaseBefore,
                  lastReleaseAfter, lastStoppedTask, lastStoppedInstance,
                  lastStoppedExpected>>

BeginStop(t, q) ==
  /\ state[t] = "running"
  /\ q \in QuiesceCommands
  /\ IF EnforceQuiesceFence
        THEN /\ quiesce[t] = q
             /\ ~admissionOpen[t]
             /\ activities[t] = {}
        ELSE TRUE
  /\ state' = [state EXCEPT ![t] = "stopping"]
  /\ admissionOpen' = [admissionOpen EXCEPT ![t] = FALSE]
  /\ pending' = [pending EXCEPT ![t] = "stop"]
  /\ UNCHANGED <<instance, activities, activityInstance, quiesce,
                  lastReleasedTask, lastReleasedActivity, lastReleaseBefore,
                  lastReleaseAfter, lastStoppedTask, lastStoppedInstance,
                  lastStoppedExpected>>

LoseStopResponse(t) ==
  /\ state[t] = "stopping"
  /\ state' = [state EXCEPT ![t] = "unknown"]
  /\ UNCHANGED <<instance, activities, activityInstance, admissionOpen,
                  quiesce, pending, lastReleasedTask, lastReleasedActivity,
                  lastReleaseBefore, lastReleaseAfter, lastStoppedTask,
                  lastStoppedInstance, lastStoppedExpected>>

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
  /\ UNCHANGED <<instance, activities, activityInstance, lastReleasedTask,
                  lastReleasedActivity, lastReleaseBefore, lastReleaseAfter>>

ObserveRunning(t, observedInstance) ==
  /\ state[t] = "unknown"
  /\ pending[t] = "none"
  /\ observedInstance = instance[t]
  /\ state' = [state EXCEPT ![t] = "running"]
  /\ admissionOpen' = [admissionOpen EXCEPT ![t] = TRUE]
  /\ UNCHANGED <<instance, activities, activityInstance, quiesce, pending,
                  lastReleasedTask, lastReleasedActivity, lastReleaseBefore,
                  lastReleaseAfter, lastStoppedTask, lastStoppedInstance,
                  lastStoppedExpected>>

HostRestart ==
  /\ \E t \in Active:
       /\ state' = [x \in Tasks |-> IF x \in Active THEN "unknown" ELSE state[x]]
       /\ activities' = [x \in Tasks |-> {}]
       /\ activityInstance' = [x \in Tasks |-> [a \in ActivityIDs |-> 0]]
       /\ admissionOpen' = [x \in Tasks |-> FALSE]
       /\ quiesce' = [x \in Tasks |-> 0]
       /\ pending' = [x \in Tasks |-> IF x \in Active THEN "none" ELSE pending[x]]
       /\ UNCHANGED <<instance, lastReleasedTask, lastReleasedActivity,
                       lastReleaseBefore, lastReleaseAfter, lastStoppedTask,
                       lastStoppedInstance, lastStoppedExpected>>

Next ==
  \/ \E t \in Tasks: BeginStart(t) \/ LoseStartResponse(t)
  \/ \E t \in Tasks, i \in Instances: CommitRunning(t, i)
  \/ \E t \in Tasks, a \in ActivityIDs, i \in Instances:
       Acquire(t, a, i) \/ RetryAcquire(t, a, i) \/ Release(t, a, i)
  \/ \E t \in Tasks, q \in QuiesceCommands:
       BeginQuiesce(t, q) \/ CancelQuiesce(t, q) \/ BeginStop(t, q)
  \/ \E t \in Tasks: LoseStopResponse(t)
  \/ \E t \in Tasks, i \in Instances: ObserveStopped(t, i)
  \/ \E t \in Tasks, i \in Instances: ObserveRunning(t, i)
  \/ HostRestart

TypeOK ==
  /\ state \in [Tasks -> States]
  /\ instance \in [Tasks -> Instances \cup {0}]
  /\ activities \in [Tasks -> SUBSET ActivityIDs]
  /\ activityInstance \in [Tasks -> [ActivityIDs -> Instances \cup {0}]]
  /\ admissionOpen \in [Tasks -> BOOLEAN]
  /\ quiesce \in [Tasks -> QuiesceCommands \cup {0}]
  /\ pending \in [Tasks -> {"none", "start", "stop"}]
  /\ lastReleasedTask \in Tasks \cup {0}
  /\ lastReleasedActivity \in ActivityIDs \cup {0}
  /\ lastReleaseBefore \in SUBSET ActivityIDs
  /\ lastReleaseAfter \in SUBSET ActivityIDs
  /\ lastStoppedTask \in Tasks \cup {0}
  /\ lastStoppedInstance \in Instances \cup {0}
  /\ lastStoppedExpected \in Instances \cup {0}

ActivityRequiresCurrentInstance ==
  \A t \in Tasks:
    activities[t] # {} =>
      /\ state[t] = "running"
      /\ admissionOpen[t]
      /\ \A a \in activities[t]: activityInstance[t][a] = instance[t]

ActivityTracksExactInstance ==
  \A t \in Tasks, a \in ActivityIDs:
    (a \in activities[t]) <=> (activityInstance[t][a] # 0)

ExactActivityRelease ==
  lastReleasedTask = 0 \/
    lastReleaseAfter = lastReleaseBefore \ {lastReleasedActivity}

QuiesceExcludesActivity ==
  \A t \in Tasks: quiesce[t] # 0 => ~admissionOpen[t] /\ activities[t] = {}

ExactStopObservation ==
  lastStoppedTask = 0 \/ lastStoppedInstance = lastStoppedExpected

Spec == Init /\ [][Next]_vars
=============================================================================
