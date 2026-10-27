--------------------- MODULE ApplianceRecoverySelection ---------------------
EXTENDS Naturals
CONSTANTS HostFirst, CheckRevision, CheckReady
VARIABLE s
vars == <<s>>
Actors == {"foreground", "background"}

Init == s = [revision |-> 0, epoch |-> 0, guest |-> FALSE,
  desired |-> TRUE, published |-> FALSE, owner |-> "none",
  step |-> [a \in Actors |-> IF HostFirst THEN "capture" ELSE "observe"],
  candidate |-> [a \in Actors |-> 0], observed |-> [a \in Actors |-> FALSE],
  claim |-> [a \in Actors |-> 0], disabled |-> FALSE, restarted |-> FALSE,
  returned |-> FALSE, violations |-> {}]

CaptureHost(a) ==
  /\ s.step[a] = "capture"
  /\ s' = [s EXCEPT !.candidate[a] = s.revision,
       !.step[a] = IF HostFirst THEN "observe" ELSE "decide"]

ObserveGuest(a) ==
  /\ s.step[a] = "observe"
  /\ s' = [s EXCEPT !.observed[a] = s.guest,
       !.step[a] = IF HostFirst THEN "decide" ELSE "capture"]

SelectRecovery(a) ==
  /\ s.step[a] = "decide" /\ s.owner = "none"
  /\ IF s.observed[a] \/ ~s.desired \/
          (CheckRevision /\ s.candidate[a] # s.revision)
     THEN s' = [s EXCEPT !.step[a] = "done"]
     ELSE s' = [s EXCEPT !.step[a] = "prepared", !.claim[a] = s.epoch,
          !.revision = IF s.published THEN @ + 1 ELSE @,
          !.candidate[a] = IF s.published THEN s.revision + 1 ELSE s.revision,
          !.published = FALSE, !.owner = a]

Launch(a) ==
  /\ s.step[a] = "prepared"
  /\ s.owner = a
  /\ IF s.claim[a] # s.epoch \/ ~s.desired \/ s.candidate[a] # s.revision
     THEN s' = [s EXCEPT !.step[a] = "done", !.owner = "none"]
     ELSE s' = [s EXCEPT !.step[a] = "commit", !.guest = TRUE,
          !.revision = @ + 1,
          !.violations = IF s.guest THEN @ \cup {"duplicate_launch"} ELSE @]

Publish(a) ==
  /\ s.step[a] = "commit"
  /\ s.owner = a
  /\ s' = [s EXCEPT !.published = s.guest /\ s.desired /\ s.claim[a] = s.epoch /\ s.revision = s.candidate[a] + 1,
       !.guest = s.guest /\ s.desired /\ s.claim[a] = s.epoch /\ s.revision = s.candidate[a] + 1,
       !.step[a] = "done", !.revision = @ + 1, !.owner = "none"]

ExplicitDisable ==
  /\ ~s.disabled
  /\ s' = [s EXCEPT !.disabled = TRUE, !.desired = FALSE,
       !.revision = @ + 1, !.published = FALSE]

GuestRestart ==
  /\ ~s.restarted
  /\ s' = [s EXCEPT !.restarted = TRUE, !.epoch = @ + 1,
       !.guest = FALSE, !.published = FALSE]

Ready == ~s.desired \/ s.published
SyncReturn ==
  /\ s.step["foreground"] = "done" /\ ~s.returned
  /\ (~CheckReady \/ Ready)
  /\ s' = [s EXCEPT !.returned = TRUE,
       !.violations = IF Ready THEN @ ELSE @ \cup {"false_completion"}]

Terminated == (\A a \in Actors: s.step[a] = "done") /\ s.returned /\ s.disabled /\ s.restarted /\ UNCHANGED vars

Next == Terminated \/ (\E a \in Actors: CaptureHost(a) \/ ObserveGuest(a) \/
          SelectRecovery(a) \/ Launch(a) \/ Publish(a)) \/
        ExplicitDisable \/ GuestRestart \/ SyncReturn
Spec == Init /\ [][Next]_vars
NoDuplicateLaunch == "duplicate_launch" \notin s.violations
SynchronousCompletion == "false_completion" \notin s.violations
=============================================================================
