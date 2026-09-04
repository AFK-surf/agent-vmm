---------------------------- MODULE GuestOCIStartup --------------------------
EXTENDS Naturals

CONSTANT EnforceExistingGCDrain

VARIABLES kind, phase, gcPending, unsafeTighten

\* Implementation anchor: internal/guest/control/runtime_linux.go
\* settleOCIStartupQuota and cmd/agent-vmmd/main_linux.go startup ordering.

vars == <<kind, phase, gcPending, unsafeTighten>>

Init ==
  /\ kind \in {"fresh", "existing"}
  /\ phase = "allowance"
  /\ gcPending = (kind = "existing")
  /\ unsafeTighten = FALSE

StartContainerd ==
  /\ phase = "allowance"
  /\ phase' = "containerd"
  /\ UNCHANGED <<kind, gcPending, unsafeTighten>>

RecoverState ==
  /\ phase = "containerd"
  /\ phase' = "recovered"
  /\ UNCHANGED <<kind, gcPending, unsafeTighten>>

DrainExistingGC ==
  /\ kind = "existing"
  /\ phase = "recovered"
  /\ gcPending
  /\ phase' = "gc_drained"
  /\ gcPending' = FALSE
  /\ UNCHANGED <<kind, unsafeTighten>>

TightenFresh ==
  /\ kind = "fresh"
  /\ phase = "recovered"
  /\ phase' = "tightened"
  /\ UNCHANGED <<kind, gcPending, unsafeTighten>>

TightenExisting ==
  /\ kind = "existing"
  /\ phase \in IF EnforceExistingGCDrain THEN {"gc_drained"}
                 ELSE {"recovered", "gc_drained"}
  /\ unsafeTighten' = (gcPending \/ unsafeTighten)
  /\ phase' = "tightened"
  /\ UNCHANGED <<kind, gcPending>>

PublishReady ==
  /\ phase = "tightened"
  /\ phase' = "ready"
  /\ UNCHANGED <<kind, gcPending, unsafeTighten>>

Next == StartContainerd \/ RecoverState \/ DrainExistingGC \/
        TightenFresh \/ TightenExisting \/ PublishReady

TypeOK ==
  /\ kind \in {"fresh", "existing"}
  /\ phase \in {"allowance", "containerd", "recovered", "gc_drained", "tightened", "ready"}
  /\ gcPending \in BOOLEAN
  /\ unsafeTighten \in BOOLEAN

ExistingDrainBeforeTighten == ~unsafeTighten

Spec == Init /\ [][Next]_vars

=============================================================================
