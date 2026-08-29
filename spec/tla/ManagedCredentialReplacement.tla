-------------------- MODULE ManagedCredentialReplacement --------------------
(***************************************************************************)
(* One device's controller-signed membership credential at one policy      *)
(* revision. A byte-identical install is idempotent. A different valid     *)
(* credential at that revision may replace the old payload only after the  *)
(* stored credential expires. Invalid signatures never become stored.      *)
(*                                                                         *)
(* Implementation anchors:                                                *)
(* - internal/host/trust/manager.go                                       *)
(* - internal/host/store/trust.go                                         *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS UnsafeUnexpiredReplacement, UnsafeForgedReplacement

VARIABLES stored, storedExpired, policyRevision, pulse

vars == <<stored, storedExpired, policyRevision, pulse>>

Init ==
  /\ stored = "old"
  /\ storedExpired = FALSE
  /\ policyRevision = 2
  /\ pulse = FALSE

Expire ==
  /\ ~storedExpired
  /\ storedExpired' = TRUE
  /\ UNCHANGED <<stored, policyRevision, pulse>>

InstallIdentical ==
  /\ stored \in {"old", "new"}
  /\ pulse' = ~pulse
  /\ UNCHANGED <<stored, storedExpired, policyRevision>>

InstallValidReplacement ==
  /\ stored = "old"
  /\ storedExpired
  /\ stored' = "new"
  /\ UNCHANGED <<storedExpired, policyRevision, pulse>>

RejectUnexpiredReplacement ==
  /\ stored = "old"
  /\ ~storedExpired
  /\ pulse' = ~pulse
  /\ UNCHANGED <<stored, storedExpired, policyRevision>>

RejectForgedReplacement ==
  /\ stored \in {"old", "new"}
  /\ pulse' = ~pulse
  /\ UNCHANGED <<stored, storedExpired, policyRevision>>

ReplaceBeforeExpiry ==
  /\ UnsafeUnexpiredReplacement
  /\ stored = "old"
  /\ ~storedExpired
  /\ stored' = "new"
  /\ UNCHANGED <<storedExpired, policyRevision, pulse>>

InstallForged ==
  /\ UnsafeForgedReplacement
  /\ stored \in {"old", "new"}
  /\ stored' = "forged"
  /\ UNCHANGED <<storedExpired, policyRevision, pulse>>

Next == Expire \/ InstallIdentical \/ InstallValidReplacement
        \/ RejectUnexpiredReplacement \/ RejectForgedReplacement
        \/ ReplaceBeforeExpiry \/ InstallForged

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ stored \in {"old", "new", "forged"}
  /\ storedExpired \in BOOLEAN
  /\ policyRevision \in Nat
  /\ pulse \in BOOLEAN

ReplacementRequiresExpired == stored = "new" => storedExpired
ForgedCredentialNeverStored == stored # "forged"
PolicyRevisionIsStable == policyRevision = 2

=============================================================================
