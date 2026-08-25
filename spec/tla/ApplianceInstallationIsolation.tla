-------------------- MODULE ApplianceInstallationIsolation -------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Installations, EnforceIsolation

VARIABLES phase, removeOwner
vars == <<phase, removeOwner>>
Other(i) == CHOOSE j \in Installations: j # i

Init ==
  /\ Cardinality(Installations) >= 2
  /\ phase = [i \in Installations |-> "PENDING_DESTROY"]
  /\ removeOwner = [i \in Installations |-> "NONE"]

ExecuteOwnRemove(i) ==
  /\ phase[i] = "PENDING_DESTROY"
  /\ phase' = [phase EXCEPT ![i] = "REMOVED"]
  /\ removeOwner' = [removeOwner EXCEPT ![i] = i]

CrossDomainRemove(i) ==
  /\ ~EnforceIsolation /\ phase[i] = "PENDING_DESTROY"
  /\ phase' = [phase EXCEPT ![Other(i)] = "REMOVED"]
  /\ removeOwner' = [removeOwner EXCEPT ![Other(i)] = i]

Next == \E i \in Installations: ExecuteOwnRemove(i) \/ CrossDomainRemove(i)

TypeOK ==
  /\ phase \in [Installations -> {"PENDING_DESTROY", "REMOVED"}]
  /\ removeOwner \in [Installations -> Installations \cup {"NONE"}]
InstallationNonInterference ==
  \A i \in Installations: phase[i] = "REMOVED" => removeOwner[i] = i

Spec == Init /\ [][Next]_vars

=============================================================================
