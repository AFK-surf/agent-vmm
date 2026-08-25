------------------------ MODULE DomainAuthorityIsolation ----------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Domains, MaxRevision, EnforceIdentity, EnforceIsolation

VARIABLES mutationOwner, resourceRevision, crossDomainMutation

vars == <<mutationOwner, resourceRevision, crossDomainMutation>>

Other(d) == CHOOSE other \in Domains: other # d

Init ==
  /\ Cardinality(Domains) >= 2
  /\ mutationOwner = [d \in Domains |-> d]
  /\ resourceRevision = [d \in Domains |-> 0]
  /\ crossDomainMutation = FALSE

AuthorizedCommand(d) ==
  /\ resourceRevision[d] < MaxRevision
  /\ resourceRevision' = [resourceRevision EXCEPT ![d] = @ + 1]
  /\ UNCHANGED <<mutationOwner, crossDomainMutation>>

WrongIdentityCommand(d) ==
  /\ ~EnforceIdentity
  /\ mutationOwner' = [mutationOwner EXCEPT ![d] = Other(d)]
  /\ UNCHANGED <<resourceRevision, crossDomainMutation>>

CrossDomainFailure(d) ==
  /\ ~EnforceIsolation
  /\ resourceRevision[d] < MaxRevision
  /\ resourceRevision[Other(d)] < MaxRevision
  /\ resourceRevision' = [resourceRevision EXCEPT
       ![d] = @ + 1, ![Other(d)] = @ + 1]
  /\ crossDomainMutation' = TRUE
  /\ UNCHANGED mutationOwner

Next == \E d \in Domains:
  AuthorizedCommand(d) \/ WrongIdentityCommand(d) \/ CrossDomainFailure(d)

TypeOK ==
  /\ mutationOwner \in [Domains -> Domains]
  /\ resourceRevision \in [Domains -> 0..MaxRevision]
ExactRegistrationIdentity == \A d \in Domains: mutationOwner[d] = d
DomainNonInterference == ~crossDomainMutation

Spec == Init /\ [][Next]_vars

=============================================================================
