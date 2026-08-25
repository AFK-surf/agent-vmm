-------------------------- MODULE AggregateAdmission -------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Domains, MaxResource, EnforceAdmission

VARIABLE reservation

vars == <<reservation>>

RECURSIVE DomainSum(_,_)
DomainSum(domains, values) ==
  IF domains = {} THEN 0
  ELSE LET d == CHOOSE item \in domains: TRUE IN
    values[d] + DomainSum(domains \ {d}, values)

SumReservations == DomainSum(Domains, reservation)

Init ==
  /\ Cardinality(Domains) >= 2
  /\ reservation = [d \in Domains |-> 0]

Allocate(d) ==
  /\ IF EnforceAdmission
        THEN Cardinality(Domains) + SumReservations < MaxResource
        ELSE reservation[d] < MaxResource
  /\ reservation' = [reservation EXCEPT ![d] = @ + 1]

Delete(d) ==
  /\ reservation[d] > 0
  /\ reservation' = [reservation EXCEPT ![d] = @ - 1]

Next == \E d \in Domains: Allocate(d) \/ Delete(d)

TypeOK == reservation \in [Domains -> Nat]
AggregateNoOvercommit == Cardinality(Domains) + SumReservations <= MaxResource

Spec == Init /\ [][Next]_vars

=============================================================================
