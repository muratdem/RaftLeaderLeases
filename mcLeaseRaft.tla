---- MODULE mcLeaseRaft ----
EXTENDS TLC, leaseRaft1

\* State Constraint. Used for model checking only.
CONSTANTS MaxTerm, MaxLogLen, MaxClock

StateConstraint == 
    /\ clock < MaxClock
    /\ \A s \in Server :
        /\ currentTerm[s] <= MaxTerm
        /\ Len(log[s]) <= MaxLogLen


ServerSymmetry == Permutations(Server)      

BaitInv == TLCGet("level") < 99
====
