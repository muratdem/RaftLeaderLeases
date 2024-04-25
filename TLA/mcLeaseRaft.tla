---- MODULE mcLeaseRaft ----
EXTENDS TLC, leaseRaft1

\* State Constraint. Used for model checking only.
CONSTANTS MaxTerm, MaxLogLen, MaxClock

StateConstraint == 
    /\ \A s \in Server :
        /\ currentTerm[s] <= MaxTerm
        /\ Len(log[s]) <= MaxLogLen
        /\ clock <= MaxClock

ServerSymmetry == Permutations(Server)      

BaitInv == TLCGet("level") < 99

====



For leaseRaft2 use this one:

---- MODULE mcLeaseRaft ----
EXTENDS TLC, leaseRaft2

\* State Constraint. Used for model checking only.
CONSTANTS MaxTerm, MaxLogLen, MaxClock

StateConstraint == 
    /\ \A s \in Server :
        /\ currentTerm[s] <= MaxTerm
        /\ Len(log[s]) <= MaxLogLen
        /\ clock[s] <= MaxClock

ServerSymmetry == Permutations(Server)      

BaitInv == TLCGet("level") < 99