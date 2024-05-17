---- MODULE mcLeaseRaftLogBased ----
EXTENDS TLC, leaseRaftLogBased

\* State Constraint. Used for model checking only.
CONSTANTS MaxTerm, MaxLogLen, MaxClock

StateConstraint ==
    /\ \A s \in Server :
        /\ currentTerm[s] <= MaxTerm
        /\ Len(log[s]) <= MaxLogLen
        /\ clock <= MaxClock

ServerSymmetry == Permutations(Server)

Alias == [
    currentTerm |-> currentTerm,
    state |-> state,
    log |-> log,
    committed |-> committed,
    clock |-> clock,
    lease |-> lease,
    latestRead |-> latestRead,
    n1MaxReplicated |-> MaxMajorityReplicatedEntry("n1"),
    n2MaxReplicated |-> MaxMajorityReplicatedEntry("n2"),
    n3MaxReplicated |-> MaxMajorityReplicatedEntry("n3")
]
====
