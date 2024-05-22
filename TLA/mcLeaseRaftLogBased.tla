---- MODULE mcLeaseRaftLogBased ----
EXTENDS TLC, leaseRaftLogBased

CONSTANTS MaxTerm, MaxLogLen, MaxClock

\* Used for model checking only.
StateConstraint ==
    /\ \A s \in Server :
        /\ currentTerm[s] <= MaxTerm
        /\ Len(log[s]) <= MaxLogLen
        /\ clock <= MaxClock

ServerSymmetry == Permutations(Server)

\* Add info to error traces.
Alias == [
    currentTerm |-> currentTerm,
    state |-> state,
    log |-> log,
    replicationTimes |-> replicationTimes,
    committed |-> committed,
    clock |-> clock,
    latestRead |-> latestRead,
    whichServersHaveLeases |-> [s \in Server |-> [read |-> HasLease(s, TRUE), write |-> HasLease(s, TRUE)]]
]
====
