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

SeqToSet(S) == { S[i] : i \in 1..Len(S) }

\* This is a "view". The model-checker considers two states equivalent if they
\* produce the same view. We reduce the state space by normalizing replication
\* times so the smallest time is always 0: e.g., [server1 |-> <<1, 2>>] is the
\* same as [server1 |-> <<2, 3>>], they are both normalized to [server1 |-> 
\* <<0, 1>>]. Since the spec only cares about time differences, not absolute
\* time, it's safe to normalize times this way and it makes model-checking
\* practical. See Lamport, "Real-Time Model Checking is Really Simple", 2005.
ClockAbstractionView == LET
    allTimes == UNION { SeqToSet(replicationTimes[s]) : s \in Server }
    start == IF allTimes = {} THEN 0 ELSE Min(allTimes)
    normalizedReplicationTimes == [
        s \in Server |-> [
            i \in DOMAIN replicationTimes[s] |-> replicationTimes[s][i] - start
        ]
    ]
    \* All vars but replace replicationTimes w/ normalizedReplicationTimes, omit clock!
    IN [
        currentTerm |-> currentTerm,
        state |-> state,
        log |-> log,
        committed |-> committed,
        commitIndex |-> commitIndex,
        latestRead |-> latestRead,
        normalizedReplicationTimes |-> normalizedReplicationTimes
    ]

====
