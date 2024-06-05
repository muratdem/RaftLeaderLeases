---- MODULE leaseRaftLogBased ----
\*
\* MongoDB consensus plus leases, with no separate data structure
\* for the lease: the log is the lease.
\*

EXTENDS Naturals, Integers, FiniteSets, Sequences, TLC

CONSTANTS Server
CONSTANTS Follower, Leader, Nil, Delta

VARIABLE currentTerm
VARIABLE state
VARIABLE log
VARIABLE replicationTimes
VARIABLE committed
VARIABLE commitIndex
VARIABLE clock
VARIABLE latestRead

Entry == [term: Int, clock: Int, index: Int]
TypeOK ==
    /\ currentTerm \in [Server -> Int]
    /\ state \in [Server -> {Leader, Follower}]
    /\ log \in [Server -> Seq(Entry)]
    /\ replicationTimes \in [Server -> Seq(Int)]
    /\ committed \in SUBSET Entry
    /\ commitIndex \in [Server -> Int]
    /\ clock \in Int
    /\ latestRead \in Entry

vars == <<currentTerm, state, log, replicationTimes, committed, clock, latestRead>>

\*
\* Helper operators.
\*

\* Is a sequence empty.
Empty(s) == Len(s) = 0
Max(S) == CHOOSE i \in S: (\A j \in S: i>=j)
Min(S) == CHOOSE i \in S: (\A j \in S: i=<j) 
CreateEntry(xterm, xindex) == [term |-> xterm, index |-> xindex]
MaxCommitted(S) == IF Cardinality(S) = 0 THEN CreateEntry(0, 0)
                   ELSE CHOOSE i \in S: (\A j \in S: i.index >= j.index)


\* Is log entry e in the log of node 'i'.
InLog(e, i) == log[i][e.index] = e

\* The term of the last entry in a log, or 0 if the log is empty.
LastTerm(xlog) == IF Len(xlog) = 0 THEN 0 ELSE xlog[Len(xlog)].term

\* The set of all quorums in a given set.
Quorums(S) == {i \in SUBSET(S) : Cardinality(i) * 2 > Cardinality(S)}

IsPrefix(s, t) ==
  (**************************************************************************)
  (* TRUE iff the sequence s is a prefix of the sequence t, s.t.            *)
  (* \E u \in Seq(Range(t)) : t = s \o u. In other words, there exists      *)
  (* a suffix u that with s prepended equals t.                             *)
  (**************************************************************************)
  Len(s) <= Len(t) /\ SubSeq(s, 1, Len(s)) = SubSeq(t, 1, Len(s))

CanRollback(i, j) ==
    /\ LastTerm(log[i]) < LastTerm(log[j])
    /\ ~IsPrefix(log[i],log[j])

\* Can node 'i' currently cast a vote for node 'j' in term 'term'.
CanVoteForOplog(i, j, term) ==
    LET logOk ==
        \/ LastTerm(log[j]) > LastTerm(log[i])
        \/ /\ LastTerm(log[j]) = LastTerm(log[i])
           /\ Len(log[j]) >= Len(log[i]) IN
    /\ currentTerm[i] < term
    /\ logOk

\* Is a log entry 'e' immediately committed with a quorum 'Q'.
ImmediatelyCommitted(e, Q) ==
    \A s \in Q :
        /\ Len(log[s]) >= e.index
        /\ log[s][e.index] = e
        /\ currentTerm[s] = e.term  \* they are in the same term as the log entry.

\* Helper operator for actions that propagate the term between two nodes.
UpdateTermsExpr(i, j) ==
    /\ currentTerm[i] > currentTerm[j]
    /\ currentTerm' = [currentTerm EXCEPT ![j] = currentTerm[i]]
    /\ state' = [state EXCEPT ![j] = Follower]

\* Highest entry in server s's log that is majority-replicated.
MaxMajorityReplicatedIndex(s) ==
    LET indexes == {index \in DOMAIN log[s] :
        ({t \in Server : Len(log[t]) >= index /\ log[t][index] = log[s][index]} \in Quorums(Server))}
    IN
        IF Cardinality(indexes) > 0 THEN Max(indexes) ELSE 0

\* TRUE if server i has a lease for committing writes.
CanAdvanceCommitIndex(i) ==
    /\ state[i] = Leader
    /\ Len(log[i]) > 0
    /\ LET commit_index_tmp == MaxMajorityReplicatedIndex(i) IN
        /\ commit_index_tmp > 0
        \* My newest majority-replicated entry is newer than lease timeout
        /\ replicationTimes[i][commit_index_tmp] + Delta >= clock
        \* I have committed a log entry.
        /\ log[i][commit_index_tmp].term = currentTerm[i]
        \* Last entry replicated from prior leader was at least Delta ago.
        /\ ~(\E index \in DOMAIN log[i] : 
                (log[i][index].term # currentTerm[i] /\ (replicationTimes[i][index] + Delta >= clock)))

\* TRUE if server i has a (maybe inherited) lease for reads.
CanServeConsistentReads(i) ==
    /\ state[i] = Leader
    /\ Len(replicationTimes[i]) > 0
    \* My newest majority-replicated entry is newer than lease timeout
    /\ replicationTimes[i][Len(replicationTimes[i])] + Delta >= clock

--------------------------------------------------------------------------------

\*
\* Next state actions.
\*

\* Node 'i', a Leader, handles a new client request and places the entry
\* in its log. Our "deferred-commit write" optimization means any leader
\* can write, but only a leaseholder can commit.
ClientWrite(i) ==
    /\ state[i] = Leader
    /\ clock' = clock + 1
    /\ log' = [log EXCEPT ![i] = Append(log[i], CreateEntry(currentTerm[i], Len(log[i]) + 1))]
    /\ replicationTimes' = [replicationTimes EXCEPT ![i] = Append(replicationTimes[i], clock')]
    /\ UNCHANGED <<currentTerm, state, committed, commitIndex, latestRead>>

ClientRead(i) ==
    /\ CanServeConsistentReads(i)
    \* The next two lines handle SERVER-53813.
    /\ commitIndex[i] > 0
    /\ log[i][MaxMajorityReplicatedIndex(i)].term = currentTerm[i]
    /\ LET cInd == commitIndex[i]
           l == Len(log[i]) IN
        /\ latestRead' = IF cInd = 0 THEN CreateEntry(0, 0)
                         ELSE log[i][cInd] \* Raft guarantees cInd <= l
    /\ UNCHANGED <<currentTerm, state, log, replicationTimes, committed, commitIndex, clock>>

\* Node 'i' gets a new log entry from node 'j'.
GetEntries(i, j) ==
    /\ state[i] = Follower
    \* Node j must have more entries than node i.
    /\ Len(log[j]) > Len(log[i])
       \* Ensure that the entry at the last index of node i's log must match the entry at
       \* the same index in node j's log. If the log of node i is empty, then the check
       \* trivially passes. This is the essential 'log consistency check'.
    /\ LET logOk == IF Empty(log[i])
                        THEN TRUE
                        ELSE log[j][Len(log[i])] = log[i][Len(log[i])] IN
       /\ logOk \* log consistency check
       \* If the log of node i is empty, then take the first entry from node j's log.
       \* Otherwise take the entry following the last index of node i.
       /\ LET newEntryIndex == IF Empty(log[i]) THEN 1 ELSE Len(log[i]) + 1
              newEntry      == log[j][newEntryIndex]
              newLog        == Append(log[i], newEntry)
              newReplTimes  == Append(replicationTimes[i], clock) IN
              /\ log' = [log EXCEPT ![i] = newLog]
              /\ replicationTimes' = [replicationTimes EXCEPT ![i] = newReplTimes]
              /\ commitIndex' = [commitIndex EXCEPT ![i] = 
                  IF commitIndex[i] < commitIndex[j]
                  THEN Min ({commitIndex[j], Len(newLog)})
                  ELSE commitIndex[i]]   
    /\ UNCHANGED <<committed, currentTerm, state, clock, latestRead>>

\*  Node 'i' rolls back against the log of node 'j'.
RollbackEntries(i, j) ==
    /\ state[i] = Follower
    /\ CanRollback(i, j)
    \* Roll back one log entry.
    /\ log' = [log EXCEPT ![i] = SubSeq(log[i], 1, Len(log[i])-1)]
    /\ replicationTimes' = [replicationTimes EXCEPT ![i] = SubSeq(replicationTimes[i], 1, Len(log[i])-1)]
    /\ UNCHANGED <<committed, currentTerm, state, clock, latestRead>>

\* Node 'i' gets elected as a Leader.
BecomeLeader(i, voteQuorum) ==
    LET newTerm == currentTerm[i] + 1 IN
    /\ i \in voteQuorum \* The new leader should vote for itself.
    /\ \A v \in voteQuorum : CanVoteForOplog(v, i, newTerm)
    \* Update the terms of each voter.
    /\ currentTerm' = [s \in Server |-> IF s \in voteQuorum THEN newTerm ELSE currentTerm[s]]
    /\ state' = [s \in Server |->
                    IF s = i THEN Leader
                    ELSE IF s \in voteQuorum THEN Follower \* All voters should revert to Follower state.
                    ELSE state[s]]
    \* Write a no-op, as required by Raft and to get first lease of the term
    /\ log' = [log EXCEPT ![i] = Append(log[i], CreateEntry(newTerm, Len(log[i]) + 1))]
    /\ replicationTimes' = [replicationTimes EXCEPT ![i] = Append(replicationTimes[i], clock)]
    /\ UNCHANGED <<committed, commitIndex, latestRead, clock>>

\* Leader 'i' commits its latest log entry.
CommitEntry(i, commitQuorum) ==
    /\ CanAdvanceCommitIndex(i)
    \* Must have some entries to commit.
    /\ commitIndex[i] < Len(log[i]) 
    /\ LET ind == commitIndex[i]+1
           entry == log[i][ind] IN
        \* The entry was written by this leader.
        /\ entry.term = currentTerm[i]
        \* all nodes have this log entry and are in the term of the leader.
        /\ ImmediatelyCommitted(entry, commitQuorum)
        \* Don't mark an entry as committed more than once.
        /\ entry \notin committed
        /\ committed' = committed \cup {entry}
        /\ commitIndex' = [commitIndex EXCEPT ![i] = ind]
        /\ latestRead' = entry
        /\ UNCHANGED <<currentTerm, replicationTimes, state, log, clock>>

\* Action that exchanges terms between two nodes and step down the Leader if
\* needed. This can be safely specified as a separate action, rather than
\* occurring atomically on other replication actions that involve communication
\* between two nodes. This makes it clearer to see where term propagation is
\* strictly necessary for guaranteeing safety.
UpdateTerms(i, j) ==
    /\ UpdateTermsExpr(i, j)
    /\ UNCHANGED <<log, replicationTimes, committed, commitIndex, latestRead, clock>>

\* Action for incrementing the clock
Tick ==
    /\ clock' = clock + 1
    /\ UNCHANGED <<currentTerm, state, log, replicationTimes, committed, commitIndex, latestRead>>

Init ==
    /\ currentTerm = [i \in Server |-> 0]
    /\ state = [i \in Server |-> Follower]
    /\ log = [i \in Server |-> <<>>]
    /\ replicationTimes = [i \in Server |-> <<>>]
    /\ committed = {}
    /\ commitIndex = [i \in Server |-> 0]
    /\ clock = 0
    /\ latestRead = CreateEntry(0, 0)

Next ==
    \/ \E s \in Server : ClientWrite(s)
    \/ \E s \in Server : ClientRead(s)
    \/ \E s, t \in Server : GetEntries(s, t)
    \/ \E s, t \in Server : RollbackEntries(s, t)
    \/ \E s \in Server : \E Q \in Quorums(Server) : BecomeLeader(s, Q)
    \/ \E s \in Server : \E Q \in Quorums(Server) : CommitEntry(s, Q)
    \/ \E s,t \in Server : UpdateTerms(s, t)
    \/ Tick

Spec == Init /\ [][Next]_vars

--------------------------------------------------------------------------------

\*
\* Correctness properties
\*

EntryIndexes ==
    \A s \in Server:
        \A index \in DOMAIN log[s]:
            log[s][index].index = index

LogAndReplicationTimesLengths ==
    \A s \in Server:
        Len(log[s]) = Len(replicationTimes[s])

OneLeaderPerTerm ==
    \A s,t \in Server :
        (/\ state[s] = Leader
         /\ state[t] = Leader
         /\ currentTerm[s] = currentTerm[t]) => (s = t)

OneCommitLeaseHolder ==
    \A s, t \in Server :
        (CanAdvanceCommitIndex(s) /\ CanAdvanceCommitIndex(t)) => (s = t)

\* If a server can commit, no other server can do linearizable reads.
ReadLeaseSafety ==
    \A s, t \in Server :
        \* s has the right to advance commit index => no other server can read
        (CanAdvanceCommitIndex(s) /\ s # t) => ~CanServeConsistentReads(t)

LeaderAppendOnly ==
    [][\A s \in Server : state[s] = Leader => Len(log'[s]) >= Len(log[s])]_vars

\* <<index, term>> pairs uniquely identify log prefixes
LogMatching ==
    \A s,t \in Server :
    \A i \in DOMAIN log[s] :
        (\E j \in DOMAIN log[t] : i = j /\ log[s][i] = log[t][j]) =>
        (SubSeq(log[s],1,i) = SubSeq(log[t],1,i)) \* prefixes must be the same

\* A node elected as Leader contains all entries committed in previous terms
LeaderCompleteness ==
    \A s \in Server : (state[s] = Leader) =>
        \A c \in committed : (c.term < currentTerm[s] => InLog(c, s))

\* If two entries are committed at the same index, they must be the same entry
StateMachineSafety ==
    \A c1, c2 \in committed : (c1.index = c2.index) => (c1 = c2)

\* Linearizability of reads
LinearizableReads ==
    latestRead = IF Cardinality(committed) = 0 THEN CreateEntry(0, 0)
                 ELSE MaxCommitted(committed)

=============================================================================
