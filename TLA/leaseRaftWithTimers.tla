---- MODULE leaseRaftWithTimers ----
\* Raft with timer-based leases, no need for synchronized clocks. Includes
\* deferred commit writes, but not inherited lease reads (the latter needs
\* synced clocks.) Follows MongoDB, not Raft, where they differ.
\* TODO: epsilon
EXTENDS Naturals, Integers, FiniteSets, Sequences, TLC
CONSTANTS Server, Key, Delta, Follower, Leader
VARIABLE currentTerm, state, log, replicationTimes, matchIndex, commitIndex
VARIABLE clock
\* For invariant-checking:
VARIABLE committed, latestRead

Entry == [term: Int, key: Key, index: Int]
TypeOK ==
    /\ currentTerm \in [Server -> Int]
    /\ state \in [Server -> {Leader, Follower}]
    /\ log \in [Server -> Seq(Entry)]
    /\ replicationTimes \in [Server -> Seq(Int)]
    /\ matchIndex \in [Server -> [Server -> Int]]
    /\ committed \in SUBSET Entry
    /\ clock \in Int
    /\ commitIndex \in [Server -> Int]
    /\ latestRead \in [Key -> Entry]

vars == <<currentTerm, state, log, replicationTimes, matchIndex, commitIndex, 
  clock, committed, latestRead>>

Empty(s) == Len(s) = 0
Max(S) == CHOOSE i \in S: (\A j \in S: i>=j)
Min(S) == CHOOSE i \in S: (\A j \in S: i=<j) 
Range(f) == {f[x]: x \in DOMAIN f}
CreateEntry(xterm, xkey, xindex) == [
  term |-> xterm, key |-> xkey, index |-> xindex]
FilterKey(S,k) == {e \in S: e.key=k}
MaxCommitted(S, k) == IF Cardinality(FilterKey(S,k)) = 0
  THEN CreateEntry(0, k, 0)
  ELSE CHOOSE i \in FilterKey(S,k):
  (\A j \in FilterKey(S,k): i.index >= j.index)

\* Is log entry e in the log of node 'i'.
InLog(e, i) == log[i][e.index] = e

\* Find latest committed entry for key in log of i
LastCommitted(k, i) == 
  IF commitIndex[i] = 0 THEN 0
  ELSE 
    IF ~(\E j \in 1..commitIndex[i]: Len(log[i])>= j /\ log[i][j].key=k)
      THEN 0
      ELSE CHOOSE j \in 1..commitIndex[i]:
        /\ log[i][j].key=k 
        /\ \A l \in 1..commitIndex[i]: log[i][l].key=k => j>=l

\* The term of the last entry in a log, or 0 if the log is empty.
LastTerm(xlog) ==
  IF Len(xlog) = 0 THEN 0 ELSE xlog[Len(xlog)].term

\* The set of all quorums in a given set.
Quorums(S) ==
    {i \in SUBSET(S) : Cardinality(i) * 2 > Cardinality(S)}

IsPrefix(s, t) ==
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
  
\* Term of last committed entry in log of server 'i', or 0.
TermOfLastCommittedEntry(i) ==
  IF (Len(log[i]) = 0 \/ commitIndex[i] = 0) THEN 0
  ELSE log[i][Min({Len(log[i]), commitIndex[i]})].term

--------------------------------------------------------------------------------
\* Next state actions.

\* Node i adds a write to key k to its log. Doesn't need lease,
\* due to deferred commit writes.
ClientWrite(i, k) ==
  /\ state[i] = Leader
  /\ log' = [log EXCEPT ![i] = Append(
    log[i], CreateEntry(currentTerm[i], k, Len(log[i]) + 1))]
  /\ replicationTimes' = [
    replicationTimes EXCEPT ![i] = Append(replicationTimes[i], clock)]
  /\ matchIndex' = [
    matchIndex EXCEPT ![i] = [
        s \in Server |-> IF s=i THEN Len(log[i])+1 ELSE matchIndex[i][s]]]
  /\ UNCHANGED <<currentTerm, state, committed, commitIndex,
                 latestRead, clock>>

\* Node i reads key k.
ClientRead(i, k) ==
  /\ state[i] = Leader
  /\ Len(log[i]) > 0
  /\ commitIndex[i] > 0
  \* SERVER-53813 (not fixed in MongoDB yet, but assume it is)
  /\ TermOfLastCommittedEntry(i) = currentTerm[i]
  /\ replicationTimes[i][commitIndex[i]] + Delta >= clock
  /\ LET cInd == LastCommitted(k, i) IN
    /\ latestRead' = [latestRead EXCEPT ![k] = 
              IF cInd = 0 THEN CreateEntry(0, k, 0)
              ELSE log[i][cInd]]
  /\ UNCHANGED <<currentTerm, state, log, replicationTimes, matchIndex,
                 committed, commitIndex, clock>>

\* Node 'i' gets a new log entry from node 'j'.
\* In Raft, j's term >= i's term. MongoDB doesn't require this, see
\* "Fault-Tolerant Replication with Pull-Based Consensus in MongoDB" Section 3.3.
GetEntries(i, j) ==
  /\ state[i] = Follower
  /\ Len(log[j]) > Len(log[i])
  /\ currentTerm' = [
    currentTerm EXCEPT ![i] = Max({currentTerm[i], currentTerm[j]})]
     \* Ensure that the entry at the last index of node i's log must match the entry at
     \* the same index in node j's log. If the log of node i is empty, then the check
     \* trivially passes. This is the essential 'log consistency check'.
  /\ LET logOk == IF Empty(log[i])
            THEN TRUE
            ELSE log[j][Len(log[i])] = log[i][Len(log[i])] IN
     /\ logOk \* log consistency check
     /\ LET newEntryIndex == IF Empty(log[i]) THEN 1 ELSE Len(log[i]) + 1
            newEntry == log[j][newEntryIndex]
            newLog == Append(log[i], newEntry)
            newReplTimes == Append(replicationTimes[i], clock) IN
        /\ log' = [log EXCEPT ![i] = newLog]
        /\ replicationTimes' = [replicationTimes EXCEPT ![i] = newReplTimes]
        \* Update source's matchIndex immediately & reliably (unrealistic).
        /\ matchIndex' = [
            matchIndex EXCEPT ![j] = [
              s \in Server |-> IF s=i THEN Len(log[i])+1 ELSE matchIndex[j][s]]]
        \* Raft clamps commitIndex to log length, MongoDB doesn't.
        /\ commitIndex' = [
            commitIndex EXCEPT ![i] = Max({commitIndex[i], commitIndex[j]})]
  /\ UNCHANGED <<committed, state, clock, latestRead>>

\*  Node 'i' rolls back against the log of node 'j'.
RollbackEntries(i, j) ==
  /\ state[i] = Follower
  /\ CanRollback(i, j)
  \* Roll back one log entry.
  /\ log' = [log EXCEPT ![i] = SubSeq(log[i], 1, Len(log[i])-1)]
  /\ replicationTimes' = [replicationTimes EXCEPT ![i] = SubSeq(
    replicationTimes[i], 1, Len(log[i])-1)]
  /\ matchIndex' = [matchIndex EXCEPT ![i] = [
    s \in Server |-> IF s=i THEN Len(log[i])-1 ELSE matchIndex[i][s]]]
  /\ currentTerm' = [currentTerm EXCEPT ![i] = Max(
    {currentTerm[i], currentTerm[j]})]
  /\ UNCHANGED <<committed, commitIndex, state, clock,
                 latestRead>>

\* Node 'i' gets elected as a Leader.
BecomeLeader(i, voteQuorum) ==
  LET t == currentTerm[i] + 1 IN
  /\ i \in voteQuorum \* Votes for itself.
  /\ \A v \in voteQuorum : CanVoteForOplog(v, i, t)
  \* Update the terms of each voter.
  /\ currentTerm' = [s \in Server |->
    IF s \in voteQuorum THEN t ELSE currentTerm[s]]
  \* Reset my matchIndex.
  /\ matchIndex' = [matchIndex EXCEPT ![i] = [j \in Server |-> 0]]
   \* All voters become followers.
  /\ state' = [s \in Server |->
          IF s = i THEN Leader
          ELSE IF s \in voteQuorum THEN Follower
          ELSE state[s]]
  /\ UNCHANGED <<committed, log, replicationTimes, commitIndex, latestRead,
                 clock>>

\* Leader 'i' commits its latest log entry.
CommitEntry(i) ==
  /\ state[i] = Leader
  /\ Len(log[i]) > 0
  \* Last entry replicated from prior leader was at least Delta ago.
  /\ \A index \in DOMAIN log[i] : 
    log[i][index].term # currentTerm[i] => (
      replicationTimes[i][index] + Delta < clock)
  \* Must have some entries to commit.
  /\ commitIndex[i] < Len(log[i]) 
  /\ LET ind == commitIndex[i]+1
       entry == log[i][ind] IN
    \* The entry was written by this leader.
    /\ entry.term = currentTerm[i]
    \* Most nodes have this log entry. MongoDB checks most nodes have our term,
    \* not Raft. We check other nodes' terms directly (unrealistic).
    /\ \E q \in Quorums(Server) :
      \A s \in q : matchIndex[i][s] >= ind /\ currentTerm[s] = currentTerm[i]
    \* Don't mark an entry as committed more than once.
    /\ entry \notin committed
    /\ committed' = committed \cup {entry}
    /\ commitIndex' = [commitIndex EXCEPT ![i] = ind]
    /\ latestRead' = [latestRead EXCEPT ![entry.key] = entry]     
  /\ UNCHANGED <<currentTerm, replicationTimes, matchIndex, state, log, clock>>

\* Exchanges terms between two nodes and step down the Leader if needed.
UpdateTerms(i, j) ==
  /\ currentTerm[i] > currentTerm[j]
  /\ currentTerm' = [
    currentTerm EXCEPT ![j] = currentTerm[i]]
  /\ state' = [state EXCEPT ![j] = Follower]
  /\ UNCHANGED <<log, replicationTimes, matchIndex, committed, commitIndex,
                 latestRead, clock>>

\* Node 'i' learns the commitIndex of node 'j'.
UpdateCommitIndex(i, j) == 
  /\ state[i] = Follower
  /\ state[j] = Leader
  /\ commitIndex[i] < commitIndex[j]
  /\ commitIndex' = [commitIndex EXCEPT ![i] = commitIndex[j]]
  /\ UNCHANGED <<state, log, replicationTimes, matchIndex, committed,
                 currentTerm, latestRead, clock>>
  
\* Action for incrementing the clock
Tick ==
  /\ clock' = clock + 1
  /\ UNCHANGED <<currentTerm, state, log, replicationTimes, matchIndex,
                 committed, commitIndex, latestRead>>

Init ==
  /\ currentTerm = [i \in Server |-> 0]
  /\ state = [i \in Server |-> Follower]
  /\ log = [i \in Server |-> <<>>]
  /\ replicationTimes = [i \in Server |-> <<>>]
  /\ matchIndex = [i \in Server |-> [j \in Server |-> 0]]
  /\ committed = {}
  /\ commitIndex = [i \in Server |-> 0]
  /\ clock = 0
  /\ latestRead = [k \in Key |-> CreateEntry(0, k, 0)]

Next ==
  \/ \E s \in Server : \E k \in Key : ClientWrite(s,k)
  \/ \E s \in Server : \E k \in Key : ClientRead(s,k)
  \/ \E s, t \in Server : GetEntries(s, t)
  \/ \E s, t \in Server : RollbackEntries(s, t)
  \/ \E s \in Server : \E Q \in Quorums(Server) : BecomeLeader(s, Q)
  \/ \E s \in Server : CommitEntry(s)
  \/ \E s,t \in Server : UpdateTerms(s, t)
  \/ \E s,t \in Server : UpdateCommitIndex(s, t)
  \/ Tick

Spec == Init /\ [][Next]_vars
--------------------------------------------------------------------------------
\* Correctness properties
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

\* <<index, term>> pairs uniquely identify log prefixes
LogMatching ==
  \A s,t \in Server :
  \A i \in DOMAIN log[s] :
    (\E j \in DOMAIN log[t] :
      i = j /\ log[s][i] = log[t][j]) =>
        \* prefixes must be the same
        (SubSeq(log[s],1,i) = SubSeq(log[t],1,i))

\* A Leader has all entries committed in prior terms
LeaderCompleteness ==
  \A s \in Server : (state[s] = Leader) =>
    \A c \in committed : (
      c.term < currentTerm[s] => InLog(c, s))

\* Two entries committed at same index are same entry.
StateMachineSafety ==
  \A c1, c2 \in committed : (c1.index = c2.index) => (c1 = c2)

\* For all k, latestRead for k is last committed write to k.
LinearizableReads == 
  latestRead = [ k \in Key |-> MaxCommitted(committed,k) ]
=============================================================================
