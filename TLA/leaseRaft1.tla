---- MODULE leaseRaft1 ----
\* LeaseGuard protocol with inherited lease read & deferred commit writes
\* This spec uses perfectly synchronized clocks

EXTENDS Naturals, Integers, FiniteSets, Sequences, TLC

CONSTANTS Server, Key, Delta
CONSTANTS Follower, Leader, Nil

VARIABLE currentTerm
VARIABLE state
VARIABLE log
VARIABLE committed
VARIABLE commitIndex
VARIABLE clock
VARIABLE lease
VARIABLE latestRead

Entry == [term: Int, clock: Int, key: Key, index: Int]
Lease == [term: Int, expires: Int]
TypeOK == 
    /\ currentTerm \in [Server -> Int]
    /\ state \in [Server -> {Leader, Follower}]
    /\ log \in [Server -> Seq(Entry)]
    /\ committed \in SUBSET Entry
    /\ commitIndex \in [Server -> Int]
    /\ clock \in Int
    /\ lease \in [Server -> Lease]
    /\ latestRead \in [Key -> Entry]

vars == <<currentTerm, state, log, committed, commitIndex, clock, lease, latestRead>>

\*
\* Helper operators.
\*

\* Is a sequence empty.
Empty(s) == Len(s) = 0
Max(S) == CHOOSE i \in S: (\A j \in S: i>=j) 
Min(S) == CHOOSE i \in S: (\A j \in S: i=<j) 


CreateEntry(xterm, xclock, xkey, xindex) == [term |-> xterm, clock |-> xclock, key|-> xkey, index |-> xindex]
CreateLease(xterm, xclock) == [term |-> xterm, expires |-> xclock]

FilterKey(S,k) == {e \in S: e.key=k}
MaxCommitted(S, k) == IF Cardinality(FilterKey(S,k)) = 0 THEN CreateEntry(0, 0, k, 0)
                      ELSE CHOOSE i \in FilterKey(S,k): (\A j \in FilterKey(S,k): i.index >= j.index) 


\* Is log entry e in the log of node 'i'.
InLog(e, i) == log[i][e.index] = e

\* Find latest committed entry for key in log of i
LastCommitted(k, i) == 
    IF commitIndex[i] = 0 THEN 0
    ELSE 
        \* Raft guarantees commitIndex[i] <= Len(log[i])
        IF ~(\E j \in 1..commitIndex[i]: log[i][j].key=k)
            THEN 0
            ELSE CHOOSE j \in 1..commitIndex[i]:
                /\ log[i][j].key=k 
                /\ \A l \in 1..commitIndex[i]: log[i][l].key=k => j>=l

\* Find latest entry for key in log of i before i's current term
LastInPriorTerm(k, i) == 
    IF ~(\E j \in 1..Len(log[i]): log[i][j].term<currentTerm[i] /\ log[i][j].key=k)
    THEN 0
    ELSE CHOOSE j \in 1..Len(log[i]):
        /\ log[i][j].key=k
        /\ log[i][j].term<currentTerm[i]
        /\ \A l \in 1..Len(log[i]):
            (log[i][l].key=k /\ log[i][l].term<currentTerm[i]) => j>=l

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

--------------------------------------------------------------------------------

\*
\* Next state actions.
\*

\* Node 'i', a Leader, handles a new client request and places the entry
\* in its log. Due to deferred commit writes, leader doesn't check lease to accept clientWrite
ClientWrite(i, k) ==
    /\ state[i] = Leader
    /\ clock' = clock + 1     
    /\ log' = [log EXCEPT ![i] = Append(log[i], CreateEntry(currentTerm[i], clock', k, Len(log[i]) + 1))]
    /\ UNCHANGED <<currentTerm, state, committed, commitIndex, lease, latestRead>>

\* This may only set latestRead to an earlier value than what is committed, 
\* and that would be caught by LinearizableReads invariant
ClientRead(i, k) ==
    /\ state[i] = Leader
    \*  /\ lease[i].term = currentTerm[i] \* new leader can serve read on old leader's lease!!
    /\ lease[i].expires >= clock
    \* limbo-read guarding for inherited lease
    /\ currentTerm[i] # lease[i].term =>
        LastCommitted(k, i) = LastInPriorTerm(k, i)
    /\ LET cInd == LastCommitted(k, i) IN
        /\ latestRead' = [latestRead EXCEPT ![k] = 
                            IF cInd = 0 THEN CreateEntry(0, 0, k, 0)
                            ELSE log[i][cInd] ] \* Raft guarantees cInd <= Len(log[i])
    /\ UNCHANGED <<currentTerm, state, log, committed, commitIndex, clock, lease>>

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
       /\ LET newEntryIndex == Len(log[i]) + 1
              newEntry      == log[j][newEntryIndex]
              newLog        == Append(log[i], newEntry) IN
              /\ log' = [log EXCEPT ![i] = newLog]
              /\ lease' = [lease EXCEPT ![i] =  
                            IF lease[i].term <= newEntry.term
                            THEN CreateLease(newEntry.term, newEntry.clock+Delta)
                            ELSE lease[i]]
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
    /\ UNCHANGED <<committed, commitIndex, currentTerm, state, clock, lease, latestRead>>

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
    /\ UNCHANGED <<log, committed, commitIndex, latestRead, lease, clock>>   
            
\* Leader 'i' commits its latest log entry.
CommitEntry(i, commitQuorum) ==
    \* Must have some entries to commit.
    /\ commitIndex[i] < Len(log[i]) 
    \* This node is leader.
    /\ state[i] = Leader
    /\ LET ind == commitIndex[i]+1
           entry == log[i][ind] IN
        \* The entry was written by this leader.
        /\ entry.term = currentTerm[i]
        \* all nodes have this log entry and are in the term of the leader.
        /\ ImmediatelyCommitted(entry, commitQuorum)
        \* If prev leader lease still in session, can't advance commitIndex! (deferred commit write)
        /\ (lease[i].expires < clock \/ lease[i].term = currentTerm[i])
        \* Don't mark an entry as committed more than once.
        /\ entry \notin committed
        /\ committed' = committed \cup {entry}
        /\ commitIndex' = [commitIndex EXCEPT ![i] = ind]
        /\ latestRead' = [latestRead EXCEPT ![entry.key] = entry]         
        /\ lease' = [lease EXCEPT ![i] = CreateLease(entry.term, entry.clock+Delta)]
        /\ UNCHANGED <<currentTerm, state, log, clock>>

\* Action that exchanges terms between two nodes and step down the Leader if
\* needed. This can be safely specified as a separate action, rather than
\* occurring atomically on other replication actions that involve communication
\* between two nodes. This makes it clearer to see where term propagation is
\* strictly necessary for guaranteeing safety.
UpdateTerms(i, j) == 
    /\ UpdateTermsExpr(i, j)
    /\ UNCHANGED <<log, committed, commitIndex, lease, latestRead, clock>>

\* Action for incrementing the clock
Tick == 
    /\ clock' = clock + 1
    /\ UNCHANGED <<currentTerm, state, log, committed, commitIndex, lease, latestRead>>

Init == 
    /\ currentTerm = [i \in Server |-> 0]
    /\ state       = [i \in Server |-> Follower]
    /\ log = [i \in Server |-> <<>>]
    /\ committed = {}
    /\ commitIndex = [i \in Server |-> 0]
    /\ clock = 0
    /\ lease = [i \in Server |-> CreateLease(-1, -1)]  
    /\ latestRead = [k \in Key |-> CreateEntry(0, 0, k, 0)]

Next == 
    \/ \E s \in Server : \E k \in Key : ClientWrite(s,k)
    \/ \E s \in Server : \E k \in Key : ClientRead(s,k)
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

OneLeaderPerTerm == 
    \A s,t \in Server :
        (/\ state[s] = Leader 
         /\ state[t] = Leader
         /\ currentTerm[s] = currentTerm[t]) => (s = t)

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

\* Linearizability of reads, checking equality of latestRead map to the latest committed/acked write values
LinearizableReads == 
    latestRead = [ k \in Key |-> MaxCommitted(committed,k) ]
 
=============================================================================
