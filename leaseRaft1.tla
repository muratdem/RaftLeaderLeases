---- MODULE leaseRaft1 ----
\*
\* Basic, static version of MongoDB Raft protocol.
\*

EXTENDS Naturals, Integers, FiniteSets, Sequences, TLC

CONSTANTS Server
CONSTANTS Secondary, Primary, Nil, Delta

VARIABLE currentTerm
VARIABLE state
VARIABLE log
VARIABLE committed
VARIABLE clock
VARIABLE lease
VARIABLE latestRead

vars == <<currentTerm, state, log, committed, clock, lease, latestRead>>

\*
\* Helper operators.
\*

\* Is a sequence empty.
Empty(s) == Len(s) = 0
Max(S) == CHOOSE i \in S: (\A j \in S: i>=j) 

MaxCommitted(S) == IF Cardinality(S) = 0 THEN [entry |-> <<0,0>>]
                   ELSE CHOOSE i \in S: (\A j \in S: i.entry[1]>=j.entry[1]) 


\* Is log entry e = <<index, term>> in the log of node 'i'.
InLog(e, i) == \E x \in DOMAIN log[i] : x = e[1] /\ log[i][x] = e[2]

\* The term of the last entry in a log, or 0 if the log is empty.
LastTerm(xlog) == IF Len(xlog) = 0 THEN 0 ELSE xlog[Len(xlog)][1]
LastEntry(xlog) == <<Len(xlog),xlog[Len(xlog)]>>
GetTerm(xlog, index) == IF index = 0 THEN 0 ELSE xlog[index]
LogTerm(i, index) == GetTerm(log[i], index)

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

\* Is a log entry 'e'=<<i, t>> immediately committed in term 't' with a quorum 'Q'.
ImmediatelyCommitted(e, Q) == 
    LET eind == e[1] 
        eterm == e[2][1] IN
    \A s \in Q :
        /\ Len(log[s]) >= eind
        /\ InLog(e, s) \* they have the entry.
        /\ currentTerm[s] = eterm  \* they are in the same term as the log entry. 

\* Helper operator for actions that propagate the term between two nodes.
UpdateTermsExpr(i, j) ==
    /\ currentTerm[i] > currentTerm[j]
    /\ currentTerm' = [currentTerm EXCEPT ![j] = currentTerm[i]]
    /\ state' = [state EXCEPT ![j] = Secondary] 

--------------------------------------------------------------------------------

\*
\* Next state actions.
\*

\* Node 'i', a primary, handles a new client request and places the entry 
\* in its log.    
ClientWrite(i) ==
    /\ state[i] = Primary
    /\ (   (lease[i][1]= currentTerm[i] /\ lease[i][2] >= clock) \* lease belongs to me and unexpired
        \/ (lease[i][1]# currentTerm[i] /\ lease[i][2] < clock) \* lease belongs to prev lead and expired
        )
    /\ clock' = clock + 1     
    /\ log' = [log EXCEPT ![i] = Append(log[i], <<currentTerm[i],clock'>>)]
    /\ UNCHANGED <<currentTerm, state, committed, lease, latestRead>>

ClientRead(i) ==
    /\ state[i] = Primary
\*  /\ lease[i][1] = currentTerm[i] \* new leader can serve read on old leader's lease!!
    /\ lease[i][2] >= clock
    /\ LET cInd == MaxCommitted(committed).entry[1] 
           l == Len(log[i]) IN
        /\ latestRead' = IF cInd = 0 THEN <<0,0>>
                         ELSE log[i][cInd] \* Raft guarantees cInd <= l
    /\ UNCHANGED <<currentTerm, state, log, committed, clock, lease>>

\* Node 'i' gets a new log entry from node 'j'.
GetEntries(i, j) ==
    /\ state[i] = Secondary
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
              newLog        == Append(log[i], newEntry) IN
              /\ log' = [log EXCEPT ![i] = newLog]
              /\ lease' = [lease EXCEPT ![i] =  
                            IF lease[i][1]<= newEntry[1] 
                            THEN <<newEntry[1], newEntry[2]+Delta>>
                            ELSE lease[i]]
    /\ UNCHANGED <<committed, currentTerm, state, clock, latestRead>>

\*  Node 'i' rolls back against the log of node 'j'.  
RollbackEntries(i, j) ==
    /\ state[i] = Secondary
    /\ CanRollback(i, j)
    \* Roll back one log entry.
    /\ log' = [log EXCEPT ![i] = SubSeq(log[i], 1, Len(log[i])-1)]
    /\ UNCHANGED <<committed, currentTerm, state, clock, lease, latestRead>>

\* Node 'i' gets elected as a primary.
BecomeLeader(i, voteQuorum) == 
    LET newTerm == currentTerm[i] + 1 IN
    /\ i \in voteQuorum \* The new leader should vote for itself.
    /\ \A v \in voteQuorum : CanVoteForOplog(v, i, newTerm)
    \* Update the terms of each voter.
    /\ currentTerm' = [s \in Server |-> IF s \in voteQuorum THEN newTerm ELSE currentTerm[s]]
    /\ state' = [s \in Server |->
                    IF s = i THEN Primary
                    ELSE IF s \in voteQuorum THEN Secondary \* All voters should revert to secondary state.
                    ELSE state[s]]
    /\ UNCHANGED <<log, committed, latestRead, lease, clock>>   
            
\* Primary 'i' commits its latest log entry.
CommitEntry(i, commitQuorum) ==
    LET ind == Len(log[i]) IN
    \* Must have some entries to commit.
    /\ ind > 0
    \* This node is leader.
    /\ state[i] = Primary
    \* The entry was written by this leader.
    /\ log[i][ind][1] = currentTerm[i]
    \* all nodes have this log entry and are in the term of the leader.
    /\ ImmediatelyCommitted(<<ind,log[i][ind]>>, commitQuorum)
    \* Don't mark an entry as committed more than once.
    /\ ~\E c \in committed : c.entry = <<ind, log[i][ind]>>
    /\ committed' = committed \cup
            {[ entry  |-> <<ind, log[i][ind]>>,
               term  |-> currentTerm[i]]}
    /\ latestRead' = log[i][ind]         
    /\ lease' = [lease EXCEPT ![i] = <<currentTerm[i], log[i][ind][2]+Delta>>]
    /\ UNCHANGED <<currentTerm, state, log, clock>>

\* Action that exchanges terms between two nodes and step down the primary if
\* needed. This can be safely specified as a separate action, rather than
\* occurring atomically on other replication actions that involve communication
\* between two nodes. This makes it clearer to see where term propagation is
\* strictly necessary for guaranteeing safety.
UpdateTerms(i, j) == 
    /\ UpdateTermsExpr(i, j)
    /\ UNCHANGED <<log, committed, lease, latestRead, clock>>

\* Action for incrementing the clock
Tick == 
    /\ clock' = clock + 1
    /\ UNCHANGED <<currentTerm, state, log, committed, lease, latestRead>>

Init == 
    /\ currentTerm = [i \in Server |-> 0]
    /\ state       = [i \in Server |-> Secondary]
    /\ log = [i \in Server |-> <<>>]
    /\ committed = {}
    /\ clock = 0
    /\ lease = [i \in Server |-> <<0,0>>]  
    /\ latestRead = <<0,0>>

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

OnePrimaryPerTerm == 
    \A s,t \in Server :
        (/\ state[s] = Primary 
         /\ state[t] = Primary
         /\ currentTerm[s] = currentTerm[t]) => (s = t)

LeaderAppendOnly == 
    [][\A s \in Server : state[s] = Primary => Len(log'[s]) >= Len(log[s])]_vars

\* <<index, term>> pairs uniquely identify log prefixes.
LogMatching == 
    \A s,t \in Server : 
    \A i \in DOMAIN log[s] :
        (\E j \in DOMAIN log[t] : i = j /\ log[s][i] = log[t][j]) => 
        (SubSeq(log[s],1,i) = SubSeq(log[t],1,i)) \* prefixes must be the same.

\* When a node gets elected as primary it contains all entries committed in previous terms.
LeaderCompleteness == 
    \A s \in Server : (state[s] = Primary) => 
        \A c \in committed : (c.term < currentTerm[s] => InLog(c.entry, s))

\* If two entries are committed at the same index, they must be the same entry.
StateMachineSafety == 
    \A c1, c2 \in committed : (c1.entry[1] = c2.entry[1]) => (c1 = c2)

\* Linearizability of reads
LinearizableReads == 
    latestRead = IF Cardinality(committed) = 0 THEN <<0,0>>
                 ELSE MaxCommitted(committed).entry[2]
 
=============================================================================
