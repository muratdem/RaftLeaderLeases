# RaftLeaderLeases
Mechanism for Raft Leader Leases, and linearizable reads from leader.

TLA+ folder includes the TLA+ models for Raft Leader Leases.

Python folder includes a simulator for the same protocol as the TLA+ spec.

Python TODO:
* Check read-your-writes, not just linearizability
* Nodes restart and must do a clean sync
