# RaftLeaderLeases
Mechanism for Raft Leader Leases, and linearizable reads from leader.

TLA+ folder includes the TLA+ models for Raft Leader Leases.

Python folder includes a simulator for the same protocol as the TLA+ spec.

* guess_network_latency_distribution.py: standalone script to determine the probability distribution of network latency.
* lease_raft.py: implementation of Raft with leader leases.
* lease_raft_test.py: unittests for lease_raft.py.
* run_raft_with_params.py: simulate Raft with reads, writes, and partitions
* params.py: parameters for run_raft_with_params.py.
* experiments.py: create performance charts for the research paper.
