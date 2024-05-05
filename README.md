# LeaseGuard

Mechanism for Raft leader leases, and linearizable reads from leader.

TLA+ folder includes the TLA+ models for Raft and LeaseGuard.

Python folder includes a simulator for the same protocol as the TLA+ spec.

* guess_network_latency_distribution.py: standalone script to determine the probability distribution of network latency.
* lease_guard.py: implementation of Raft with LeaseGuard.
* lease_guard_test.py: unittests for lease_guard.py.
* run_raft_with_params.py: simulate Raft and LeaseGuard with reads, writes, and partitions.
* params.py: parameters for run_raft_with_params.py.
* experiments.py: create performance charts for the research paper.
