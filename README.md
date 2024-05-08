# LeaseGuard

Mechanism for Raft leader leases, and linearizable reads from leader.

TLA+ folder includes the TLA+ models for Raft and LeaseGuard.

Python folder includes a simulator for the same protocol as the TLA+ spec.

* guess_network_latency_distribution.py: standalone script to determine the probability distribution of network latency.
* lease_guard.py: implementation of Raft with LeaseGuard.
* lease_guard_test.py: unittests for lease_guard.py.
* run_with_params.py: simulate Raft and LeaseGuard with reads, writes, and partitions.
* params.py: parameters for run_raft_with_params.py.
* unavailability_experiment.py: simulate a leader failure and recovery, create unavailability_experiment.csv.
* network_latency_experiment.py: test effect of latency on read/write speed, create network_latency_experiment.csv.
