# LeaseGuard

Mechanism for Raft leader leases, and linearizable reads from leader.

TLA+ folder includes the TLA+ models for Raft and LeaseGuard.

Python folder includes a simulator for the same protocol as the TLA+ spec.

* guess_network_latency_distribution.py: standalone script to determine the probability distribution of network latency.
* lease_guard.py: implementation of Raft with LeaseGuard.
* lease_guard_test.py: unittests for lease_guard.py.
* run_with_params.py: simulate Raft and LeaseGuard with reads, writes, and partitions.
* params.py: parameters for run_with_params.py.
* unavailability_experiment.py: simulate a leader failure and recovery, create unavailability_experiment.csv.
* network_latency_experiment.py: test effect of latency on read/write speed, create network_latency_experiment.csv.

## Inherited lease reads
A problem with inherited lease reads is that the leader i commits first, and secondaries commitIndex lag i's commitIndex. When a secondary j becomes a leader, it should be careful about serving inherited lease reads if its commitIndex[j] is less than Len(log[j]). If that is the case, we call the log suffix betweeen commitIndex[j] and Len(log[j]) as the limbo region. It is possible that commitIndex[i]=Len(log[j]) because i observed majority replication of this item and i has acknowledged that item at Len(log[j]). But j cannot know about this and rely on this to move commitIndex[j] to Len(log[j]). Because the other case is also possible, and if j serves read at Len(log[j)], this violates linearizability for the client if i also serves reads banking on its lease. 

The fix for this is to treat the limbo region reads specially at j. It is Ok for j to serve reads/queries whose results are unchanged by the writes in the limbo region. So j should refrain from responding to queries whose results are changed by the updates in the limbo region, and serve all the other queries. This enables both i and j to serve linearizable queries.
