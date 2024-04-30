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

Python TODO:
* Check read-your-writes, not just linearizability
* Nodes restart and must do a clean sync
* Separate the scripts that make CSVs from the one that makes charts
* Compare linearizable read perf with noop write vs lease vs "prior lease" optimization, x-axis is network latency
* Compare linearizable noop rate with today's impl vs on-demand lease extension
* Compare time to first committed write after unexpected election with & w/o lease, holding election timeout constant?
  * If ET > LT, there's a period after leader death with no leader and no leaseholder, no writes OR reads!
  * If ET < LT, new leader has to wait for old leader's lease to expire before writing
  * ET + majority commit time = LT?
  * Simpler: ET == LT
  * See Raft paper Fig 16
* Compare old-style linearizable read to rc:majority and rc:local latency
* Effect of clock skew on write unavailability? effect on rate of inconsistencies?
* For the paper: white-box linearizability checking, using absolute timestamps, made possible by simulation
  * Murat has seen this in a recent paper?
