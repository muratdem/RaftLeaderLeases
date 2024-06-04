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

This TLA-WEB trace illustrates the problem where client read from n3 with inherited-lease violates linearizability at Step 9.
https://will62794.github.io/tla-web/#!/home?specpath=https%3A%2F%2Fraw.githubusercontent.com%2Fmuratdem%2FRaftLeaderLeases%2Fe8c3b52930dce31a1381d94442d1b7e15fde8ecb%2FTLA%2FleaseRaft1.tla&constants%5BServer%5D=%7Bn1%2Cn2%2Cn3%7D&constants%5BFollower%5D=%22Follower%22&constants%5BLeader%5D=%22Leader%22&constants%5BNil%5D=%22Nil%22&constants%5BDelta%5D=3&trace=76a3a1d3%2C2fb4bd02_753de38b%2C52fdceac_0ac545d4%2C6dbb861f_3c189024%2C6ef02bf6_f95a2828%2Cc477c3f2_c1630918%2Ca1a2243c_43ce9fa7%2Ca1a2243c_05be568a%2C3fbd98ec_85e7bffe&traceExprs%5B0%5D=LinearizableReads

The fix for this is to treat the limbo region reads specially at j. It is Ok for j to serve reads/queries whose results are unchanged by the writes in the limbo region. So j should refrain from responding to queries whose results are changed by the updates in the limbo region, and serve all the other queries. This enables both i and j to serve linearizable queries.

Here is the trace for the fixed specification using the no-limbo-read patch. Note that the new leader n2 can serve read for k1 using inherited read lease optimization because k1 is clean. But n2 is not enabled to serve read for k2 because it is a limbo-update.
https://will62794.github.io/tla-web/#!/home?specpath=https%3A%2F%2Fraw.githubusercontent.com%2Fmuratdem%2FRaftLeaderLeases%2F663a7f9267777b78f29b183500c92c1d12d5bbf9%2FTLA%2FleaseRaft1.tla&constants%5BServer%5D=%7Bn1%2Cn2%2Cn3%7D&constants%5BKey%5D=%7Bk1%2Ck2%7D&constants%5BDelta%5D=3&constants%5BFollower%5D=%22Follower%22&constants%5BLeader%5D=%22Leader%22&constants%5BNil%5D=%22Nil%22&trace=7ec544bd%2C345fb419_41d2c9ba%2C7004891e_48ce95fe%2C51be74df_9b5827e4%2C3a88c2b6_df63a60a%2C45eed75a_08d770a2%2C9522b94c_30da8751%2C34c2c5f0_7ca1092f%2C08828acb_7697da86&traceExprs%5B0%5D=LinearizableReads