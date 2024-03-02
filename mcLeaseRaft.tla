---- MODULE mcLeaseRaft ----
EXTENDS TLC, leaseRaft1

BaitInv == TLCGet("level") < 99
====