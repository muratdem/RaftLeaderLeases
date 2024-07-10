from omegaconf import DictConfig

BASE_PARAMS = DictConfig({
    # All times in microseconds.
    "max_clock_error": 15 * 1000,  # Very inaccurate clock.
    "election_timeout": 500 * 1000,  # 1/2 second, high to avoid flapping.
    # Time for message to go from one replica set node to another.
    "one_way_latency_mean": 191,
    # Variance for one-way latency, which is lognormal distributed.
    "one_way_latency_variance": 391,
    "noop_rate": 250 * 1000,  # How long between no-op writes.
    "heartbeat_rate": 500 * 1000,  # How long between heartbeat messages.
    "operations": 50,  # How many reads + writes to simulate.
    "interarrival": 100 * 1000,
    # Mean time between operations, which are Poisson-distributed.
    "stepdown_rate": None,
    # Mean time between stepdowns, which are Poisson-distributed.
    "partition_rate": None,
    # Mean time between partitions, which are Poisson-distributed.
    "heal_rate": None,  # Mean time to heal a partition, exponentially distributed.
    "keyspace_size": 5,  # Number of keys. Reads/writes choose keys at random.
    "zipf_skewness": 1,  # Zipfian distro "a" param, read/write access skewness.
    "lease_enabled": True,
    "inherit_lease_enabled": True,
    "defer_commit_enabled": True,
    "lease_timeout": 500 * 1000,  # Half a second.
    "log_write_micros": None,  # Optional, min microseconds between oplog writes.
    "check_linearizability": True,  # Whether to check if the history was linearizable.
    "metrics_start_ts": 1 * 1000 * 1000,
    "jumpstart_election": True,  # Don't wait for timeout at simulation start.
    "seed": None,  # Optional random seed for reproducibility.
})
