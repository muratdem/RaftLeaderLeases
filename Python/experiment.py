"""Utilities for running experiments."""
import itertools

from omegaconf import DictConfig

BASE_PARAMS = DictConfig({
    # All times in microseconds.
    "max_clock_error": 0.00003,  # From Huygens paper.
    "election_timeout": 10 * 1000 * 1000,  # 10 seconds.
    "one_way_latency_mean": 250,
    "one_way_latency_variance": 100,
    "noop_rate": 1 * 1000 * 1000,
    "heartbeat_rate": 4 * 1000 * 1000,
    "operations": 50,
    "interarrival": 50 * 1000,
    "stepdown_rate": None,
    "partition_rate": None,
    "heal_rate": None,
    "keyspace_size": 5,
    "leases_enabled": True,
    "read_lease_optimization_enabled": True,
    "lease_timeout": 10 * 1000 * 1000,
    "check_linearizability": False,
    "metrics_start_ts": 1 * 1000 * 1000,
    "seed": 1,
})


def all_param_combos(params: DictConfig) -> list[DictConfig]:
    param_combos: dict[str, list] = {}
    for k, v in params.items():
        try:
            iter(v)  # Is it a sequence?
        except TypeError:
            param_combos[k] = [v]
        else:
            param_combos[k] = list(v)

    for values in itertools.product(*param_combos.values()):
        yield DictConfig(dict(zip(param_combos.keys(), values)))
