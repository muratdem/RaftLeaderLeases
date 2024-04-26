import csv
import itertools
import logging
import os.path
import time

from omegaconf import DictConfig

from client import ClientLogEntry
from run_raft_with_params import main_coro
from simulate import get_event_loop

_logger = logging.getLogger("experiment")


def save_metrics(metrics: dict, client_log: list[ClientLogEntry]):
    writes, reads = 0, 0
    write_time, read_time = 0, 0
    for entry in client_log:
        if entry.op_type == ClientLogEntry.OpType.ListAppend:
            writes += 1
            write_time += entry.duration
        elif entry.op_type == ClientLogEntry.OpType.Read:
            reads += 1
            read_time += entry.duration
        else:
            assert False, "unknown op type"

    metrics["write_latency"] = write_time / writes if writes else None
    metrics["read_latency"] = read_time / reads if reads else None


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


def main():
    logging.basicConfig(level=logging.INFO)
    event_loop = get_event_loop()
    csv_writer: None | csv.DictWriter = None
    csv_path = f"metrics/{os.path.splitext(os.path.basename(__file__))[0]}.csv"
    csv_file = open(csv_path, "w+")
    raw_params = DictConfig({
        # All times in microseconds.
        "max_clock_error": 0.00003,  # From Huygens paper.
        "election_timeout": 10 * 1000 * 1000,  # 10 seconds.
        "one_way_latency_mean": list(range(100, 1001, 100)), # TODO: more
        "one_way_latency_variance": 100,
        "noop_rate": 1 * 1000 * 1000,
        "heartbeat_rate": 4 * 1000 * 1000,
        "operations": 50,
        "interarrival": 50 * 1000,
        "stepdown_rate": None,
        "partition_rate": None,
        "heal_rate": None,
        "keyspace_size": 5,
        "leases_enabled": [False, True],
        "lease_timeout": 10 * 1000 * 1000,
        "check_linearizability": False,
        "metrics_start_ts": 1 * 1000 * 1000,
        "seed": 1,
    })

    for params in all_param_combos(raw_params):
        start = time.monotonic()
        metrics = {}
        event_loop.create_task(
            "main", main_coro(params=params, metrics=metrics, jumpstart_election=True))
        event_loop.run()
        _logger.info(f"metrics: {metrics}")
        stats = metrics | dict(params)
        stats["real_duration"] = time.monotonic() - start
        if csv_writer is None:
            csv_writer = csv.DictWriter(csv_file, fieldnames=stats.keys())
            csv_writer.writeheader()

        csv_writer.writerow(stats)
        event_loop.reset()

    csv_file.close()
    _logger.info(csv_path)


if __name__ == "__main__":
    main()
