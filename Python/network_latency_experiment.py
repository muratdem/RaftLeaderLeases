import csv
import logging
import os.path
import time

from client import ClientLogEntry
from params import BASE_PARAMS
from run_with_params import do_linearizability_check, main_coro
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


def main():
    logging.basicConfig(level=logging.INFO)
    event_loop = get_event_loop()
    csv_writer: None | csv.DictWriter = None
    csv_path = f"metrics/{os.path.splitext(os.path.basename(__file__))[0]}.csv"
    csv_file = open(csv_path, "w+")
    raw_params = BASE_PARAMS.copy()
    # Same params as unavailability_experiment.py.
    raw_params.update({
        "operations": 10 * 1000,
        "keyspace_size": 1000,
        "interarrival": 300,
        "log_write_micros": 250,  # SSD I/O latency.
        "seed": 1,
    })

    for latency in range(100, 501, 100):
        for lease_enabled in [True, False]:
            params = raw_params.copy()
            params.update({
                "one_way_latency_mean": latency,
                "one_way_latency_variance": 2 * latency,
                "lease_enabled": lease_enabled,
            })

            start = time.monotonic()
            metrics = event_loop.run_until_complete(event_loop.create_task(
                "main", main_coro(params=params, jumpstart_election=True)))
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
