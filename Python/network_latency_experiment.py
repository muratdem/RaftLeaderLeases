import csv
import logging
import os.path
import time

from params import BASE_PARAMS
from run_with_params import main_coro
from simulate import get_event_loop

_logger = logging.getLogger("experiment")


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

    for leaseguard_enabled, quorum_check, title in [
        (False, True, "quorum"),
        (False, False, "inconsistent"),
        (True, False, "lease"),
    ]:
        for latency_ms in range(1, 11):
            params = raw_params.copy()
            params.update({
                "one_way_latency_mean": 1000 * latency_ms, # Convert ms to micros.
                "one_way_latency_variance": 1000 * latency_ms,
                "quorum_check_enabled": quorum_check,
                "leaseguard_enabled": leaseguard_enabled,
                "inherit_lease_enabled": False,  # Irrelevant.
                "defer_commit_enabled": False,  # Irrelevant.
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
