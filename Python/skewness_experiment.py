import csv
import logging
import os.path

import numpy as np

from params import BASE_PARAMS
from prob import PRNG

_logger = logging.getLogger("experiment")

PARAMS = BASE_PARAMS.copy()
PARAMS.update({
    "keyspace_size": 1000,
    "limbo_region_size": 100,
    "operations": 1000,
    "interarrival": 300,  # microseconds
})

MAX_THROUGHPUT_PER_SEC = 1e6 / PARAMS.interarrival

SUB_EXPERIMENT_PARAMS = []
for zipf_skewness in np.arange(1.05, 2.0, 0.05):
    sub_exp_params = PARAMS.copy()
    sub_exp_params["zipf_skewness"] = float(zipf_skewness)
    SUB_EXPERIMENT_PARAMS.append(sub_exp_params)


def main():
    logging.basicConfig(level=logging.INFO)
    csv_writer: None | csv.DictWriter = None
    csv_path = f"metrics/{os.path.splitext(os.path.basename(__file__))[0]}.csv"
    csv_file = open(csv_path, "w+")

    for sub_exp_params in SUB_EXPERIMENT_PARAMS:
        logging.info(f"Skewness {sub_exp_params.zipf_skewness}")
        n_samples = 100
        successes = 0
        for i in range(n_samples):
            sub_sub_exp_params = sub_exp_params.copy()
            sub_sub_exp_params.update({"seed": i})
            prng = PRNG(sub_sub_exp_params)
            # The new primary has limbo_region_size log entries between the last commit
            # index it learned and the last prior-term entry it replicated. To which
            # keys did these entries write? Higher skewness->more dupes, fewer keys.
            limbo_keys = set(
                prng.random_key() for _ in range(sub_exp_params.limbo_region_size))
            # The new primary attempts to read before it gets a write lease. How many of
            # these reads are blocked?
            successes += sum(
                0 if prng.random_key() in limbo_keys else 1
                for _ in range(sub_exp_params.operations))

        success_ratio = successes / float(sub_exp_params.operations * n_samples)
        stats = {
            "zipf_skewness": sub_exp_params.zipf_skewness,
            "seed": sub_exp_params.seed,
            "success_ratio": success_ratio,
            "throughput_per_sec": MAX_THROUGHPUT_PER_SEC * success_ratio,
        }

        if csv_writer is None:
            csv_writer = csv.DictWriter(csv_file, fieldnames=stats.keys())
            csv_writer.writeheader()

        csv_writer.writerow(stats)

    csv_file.close()
    _logger.info(csv_path)


if __name__ == "__main__":
    main()
