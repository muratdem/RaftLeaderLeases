import csv
import logging
import os.path

import numpy as np

from params import BASE_PARAMS
from prob import PRNG

_logger = logging.getLogger("experiment")

PARAMS = BASE_PARAMS.copy()
PARAMS.update({"keyspace_size": 1000})

LIMBO_REGION_SIZE = 100  # log entries after new leader's commit index
INTERARRIVAL = 300  # microseconds
LIMBO_PERIOD = 500 * 1000  # microseconds
MAX_THROUGHPUT_PER_SEC = 1e6 / INTERARRIVAL

SUB_EXPERIMENT_PARAMS = []
for zipf_skewness in np.arange(0, 2.1, 0.1):
    sub_exp_params = PARAMS.copy()
    sub_exp_params["zipf_skewness"] = float(zipf_skewness)
    SUB_EXPERIMENT_PARAMS.append(sub_exp_params)


def compute_expected_blocked_reads(skew: float,
                                   writes: int,
                                   reads: int,
                                   keyspace_size: int) -> float:
    # "Generalized harmonic number", https://en.wikipedia.org/wiki/Zipf%27s_law.
    ghn = sum(1.0 / i ** skew for i in range(1, keyspace_size + 1))
    ranks = np.arange(1, keyspace_size + 1)
    # p_k_in_writes[k] is P(the key of rank k is in the write set).
    p_k_in_writes = 1 - (1 - 1 / (ranks ** skew * ghn)) ** writes
    # e_k_reads[k] is E[number of reads for the key of rank k].
    e_k_reads = reads / (ranks ** skew * ghn)
    # For each k, E[number of blocked reads] = P(in write set) * E[reads]. For expected
    # total blocked reads, sum over k.
    return float(np.sum(p_k_in_writes * e_k_reads))


def main():
    logging.basicConfig(level=logging.INFO)
    csv_writer: None | csv.DictWriter = None
    csv_path = f"metrics/{os.path.splitext(os.path.basename(__file__))[0]}.csv"
    csv_file = open(csv_path, "w+")

    for sub_exp_params in SUB_EXPERIMENT_PARAMS:
        # The new primary has limbo_region_size log entries between the last commit
        # index it learned and the last prior-term entry it replicated. It does
        # "ops" number of reads before it commits an entry in its term.
        # How many of these read operations are in the write set, and thus blocked?
        ops = round(LIMBO_PERIOD / INTERARRIVAL)
        expected_blocked = compute_expected_blocked_reads(
            skew=sub_exp_params.zipf_skewness,
            writes=LIMBO_REGION_SIZE,
            reads=ops,
            keyspace_size=sub_exp_params.keyspace_size)

        expected_success_ratio = (ops - expected_blocked) / float(ops)
        n_trials = 500
        blocked_limbo_reads = 0
        for i in range(n_trials):
            trial_params = sub_exp_params.copy()
            trial_params["seed"] = i
            prng = PRNG(trial_params)
            w_keys = set(prng.random_key() for _ in range(LIMBO_REGION_SIZE))
            for _ in range(ops):
                if prng.random_key() in w_keys:
                    blocked_limbo_reads += 1

        experimental_success_ratio = (
            (n_trials * ops - blocked_limbo_reads)
            / float(n_trials * ops))

        stats = {
            "zipf_skewness":
                sub_exp_params.zipf_skewness,
            "expected_success_ratio":
                expected_success_ratio,
            "expected_throughput_per_sec":
                MAX_THROUGHPUT_PER_SEC * expected_success_ratio,
            "experimental_success_ratio":
                experimental_success_ratio,
            "experimental_throughput_per_sec":
                MAX_THROUGHPUT_PER_SEC * experimental_success_ratio,
            "max_possible_throughput_per_sec":
                MAX_THROUGHPUT_PER_SEC,
        }

        _logger.info(stats)

        if csv_writer is None:
            csv_writer = csv.DictWriter(csv_file, fieldnames=stats.keys())
            csv_writer.writeheader()

        csv_writer.writerow(stats)

    csv_file.close()
    _logger.info(csv_path)


if __name__ == "__main__":
    main()
