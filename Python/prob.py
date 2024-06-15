"""Utilities for probability distributions."""

import time
from functools import lru_cache

import numpy as np
from omegaconf import DictConfig


class PRNG:
    def __init__(self, cfg: DictConfig):
        self.seed = int(time.monotonic_ns() if cfg.seed is None else cfg.seed)
        self._random_state = np.random.RandomState(self.seed % 2 ** 32)
        self._one_way_latency_mean = cfg.one_way_latency_mean
        self._one_way_latency_variance = cfg.one_way_latency_variance
        self._keyspace_size = cfg.keyspace_size
        self._zipf_skewness = cfg.zipf_skewness

        # Generate Zipfian distribution ranks
        ranks = np.arange(1, self._keyspace_size + 1)
        log_probabilities = -self._zipf_skewness * np.log(ranks)
        probabilities = np.exp(log_probabilities)
        probabilities /= probabilities.sum()  # Normalize so probabilities sum to 1.
        self._zipf_probabilities = probabilities
        self._keys = np.arange(1, self._keyspace_size + 1)

    def randint(self, low_inclusive: int, high_inclusive: int) -> int:
        # NumPy's rand_int excludes the high value, make it inclusive.
        return self._random_state.randint(low_inclusive, high_inclusive + 1)

    def uniform(self, low: float, high: float) -> float:
        return self._random_state.uniform(low, high)

    def choice(self, choices: list):
        return self._random_state.choice(choices)

    def exponential(self, scale: float) -> float:
        return self._random_state.exponential(scale)

    def one_way_latency_value(self) -> int:
        mu, sigma = _lognormal_params(
            self._one_way_latency_mean, self._one_way_latency_variance
        )
        return int(self._random_state.lognormal(mu, sigma))

    def random_key(self) -> int:
        return self._random_state.choice(self._keys, p=self._zipf_probabilities)


@lru_cache
def _lognormal_params(mean: float, variance: float) -> tuple[float, float]:
    """Get lognormal distribution's mu and sigma for a desired mean and variance."""
    sigma_squared = np.log(variance / mean**2 + 1)
    mu = np.log(mean) - sigma_squared / 2
    return mu, np.sqrt(sigma_squared)
