"""Utilities for probability distributions."""

import numpy as np
from functools import lru_cache
from scipy.optimize import fsolve


class PRNG:
    def __init__(
        self,
        seed: int,
        one_way_latency_mean: float,
        one_way_latency_variance: float,
        keyspace_size: int,
    ):
        self._random_state = np.random.RandomState(seed % 2**32)
        self._one_way_latency_mean = one_way_latency_mean
        self._one_way_latency_variance = one_way_latency_variance
        self._keyspace_size = keyspace_size

    def randint(self, low_inclusive: int, high_inclusive: int) -> int:
        # NumPy's rand_int excludes the high value, make it inclusive.
        return self._random_state.randint(low_inclusive, high_inclusive + 1)

    def exponential(self, scale: float) -> float:
        return self._random_state.exponential(scale)

    def one_way_latency_value(self):
        mu, sigma = _lognormal_params(
            self._one_way_latency_mean, self._one_way_latency_variance
        )
        return self._random_state.lognormal(mu, sigma)

    def random_key(self):
        return self._random_state.randint(0, self._keyspace_size)


@lru_cache
def _lognormal_params(mean: float, variance: float) -> tuple[float, float]:
    """Get lognormal distribution's mu and sigma for a desired mean and variance."""

    def func(sigma):
        mu = np.log(mean) - sigma**2 / 2
        calculated_variance = (np.exp(sigma**2) - 1) * np.exp(2 * mu + sigma**2)
        return calculated_variance - variance

    sigma_solution = fsolve(func, 1.0)[0]
    mu_solution = np.log(mean) - (sigma_solution**2) / 2
    return float(mu_solution), float(sigma_solution)
