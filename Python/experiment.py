"""Utilities for running experiments."""
import itertools

from omegaconf import DictConfig


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
