import copy
import csv
import enum
import itertools
import logging
import time
from collections import defaultdict
from dataclasses import dataclass

import pandas as pd
import plotly.graph_objects as go
import yaml
from omegaconf import DictConfig
from plotly.subplots import make_subplots

from lease_raft import Network, Node, ReadConcern, Role, setup_logging
from prob import PRNG
from simulate import Timestamp, get_current_ts, get_event_loop, sleep

_logger = logging.getLogger("run")


@dataclass
class ClientLogEntry:
    class OpType(enum.Enum):
        ListAppend = enum.auto()
        Read = enum.auto()

    client_id: int
    op_type: OpType
    start_ts: Timestamp
    """The absolute time when the client sent the request."""
    absolute_ts: Timestamp
    """The absolute time when the event occurred. (We're omniscient, we know this.)"""
    end_ts: Timestamp
    """The absolute time when the client received reply."""
    key: int
    value: int | list[int]
    success: bool

    @property
    def duration(self) -> int:
        assert self.end_ts >= self.start_ts
        return self.end_ts - self.start_ts

    def __str__(self) -> str:
        if self.op_type is ClientLogEntry.OpType.ListAppend:
            return (f"{self.start_ts} -> {self.absolute_ts} -> {self.end_ts}:"
                    f" write key {self.key}={self.value}"
                    f" ({'ok' if self.success else 'failed'})")

        return (f"{self.start_ts} -> {self.absolute_ts} -> {self.end_ts}:"
                f" read key {self.key}={self.value}")


async def reader(
    client_id: int,
    start_ts: Timestamp,
    nodes: list[Node],
    client_log: list[ClientLogEntry],
    prng: PRNG,
):
    await sleep(start_ts)
    # Attempt to read from any node.
    node = prng.choice(nodes)
    key = prng.random_key()
    _logger.info(f"Client {client_id} reading key {key} from {node}")
    try:
        reply = await node.read(key=key, concern=ReadConcern.MAJORITY)
        latency = get_current_ts() - start_ts
        _logger.info(f"Client {client_id} read key {key}={reply.value} from {node},"
                     f" latency={latency}")
        client_log.append(
            ClientLogEntry(
                client_id=client_id,
                op_type=ClientLogEntry.OpType.Read,
                start_ts=start_ts,
                absolute_ts=reply.absolute_ts,
                end_ts=get_current_ts(),
                key=key,
                value=reply.value,
                success=True
            )
        )
    except Exception as e:
        _logger.error(f"Client {client_id} failed reading key {key} from {node}: {e}")


async def writer(
    client_id: int,
    start_ts: Timestamp,
    nodes: list[Node],
    client_log: list[ClientLogEntry],
    prng: PRNG,
):
    await sleep(start_ts)
    # Attempt to write to any node.
    node = prng.choice(nodes)
    key = prng.random_key()
    _logger.info(f"Appending key {key}+={client_id} to {node}")
    try:
        absolute_ts = await node.write(key=key, value=client_id)
        latency = get_current_ts() - start_ts
        _logger.info(f"Appended key {key}+={client_id}, latency={latency}")
        client_log.append(
            ClientLogEntry(
                client_id=client_id,
                op_type=ClientLogEntry.OpType.ListAppend,
                start_ts=start_ts,
                absolute_ts=absolute_ts,
                end_ts=get_current_ts(),
                key=key,
                value=client_id,
                success=True
            )
        )
    except Exception as e:
        _logger.error(f"Failed appending key {key}+={client_id} on {node}: {e}")
        if str(e) != "Not primary":
            # Add it to the log, some failed writes commit eventually.
            client_log.append(
                ClientLogEntry(
                    client_id=client_id,
                    op_type=ClientLogEntry.OpType.ListAppend,
                    start_ts=start_ts,
                    absolute_ts=get_current_ts(),
                    end_ts=get_current_ts(),
                    key=key,
                    value=client_id,
                    success=False
                )
            )


async def stepdown_nemesis(nodes: dict[int, Node],
                           prng: PRNG,
                           stepdown_rate: int):
    while True:
        await sleep(round(prng.exponential(stepdown_rate)))
        primaries = [n for n in nodes.values() if n.role is Role.PRIMARY]
        if not primaries:
            continue

        primary = prng.choice(primaries)
        _logger.info(f"Stepping down {primary}")
        primary.stepdown()


async def partition_nemesis(network: Network,
                            prng: PRNG,
                            partition_rate: int,
                            heal_rate: int):
    while True:
        await sleep(round(prng.exponential(partition_rate)))
        network.make_random_partition()
        await sleep(round(prng.exponential(heal_rate)))
        network.reset_partition()


def chart_metrics(raw_params: dict, csv_path: str):
    df = pd.read_csv(csv_path)
    y_columns = [
        "read_latency",
        "write_latency",
    ]

    fig = make_subplots(rows=1, cols=1, shared_xaxes=True, vertical_spacing=0.1)
    fig.update_xaxes(title_text="one-way network latency", row=1, col=1)

    # TODO: read latency vs replication lag for rc:local and rc:linearizable
    for column in y_columns:
        fig.add_trace(
            go.Scatter(
                x=df["replication_lag"], y=df[column], mode="lines+markers", name=column
            ),
            row=1,
            col=1,
        )

    fig.update_layout(
        hovermode="x unified",
        title=", ".join(f"{k}={v}" for k, v in raw_params.items()),
    )

    # Draw a vertical line on both charts on mouseover, and don't truncate column names.
    fig.update_traces(hoverlabel={"namelength": -1}, xaxis="x1")
    chart_path = "metrics/chart.html"
    fig.write_html(chart_path)
    _logger.info(f"Created {chart_path}")


def do_linearizability_check(client_log: list[ClientLogEntry]) -> None:
    """Throw exception if "client_log" is not linearizable."""
    # We're omniscient, we know the absolute time each event occurred, so we don't need
    # a costly checking algorithm. Just sort by the absolute times.
    sorted_log = sorted(client_log, key=lambda e: e.absolute_ts)
    _logger.info("Checking linearizability. Log:")
    for entry in sorted_log:
        _logger.info(entry)

    def linearize(
        log: list[ClientLogEntry], model: defaultdict[int, list[int]]
    ) -> list[ClientLogEntry] | None:
        """Try linearizing a suffix of the log with the KV store "model" in some state.

        Return a linearization if possible, else None.
        """
        if len(log) == 0:
            return log  # Empty history is already linearized.

        # If there are simultaneous events, try ordering any linearization of them.
        first_entries = [e for e in log if e.absolute_ts == log[0].absolute_ts]
        for i, entry in enumerate(first_entries):
            # Try linearizing "entry" at history's start. No other entry's end can
            # precede this entry's start.
            log_prime = log.copy()
            log_prime.pop(i)

            # Assume a failed write has no effect, model' == model.
            if entry.op_type is ClientLogEntry.OpType.ListAppend and not entry.success:
                linearization = linearize(log_prime, model)
                if linearization is not None:
                    # Omit entry entirely from the linearization.
                    return linearization

            # Assume the write succeeded. Even if !success it might eventually commit.
            if entry.op_type is ClientLogEntry.OpType.ListAppend:
                model_prime = copy.deepcopy(model)
                model_prime[entry.key].append(entry.value)
                # Try to linearize the rest of the log with the KV store in this state.
                linearization = linearize(log_prime, model_prime)
                if linearization is not None:
                    return [entry] + linearization

            if entry.op_type is ClientLogEntry.OpType.Read:
                # What would this query return if we ran it now?
                if model[entry.key] != entry.value:
                    continue  # "entry" can't be linearized first.

                # Try to linearize the rest of the log with the KV store in this state.
                linearization = linearize(log_prime, model)
                if linearization is not None:
                    return [entry] + linearization

        return None

    check_start = time.monotonic()
    result = linearize(sorted_log, defaultdict(list))
    check_duration = time.monotonic() - check_start
    if result is None:
        _logger.info(f"Failed to linearize {len(client_log)} entries after"
                     f" {check_duration:.2f} sec")
        raise Exception("not linearizable!")

    _logger.info(
        f"Linearization of {len(client_log)} entries took {check_duration:.2f} sec:")
    for x in result:
        _logger.info(x)


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


async def main_coro(params: DictConfig, metrics: dict):
    _logger.info(params)
    prng = PRNG(cfg=params)
    _logger.info(f"Seed {prng.seed}")
    network = Network(prng=prng, node_ids=[1, 2, 3])
    nodes = {
        1: Node(node_id=1, cfg=params, prng=prng, network=network),
        2: Node(node_id=2, cfg=params, prng=prng, network=network),
        3: Node(node_id=3, cfg=params, prng=prng, network=network),
    }

    setup_logging(nodes)
    for n in nodes.values():
        n.initiate(nodes)

    while not any(n.role == Role.PRIMARY for n in nodes.values()):
        await sleep(100)

    lp = get_event_loop()
    client_log: list[ClientLogEntry] = []
    tasks = []
    start_ts = 0
    # Schedule some tasks with Poisson start times. Each does one read or one write.
    for i in range(params.operations):
        start_ts += round(prng.exponential(params.interarrival))
        if prng.randint(0, 1) == 0:
            coro = writer(
                client_id=i,
                start_ts=start_ts,
                nodes=list(nodes.values()),
                client_log=client_log,
                prng=prng,
            )
        else:
            coro = reader(
                client_id=i,
                start_ts=start_ts,
                nodes=list(nodes.values()),
                client_log=client_log,
                prng=prng,
            )

        tasks.append(lp.create_task(name=f"client {i}", coro=coro))

    lp.create_task(
        name="nemesis",
        coro=stepdown_nemesis(nodes=nodes, prng=prng,
                              stepdown_rate=params.stepdown_rate)
    ).ignore_future()

    lp.create_task(
        name="nemesis",
        coro=partition_nemesis(network=network, prng=prng,
                               partition_rate=params.partition_rate,
                               heal_rate=params.heal_rate)
    ).ignore_future()

    for t in tasks:
        await t

    lp.stop()
    _logger.info(f"Finished after {get_current_ts()} ms (simulated)")
    save_metrics(metrics, client_log)

    def avg_metric(name: str) -> float | None:
        total = sum(s.metrics.total(name) for s in nodes.values())
        sample_count = sum(s.metrics.sample_count(name) for s in nodes.values())
        if sample_count > 0:
            return total / sample_count

    metrics["replication_lag"] = avg_metric("replication_lag")
    metrics["commit_lag"] = avg_metric("commit_lag")
    metrics["commit_latency"] = avg_metric("commit_latency")
    if params.check_linearizability:
        do_linearizability_check(client_log)


def all_param_combos(raw_params: dict) -> list[DictConfig]:
    param_combos: dict[str, list] = {}
    # Load config file, keep all values as strings.
    for k, v in raw_params.items():
        v_interpreted = eval(str(v))
        try:
            iter(v_interpreted)
        except TypeError:
            param_combos[k] = [v_interpreted]
        else:
            param_combos[k] = list(v_interpreted)

    for values in itertools.product(*param_combos.values()):
        yield DictConfig(dict(zip(param_combos.keys(), values)))


def main():
    logging.basicConfig(level=logging.INFO)
    event_loop = get_event_loop()
    csv_writer: None | csv.DictWriter = None
    csv_path = "metrics/metrics.csv"
    csv_file = open(csv_path, "w+")
    raw_params = yaml.safe_load(open("params.yaml"))
    for params in all_param_combos(raw_params):
        metrics = {}
        event_loop.create_task("main", main_coro(params=params, metrics=metrics))
        event_loop.run()
        _logger.info(f"metrics: {metrics}")
        stats = metrics | dict(params)
        if csv_writer is None:
            csv_writer = csv.DictWriter(csv_file, fieldnames=stats.keys())
            csv_writer.writeheader()

        csv_writer.writerow(stats)
        event_loop.reset()

    csv_file.close()
    chart_metrics(raw_params=raw_params, csv_path=csv_path)


if __name__ == "__main__":
    main()
