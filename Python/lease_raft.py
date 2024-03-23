import collections
import csv
import enum
import itertools
import logging
import statistics
import time
from dataclasses import dataclass, field

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import yaml
from omegaconf import DictConfig

from prob import PRNG
from simulate import Timestamp, get_current_ts, get_event_loop, initiate_logging, sleep

logger = logging.getLogger("lease-raft")


class Role(enum.Enum):
    PRIMARY = enum.auto()
    SECONDARY = enum.auto()


@dataclass(order=True)
class OpTime:
    """A hybrid logical clock (HLC)."""

    ts: Timestamp
    i: int

    @classmethod
    def default(cls) -> "OpTime":
        return OpTime(-1, -1)


@dataclass(order=True)
class Write:
    key: str = field(compare=False)
    value: str = field(compare=False)
    optime: OpTime


class Metrics:
    def __init__(self):
        self._totals = collections.Counter()
        self._sample_counts = collections.Counter()

    def update(self, metric_name: str, sample: int) -> None:
        self._totals[metric_name] += sample
        self._sample_counts[metric_name] += 1

    def total(self, metric_name: str) -> int:
        return self._totals[metric_name]

    def sample_count(self, metric_name: str) -> int:
        return self._sample_counts[metric_name]

    def mean(self, metric_name: str) -> float | None:
        if self._sample_counts[metric_name] > 0:
            return self._totals[metric_name] / self._sample_counts[metric_name]


class Node:
    def __init__(self, index: int, role: Role, cfg: DictConfig, prng: PRNG):
        self.index = index
        self.role = role
        self.prng = prng
        # Map key to (value, last-written time).
        self.data: dict[str, tuple[str, OpTime]] = {}
        self.log: list[Write] = []
        self.committed_optime: OpTime = OpTime.default()
        self.last_applied_entry: Write | None = None
        self.nodes: list["Node"] | None = None
        # Map Node ids to their last-replicated timestamps.
        self.node_replication_positions: dict[int, OpTime] = {}
        self.noop_rate: int = cfg.noop_rate
        self.metrics = Metrics()

    def initiate(self, nodes: list["Node"]):
        self.nodes = nodes[:]
        self.node_replication_positions = {id(n): OpTime.default() for n in nodes}
        get_event_loop().create_task("no-op writer", self.noop_writer())
        get_event_loop().create_task("replication", self.replicate())

    @property
    def last_applied(self) -> OpTime:
        return self.log[-1].optime if self.log else OpTime.default()

    async def noop_writer(self):
        while True:
            await sleep(self.noop_rate)
            if self.role is Role.PRIMARY:
                self._write_internal("noop", "")

    async def replicate(self):
        log_position = 0
        while True:
            await sleep(1)
            if self.role is Role.PRIMARY:
                continue

            primaries = [n for n in self.nodes if n.role is Role.PRIMARY]
            if not primaries:
                continue

            # Simplification to avoid terms: sync from primary w/ longest log.
            # Imagine we could see all primaries' log lengths at once.
            primary = sorted(primaries, key=lambda n: len(n.log), reverse=True)[0]
            while log_position < len(primary.log):
                # Find the next entry to replicate.
                entry = primary.log[log_position]
                log_position += 1

                # Simulate waiting for entry to arrive. It may have arrived already.
                apply_time = entry.optime.ts + self.prng.one_way_latency_value()
                await sleep(max(0, apply_time - get_current_ts()))
                self.data[entry.key] = (entry.value, entry.optime)
                self.log.append(entry)
                self.node_replication_positions[id(self)] = entry.optime
                get_event_loop().call_later(
                    self.prng.one_way_latency_value(),
                    primary.update_secondary_position,
                    secondary=self,
                    optime=entry.optime,
                ).ignore_future()

                self.metrics.update(
                    "replication_lag", get_current_ts() - entry.optime.ts
                )

    def update_secondary_position(self, secondary: "Node", optime: OpTime):
        if self.role is not Role.PRIMARY:
            return

        # Handle out-of-order messages with max(), assume no rollbacks.
        self.node_replication_positions[id(secondary)] = max(
            self.node_replication_positions[id(secondary)], optime
        )
        self.committed_optime = statistics.median(
            self.node_replication_positions.values()
        )

        for n in self.nodes:
            if n is not self:
                get_event_loop().call_later(
                    self.prng.one_way_latency_value(),
                    n.update_committed_optime,
                    self.committed_optime,
                )

    def update_committed_optime(self, optime: OpTime):
        if optime > self.committed_optime:
            self.committed_optime = optime
            self.metrics.update("commit_lag", get_current_ts() - optime.ts)

    def _write_internal(self, key: str, value: str) -> OpTime:
        """Update a key and append an oplog entry."""
        if self.role is not Role.PRIMARY:
            raise Exception("Not primary")

        optime = OpTime(get_current_ts(), 0)
        if len(self.log) > 0 and self.log[-1].optime.ts == optime.ts:
            optime.i = self.log[-1].optime.i + 1

        w = Write(key=key, value=value, optime=optime)
        self.data[w.key] = (value, w.optime)
        self.log.append(w)
        self.node_replication_positions[id(self)] = optime
        return optime

    async def write(self, key: str, value: str):
        """Update a key, append an oplog entry, wait for w:majority."""
        optime = self._write_internal(key=key, value=value)
        while self.committed_optime < optime:
            await sleep(1)
            if self.role is not Role.PRIMARY:
                raise Exception("Stepped down while waiting for w:majority")

        commit_latency = get_current_ts() - optime.ts
        self.metrics.update("commit_latency", commit_latency)

    async def read(self, key: str) -> str | None:
        """Return a key's latest value."""
        # We're not testing any consistency guarantees for secondary reads in this
        # simulation, so assume all queries have readPreference: "primary".
        if self.role is not Role.PRIMARY:
            raise Exception("Not primary")

        # TODO: implement MVCC and read at committed ts. Remove last_written_optime.
        value, last_written_optime = self.data.get(key, (None, OpTime.default()))
        return value

    async def stepdown(self):
        """Tell this node to become secondary."""
        self.role = Role.SECONDARY

    def __str__(self) -> str:
        return f"Node {self.index} {self.role.name}"


@dataclass
class ClientLogEntry:
    class OpType(enum.Enum):
        Write = enum.auto()
        Read = enum.auto()

    client_id: int
    op_type: OpType
    server_role: Role
    start_ts: Timestamp
    end_ts: Timestamp
    key: str
    value: str | None
    success: bool

    @property
    def duration(self) -> int:
        assert self.end_ts >= self.start_ts
        return self.end_ts - self.start_ts

    def __str__(self) -> str:
        if self.op_type is ClientLogEntry.OpType.Write:
            return (f"{self.start_ts} -> {self.end_ts}:"
                    f" write key {self.key}={self.value}"
                    f" ({'ok' if self.success else 'failed'})")

        return f"{self.start_ts} -> {self.end_ts}: read key {self.key}={self.value}"


def next_value(_next=[-1]) -> str:
    _next[0] += 1
    return str(_next[0])


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
    key = str(prng.random_key())
    logger.info(f"Client {client_id} reading key {key} from {node}")
    try:
        value = await node.read(key=key)
        latency = get_current_ts() - start_ts
        logger.info(
            f"Client {client_id} read key {key}={value} from {node}, latency={latency}"
        )
        client_log.append(
            ClientLogEntry(
                client_id=client_id,
                op_type=ClientLogEntry.OpType.Read,
                server_role=node.role,
                start_ts=start_ts,
                end_ts=get_current_ts(),
                key=key,
                value=value,
                success=True
            )
        )
    except Exception as e:
        logger.error(
            f"Client {client_id} failed reading key {key} from {node}: {e}"
        )


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
    key = str(prng.random_key())
    value = next_value()
    logger.info(f"Client {client_id} writing key {key}={value} to {node}")
    try:
        await node.write(key=key, value=value)
        latency = get_current_ts() - start_ts
        logger.info(f"Client {client_id} wrote key {key}={value}, latency={latency}")
        client_log.append(
            ClientLogEntry(
                client_id=client_id,
                op_type=ClientLogEntry.OpType.Write,
                server_role=Role.PRIMARY,
                start_ts=start_ts,
                end_ts=get_current_ts(),
                key=key,
                value=value,
                success=True
            )
        )
    except Exception as e:
        logger.error(
            f"Client {client_id} failed writing key {key}={value} to {node}: {e}")
        client_log.append(
            ClientLogEntry(
                client_id=client_id,
                op_type=ClientLogEntry.OpType.Write,
                server_role=Role.PRIMARY,
                start_ts=start_ts,
                end_ts=get_current_ts(),
                key=key,
                value=value,
                success=False
            )
        )


async def nemesis(nodes: list[Node],
                  prng: PRNG,
                  stepdown_rate: int):
    while True:
        await sleep(round(prng.exponential(stepdown_rate)))
        node_index = prng.randint(0, len(nodes) - 1)
        node = (nodes)[node_index]
        logging.info(f"Stepping down {node}")
        await node.stepdown()


def do_linearizability_check(client_log: list[ClientLogEntry]) -> None:
    """Throw exception if "client_log" is not linearizable.

    Based on Lowe, "Testing for Linearizability", 2016, which summarizes Wing & Gong,
    "Testing and Verifying Concurrent Objects", 1993. Don't do Lowe's memoization trick.
    """

    logging.info("Checking linearizability. Log:")
    for entry in sorted(client_log, key=lambda e: e.start_ts):
        logging.info(entry)

    def linearize(
        log: list[ClientLogEntry], model: dict
    ) -> list[ClientLogEntry] | None:
        """Try linearizing a suffix of the log with the KV store "model" in some state.

        Return a linearization if possible, else None.
        """
        if len(log) == 0:
            return log  # Empty history is already linearized.

        for i, entry in enumerate(log):
            # Try linearizing "entry" at history's start. No other entry's end can
            # precede this entry's start.
            if any(e for e in log if e is not entry and e.end_ts < entry.start_ts):
                # "entry" can't be linearized first, because e finished earlier.
                continue

            log_prime = log.copy()
            log_prime.pop(i)

            if entry.op_type is ClientLogEntry.OpType.Write:
                # What would the KV store contain if we linearize this write here?
                # Note, the write could commit even if !entry.success, e.g. the primary
                # stepped down awaiting majority acknowledgment.
                model_prime = model.copy()
                model_prime[entry.key] = entry.value
                # Try to linearize the rest of the log with the KV store in this state.
                linearization = linearize(log_prime, model_prime)
                if linearization is not None:
                    return [entry] + linearization

            if entry.op_type is ClientLogEntry.OpType.Write and not entry.success:
                # Above, we tried to linearize, assuming the write eventually succeeded,
                # but that didn't work. Now assume the write had no effect.
                linearization = linearize(log_prime, model)
                if linearization is not None:
                    return [entry] + linearization

            if entry.op_type is ClientLogEntry.OpType.Read:
                # What would this query return if we ran it now?
                if model.get(entry.key) != entry.value:
                    continue  # "entry" can't be linearized first.

                # Try to linearize the rest of the log with the KV store in this state.
                linearization = linearize(log_prime, model)
                if linearization is not None:
                    return [entry] + linearization

        return None

    logger.info("Checking linearizability....")
    check_start = time.monotonic()
    # Sort by start_ts to make the search succeed sooner.
    result = linearize(sorted(client_log, key=lambda y: y.start_ts), {})
    check_duration = time.monotonic() - check_start
    if result is None:
        raise Exception("not linearizable!")

    logger.info(
        f"Linearization of {len(client_log)} entries took {check_duration:.2f} sec:"
    )
    for x in result:
        logger.info(x)


def save_metrics(metrics: dict, client_log: list[ClientLogEntry]):
    writes, reads = 0, 0
    write_time, read_time = 0, 0
    for entry in client_log:
        if entry.op_type == ClientLogEntry.OpType.Write:
            writes += 1
            write_time += entry.duration
        elif entry.server_role is Role.PRIMARY:
            reads += 1
            read_time += entry.duration

    metrics["write_latency"] = write_time / writes if writes else None
    metrics["read_latency"] = read_time / reads if reads else None


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
    logger.info(f"Created {chart_path}")


async def main_coro(params: DictConfig, metrics: dict):
    logger.info(params)
    seed = int(time.monotonic_ns() if params.seed is None else params.seed)
    logger.info(f"Seed {seed}")
    prng = PRNG(
        seed,
        params.one_way_latency_mean,
        params.one_way_latency_variance,
        params.keyspace_size,
    )
    nodes = [
        Node(index=1, role=Role.PRIMARY, cfg=params, prng=prng),
        Node(index=2, role=Role.SECONDARY, cfg=params, prng=prng),
        Node(index=3, role=Role.SECONDARY, cfg=params, prng=prng),
    ]
    for n in nodes:
        n.initiate(nodes)

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
                nodes=nodes,
                client_log=client_log,
                prng=prng,
            )
        else:
            coro = reader(
                client_id=i,
                start_ts=start_ts,
                nodes=nodes,
                client_log=client_log,
                prng=prng,
            )

        tasks.append(lp.create_task(name=f"client {i}", coro=coro))

    lp.create_task(
        name="nemesis",
        coro=nemesis(nodes=nodes, prng=prng, stepdown_rate=params.stepdown_rate)
    ).ignore_future()

    for t in tasks:
        await t

    lp.stop()
    logger.info(f"Finished after {get_current_ts()} ms (simulated)")
    save_metrics(metrics, client_log)

    def avg_metric(name: str) -> float | None:
        total = sum(s.metrics.total(name) for s in nodes)
        sample_count = sum(s.metrics.sample_count(name) for s in nodes)
        if sample_count > 0:
            return total / sample_count

    metrics["replication_lag"] = avg_metric("replication_lag")
    metrics["commit_lag"] = avg_metric("commit_lag")
    metrics["commit_latency"] = avg_metric("commit_latency")
    metrics["read_commit_wait_pct"] = avg_metric("read_commit_wait_pct")
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
    initiate_logging()
    event_loop = get_event_loop()
    csv_writer: None | csv.DictWriter = None
    csv_path = "metrics/metrics.csv"
    csv_file = open(csv_path, "w+")
    raw_params = yaml.safe_load(open("params.yaml"))
    for params in all_param_combos(raw_params):
        metrics = {}
        event_loop.create_task("main", main_coro(params=params, metrics=metrics))
        event_loop.run()
        logger.info(f"metrics: {metrics}")
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
