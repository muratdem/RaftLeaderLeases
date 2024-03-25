import csv
import enum
import itertools
import logging
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Callable

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import yaml
from omegaconf import DictConfig

from prob import PRNG
from simulate import Timestamp, get_current_ts, get_event_loop, sleep

logger = logging.getLogger("lease-raft")


class Role(enum.Enum):
    PRIMARY = enum.auto()
    SECONDARY = enum.auto()


@dataclass(order=True)
class Write:
    key: str = field(compare=False)
    value: str = field(compare=False)
    term: int = field(compare=False)
    ts: Timestamp


class Metrics:
    def __init__(self):
        self._totals = Counter()
        self._sample_counts = Counter()

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


class Network:
    def __init__(self, prng: PRNG, node_ids: list[int]):
        self.prng = prng
        self.node_ids = node_ids
        self.left_partition: set[int] = set(node_ids)
        self.right_partition: set[int] = set()

    def send(self, from_id: int, method: Callable, *args, **kwargs) -> None:
        assert from_id in self.node_ids
        assert isinstance(method.__self__, Node)
        to_id = method.__self__.node_id
        assert to_id in self.node_ids
        if self.reachable(from_id, to_id):
            get_event_loop().call_later(self.prng.one_way_latency_value(),
                                        method,
                                        *args,
                                        **kwargs)
        else:
            logging.debug(f"Drop {method.__name__} call from {from_id} to {to_id}")

    def make_partition(self):
        assert len(self.node_ids) == 3, "Rewrite logic for other numbers of nodes"
        loner = self.prng.choice(self.node_ids)
        self.left_partition = set([loner])
        self.right_partition = set(self.node_ids) - self.left_partition
        logger.info(f"Partitioned {self.left_partition} from {self.right_partition}")

    def reset_partition(self):
        self.left_partition = set(self.node_ids)
        self.right_partition = set()
        logger.info("Healed partition")

    def reachable(self, from_id: int, to_id: int):
        return ((from_id in self.left_partition and to_id in self.left_partition)
                or (from_id in self.right_partition and to_id in self.right_partition))


class Node:
    def __init__(self, node_id: int, cfg: DictConfig, prng: PRNG, network: Network):
        self.node_id = node_id
        self.role = Role.SECONDARY
        self.prng = prng
        self.network = network
        # Raft state (Raft paper p. 4).
        self.current_term = 0
        # Map from term to the id of the node we voted for in that term.
        self.voted_for: dict[int, int] = {}
        self.log: list[Write] = []
        self.commit_index = 0
        # Map from node id to the node's last-replicated log index.
        self.match_index: dict[int, int] = {}
        self.election_deadline = 0
        self.nodes: dict[int, Node] = {}
        # Node indexes of nodes that voted for us in each term.
        self.votes_received: defaultdict[int, set] = defaultdict(set)
        self.noop_rate: int = cfg.noop_rate
        self.metrics = Metrics()

    def initiate(self, nodes: dict[int, "Node"]):
        self.nodes = nodes.copy()
        self.match_index = {n.node_id: 0 for n in nodes.values()}
        get_event_loop().create_task("no-op writer", self.noop_writer())
        get_event_loop().create_task("replication", self.replicate())

    async def noop_writer(self):
        while True:
            await sleep(self.noop_rate)
            if self.role is Role.PRIMARY:
                self._write_internal("noop", "")

    def _maybe_rollback(self, sync_source: "Node"):
        # Search backward through source's log for latest entry that matches ours.
        for index, source_entry in reversed(list(enumerate(sync_source.log))):
            if index >= len(self.log):
                continue

            if self.log[index].term == sync_source.log[index].term:
                # This is the latest matching index.
                assert self.log[index] == sync_source.log[index]
                n_rollback = len(self.log) - index - 1
                if n_rollback > 0:
                    logging.info(f"{self} rolling back {n_rollback} entries:")
                    for e in self.log[index + 1:]:
                        logging.info(f"    {e}")
                    del self.log[index + 1:]

                return

    async def replicate(self):
        self._reset_election_deadline()
        while True:
            await sleep(1)
            if self.role is not Role.SECONDARY:
                continue

            # Imagine we magically know who the primaries are. Unlike MongoDB, we redo
            # sync source selection for every entry.
            primaries = [n for n in self.nodes.values() if n.role is Role.PRIMARY]
            if not primaries:
                if self.election_deadline <= get_current_ts():
                    self.become_candidate()  # Resets election deadline.

                continue

            sync_source = self.prng.choice(primaries)
            if not self.network.reachable(self.node_id, sync_source.node_id):
                logging.debug(
                    f"Node {self.node_id} can't sync from {sync_source.node_id}")
                await sleep(100)
                continue

            if sync_source.current_term < self.current_term:
                sync_source.stepdown()
                continue

            self._maybe_rollback(sync_source)
            if len(self.log) < len(sync_source.log):
                entry = sync_source.log[len(self.log)]
                # Simulate waiting for entry to arrive. It may have arrived already.
                apply_time = entry.ts + self.prng.one_way_latency_value()
                await sleep(max(0, apply_time - get_current_ts()))
                if self.role is not Role.SECONDARY:
                    # Elected while waiting.
                    continue

                self._reset_election_deadline()
                self.log.append(entry)
                self.match_index[self.node_id] = len(self.log) - 1
                self.network.send(self.node_id,
                                  sync_source.update_secondary_position,
                                  node_id=self.node_id,
                                  log_index=len(self.log) - 1)
                self.metrics.update("replication_lag", get_current_ts() - entry.ts)

    def become_candidate(self) -> None:
        if self.role is not Role.SECONDARY:
            return

        self._reset_election_deadline()
        self.current_term += 1
        self.voted_for[self.current_term] = self.node_id
        self.votes_received[self.current_term] = set([self.node_id])
        for node in self.nodes.values():
            if node is not self:
                self.network.send(self.node_id,
                                  node.request_vote,
                                  term=self.current_term,
                                  candidate_id=self.node_id,
                                  last_log_index=len(self.log) - 1,
                                  last_log_term=self.log[-1].term if self.log else 0)

    def request_vote(self, term: int, candidate_id: int, last_log_index: int,
                     last_log_term: int) -> None:
        granted = True
        if term < self.current_term:
            granted = False

        if term > self.current_term and self.role is Role.PRIMARY:
            self.stepdown()

        self.current_term = max(self.current_term, term)
        if self.voted_for.get(term) not in (None, candidate_id):
            granted = False

        # Raft paper 5.4.1: "If the logs have last entries with different terms, then
        # the log with the later term is more up-to-date. If the logs end with the same
        # term, then whichever log is longer is more up-to-date."
        if self.log and last_log_term < self.log[-1].term:
            granted = False

        if last_log_index < len(self.log) - 1:
            granted = False

        self.voted_for[term] = candidate_id
        self.network.send(self.node_id,
                          self.nodes[candidate_id].receive_vote,
                          voter_id=self.node_id,
                          term=self.current_term,
                          vote_granted=granted)

    def receive_vote(self, voter_id: int, term: int, vote_granted: bool) -> None:
        if term > self.current_term:
            self.current_term = term
            # Delayed vote reply reveals that we've been superseded.
            if self.role is Role.PRIMARY:
                self.stepdown()
        elif term == self.current_term and vote_granted and self.role is Role.SECONDARY:
            self.votes_received[term].add(voter_id)
            if len(self.votes_received[term]) > len(self.nodes) / 2:
                self.role = Role.PRIMARY
                logging.info(f"{self} elected in term {self.current_term}")

    def update_secondary_position(self, node_id, log_index: int):
        if self.role is not Role.PRIMARY:
            return

        # TODO: what about an out-of-order message? Is max() below the right solution?
        self.match_index[node_id] = log_index
        self.commit_index = max(self.commit_index,
                                statistics.median(self.match_index.values()))

        for n in self.nodes.values():
            if n is not self:
                self.network.send(self.node_id,
                                  n.update_commit_index,
                                  index=self.commit_index)

    def update_commit_index(self, index: int):
        self.commit_index = max(self.commit_index, index)

    def _write_internal(self, key: str, value: str) -> None:
        """Update a key and append an oplog entry."""
        if self.role is not Role.PRIMARY:
            raise Exception("Not primary")

        w = Write(key=key, value=value, term=self.current_term, ts=get_current_ts())
        self.log.append(w)
        self.match_index[self.node_id] = len(self.log) - 1

    async def write(self, key: str, value: str):
        """Update a key, append an oplog entry, wait for w:majority."""
        self._write_internal(key=key, value=value)
        write_index = len(self.log) - 1
        start_ts = get_current_ts()
        while self.commit_index < write_index:
            await sleep(1)
            if self.role is not Role.PRIMARY:
                raise Exception("Stepped down while waiting for w:majority")

        commit_latency = get_current_ts() - start_ts
        self.metrics.update("commit_latency", commit_latency)

    async def read(self, key: str) -> str | None:
        """Return a key's latest value."""
        # We're not testing any consistency guarantees for secondary reads in this
        # simulation, so assume all queries have readPreference: "primary".
        if self.role is not Role.PRIMARY:
            raise Exception("Not primary")

        # TODO: readConcern.
        for entry in reversed(self.log):
            if entry.key == key:
                return entry.value

    def stepdown(self):
        """Tell this node to become secondary."""
        self.role = Role.SECONDARY

    def _reset_election_deadline(self):
        election_timeout = 10000  # TODO: in params.yaml
        self.election_deadline = (get_current_ts() + election_timeout
                                  + self.prng.randint(0, election_timeout))

    def __str__(self) -> str:
        return f"Node {self.node_id} {self.role.name}"


@dataclass
class ClientLogEntry:
    class OpType(enum.Enum):
        Write = enum.auto()
        Read = enum.auto()

    client_id: int
    op_type: OpType
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

        if str(e) != "Not primary":
            # Add it to the log, some failed writes commit eventually.
            client_log.append(
                ClientLogEntry(
                    client_id=client_id,
                    op_type=ClientLogEntry.OpType.Write,
                    start_ts=start_ts,
                    end_ts=get_current_ts(),
                    key=key,
                    value=value,
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
        logging.info(f"Stepping down {primary}")
        primary.stepdown()


async def partition_nemesis(network: Network,
                            prng: PRNG,
                            partition_rate: int):
    while True:
        await sleep(round(prng.exponential(partition_rate)))
        network.make_partition()
        await sleep(round(prng.exponential(partition_rate)))
        network.reset_partition()


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

            # Assume a failed write has no effect, new model == old model.
            if entry.op_type is ClientLogEntry.OpType.Write and not entry.success:
                linearization = linearize(log_prime, model)
                if linearization is not None:
                    return [entry] + linearization

            # Assume the write succeeded. Even if !success it might eventually commit.
            if entry.op_type is ClientLogEntry.OpType.Write:
                model_prime = model.copy()
                model_prime[entry.key] = entry.value
                # Try to linearize the rest of the log with the KV store in this state.
                linearization = linearize(log_prime, model_prime)
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
        elif entry.op_type == ClientLogEntry.OpType.Read:
            reads += 1
            read_time += entry.duration
        else:
            assert False, "unknown op type"

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
    network = Network(prng=prng, node_ids=[1, 2, 3])
    nodes = {
        1: Node(node_id=1, cfg=params, prng=prng, network=network),
        2: Node(node_id=2, cfg=params, prng=prng, network=network),
        3: Node(node_id=3, cfg=params, prng=prng, network=network),
    }

    # Log messages show current timestamp and nodes' roles.
    class CustomFormatter(logging.Formatter):
        def format(self, record):
            original_msg = super().format(record)
            node_roles = " ".join("1" if n.role is Role.PRIMARY else "2"
                                  for n in nodes.values())
            return f"{get_current_ts(): 8} {node_roles} {original_msg}"

    formatter = CustomFormatter(fmt="%(levelname)s: %(message)s")
    for h in logging.getLogger().handlers:
        h.setFormatter(formatter)

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
                               partition_rate=params.partition_rate)
    ).ignore_future()

    for t in tasks:
        await t

    lp.stop()
    logger.info(f"Finished after {get_current_ts()} ms (simulated)")
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
    logging.basicConfig(level=logging.DEBUG)
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
