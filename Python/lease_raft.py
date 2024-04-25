import copy
import enum
import logging
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Callable

from omegaconf import DictConfig

from prob import PRNG
from simulate import Timestamp, get_current_ts, get_event_loop, sleep

_logger = logging.getLogger("lease-raft")


class Role(enum.Enum):
    PRIMARY = enum.auto()
    SECONDARY = enum.auto()


class ReadConcern(enum.Enum):
    LOCAL = enum.auto()
    MAJORITY = enum.auto()


@dataclass
class Write:
    """All writes are list-appends, to make linearizability checking easy."""
    key: int
    value: int
    """The value appended to the list associated with key."""
    term: int
    ts: Timestamp
    """Timestamp from originating primary's clock."""
    local_ts: Timestamp = field(compare=False)
    """Timestamp at this server's clock."""

    def copy_with_local_ts(self, local_ts: Timestamp) -> "Write":
        entry = copy.copy(self)
        entry.local_ts = local_ts
        return entry


@dataclass
class ReadReply:
    absolute_ts: Timestamp
    """Absolute time the read occurred."""
    value: list[int]


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
            s = ", ".join([a for a in args] + [f"{k}={v}" for k, v in kwargs.items()])
            _logger.debug(f"{from_id} -> {to_id}: {method.__name__}({s})")
            get_event_loop().call_later(self.prng.one_way_latency_value(),
                                        method,
                                        *args,
                                        **kwargs)
        else:
            _logger.debug(f"{from_id} -> {to_id}: {method.__name__} DROPPED")

    def make_partition(self,
                       left_partition: set[int],
                       right_partition: set[int]) -> None:
        assert left_partition.isdisjoint(right_partition)
        assert left_partition.union(right_partition) == set(self.node_ids)
        self.left_partition = left_partition
        self.right_partition = right_partition
        _logger.info(f"Partitioned {self.left_partition} from {self.right_partition}")

    def make_random_partition(self):
        assert len(self.node_ids) == 3, "Rewrite logic for other numbers of nodes"
        loner = set([self.prng.choice(self.node_ids)])
        self.make_partition(loner, set(self.node_ids) - loner)

    def reset_partition(self):
        self.left_partition = set(self.node_ids)
        self.right_partition = set()
        _logger.info("Healed partition")

    def reachable(self, from_id: int, to_id: int):
        return ((from_id in self.left_partition and to_id in self.left_partition)
                or (from_id in self.right_partition and to_id in self.right_partition))


class _Monitor:
    """One node's view of its peers."""

    @dataclass
    class PeerPing:
        role: Role
        term: int
        ts: Timestamp

    def __init__(self):
        # Map node id -> latest ping.
        self.pings: dict[int, _Monitor.PeerPing] = {}

    def received_ping(self, node_id: int, role: Role, term: int, ts: Timestamp) -> None:
        self.pings[node_id] = _Monitor.PeerPing(role=role, term=term, ts=ts)

    def last_primary_timestamp(self) -> Timestamp:
        return max(
            (p.ts for p in self.pings.values() if p.role == Role.PRIMARY), default=-1)

    def primaries(self, min_term: int, min_ts: Timestamp) -> list[int]:
        return [node_id for node_id, p in self.pings.items()
                if p.term >= min_term and p.ts >= min_ts and p.role == Role.PRIMARY]


class _NodeClock:
    """One node's clock."""

    def __init__(self, cfg: DictConfig, prng: PRNG):
        self.previous_true_ts = self.previous_ts = get_current_ts()
        self.max_clock_error: float = cfg.max_clock_error
        self.prng = prng

    def now(self):
        now = get_current_ts()
        true_span = now - self.previous_true_ts
        next_ts = self.prng.randint(
            int(self.previous_ts + true_span * (1 - self.max_clock_error)),
            int(self.previous_ts + true_span * (1 + self.max_clock_error)))
        self.previous_ts = next_ts
        self.previous_true_ts = now
        return next_ts


class Node:
    def __init__(self, node_id: int, cfg: DictConfig, prng: PRNG, network: Network):
        self.node_id = node_id
        self.role = Role.SECONDARY
        self.prng = prng
        self.clock = _NodeClock(cfg, prng)
        self.network = network
        self.monitor = _Monitor()
        self.leases_enabled: bool = cfg.leases_enabled
        self.lease_timeout: int = cfg.lease_timeout
        # Raft state (Raft paper p. 4).
        self.current_term = 0
        # Map from term to the id of the node we voted for in that term.
        self.voted_for: dict[int, int] = {}
        self.log: list[Write] = []
        self.commit_index = -1
        # Map from node id to the node's last-replicated log index.
        self.match_index: dict[int, int] = {}
        self.election_deadline = 0
        self.nodes: dict[int, Node] = {}
        # Node indexes of nodes that voted for us in each term.
        self.votes_received: defaultdict[int, set] = defaultdict(set)
        self.noop_rate: int = cfg.noop_rate
        self.election_timeout: int = cfg.election_timeout
        self.heartbeat_rate: int = cfg.heartbeat_rate
        self.metrics = Metrics()

    def initiate(self, nodes: dict[int, "Node"]):
        self.nodes = nodes.copy()
        self.match_index = {n.node_id: 0 for n in nodes.values()}
        get_event_loop().create_task("no-op writer", self.noop_writer())
        get_event_loop().create_task("replication", self.replicate())
        get_event_loop().create_task("heartbeat", self.heartbeat())

    async def noop_writer(self):
        while True:
            await sleep(self.noop_rate)
            if self.role is Role.PRIMARY:
                _logger.info(f"{self} writing noop")
                self._write_internal(-1, -1)

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
            await sleep(10)
            if self.role is not Role.SECONDARY:
                continue

            now = self.clock.now()
            primary_ids = self.monitor.primaries(
                min_term=self.current_term,
                min_ts=now - self.election_timeout)

            if not primary_ids:
                if self.election_deadline <= now:
                    self.become_candidate()  # Resets election deadline.

                continue

            sync_source = self.nodes[self.prng.choice(primary_ids)]
            if not self.network.reachable(self.node_id, sync_source.node_id):
                logging.debug(f"{self} can't sync from {sync_source}, unreachable")
                await sleep(100)
                continue

            self.monitor.received_ping(node_id=sync_source.node_id,
                                       role=sync_source.role,
                                       term=sync_source.current_term,
                                       ts=self.clock.now())
            if sync_source.current_term < self.current_term:
                _logger.info(f"{sync_source} stepping down, {self} has higher term")
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

                lag = get_current_ts() - entry.ts
                _logger.debug(f"{self} got entry {entry.key}+={entry.value}, lag {lag}")
                self._reset_election_deadline()
                # Update entry's local_ts, for tracking leases.
                self.log.append(entry.copy_with_local_ts(self.clock.now()))
                self.match_index[self.node_id] = len(self.log) - 1
                self.network.send(self.node_id,
                                  sync_source.update_secondary_position,
                                  node_id=self.node_id,
                                  term=self.current_term,
                                  log_index=len(self.log) - 1)
                self.metrics.update("replication_lag", lag)

    async def heartbeat(self):
        while True:
            for node in self.nodes.values():
                if node is not self:
                    logging.debug(f"{self} sending heartbeat to {node}")
                    self.network.send(self.node_id,
                                      node.request_heartbeat,
                                      node_id=self.node_id,
                                      term=self.current_term,
                                      role=self.role)

            next_heartbeat = self.heartbeat_rate
            primary_ids = self.monitor.primaries(
                min_term=self.current_term,
                min_ts=self.clock.now() - self.election_timeout)
            if self.role is not Role.PRIMARY and not primary_ids:
                # Try harder to find a primary.
                next_heartbeat = self.heartbeat_rate // 10

            await sleep(next_heartbeat)

    def request_heartbeat(self, node_id: int, term: int, role: Role) -> None:
        self.monitor.received_ping(
            node_id=node_id, role=role, term=term, ts=self.clock.now())
        node = self.nodes[node_id]
        logging.debug(f"{self} got heartbeat from {node}")
        self._maybe_stepdown(term)
        self.network.send(self.node_id,
                          node.reply_to_heartbeat,
                          node_id=self.node_id,
                          term=self.current_term,
                          role=self.role)

    def reply_to_heartbeat(self, node_id: int, term: int, role: Role) -> None:
        self.monitor.received_ping(
            node_id=node_id, role=role, term=term, ts=self.clock.now())
        node = self.nodes[node_id]
        logging.debug(f"{self} got heartbeat reply from {node}")
        self._maybe_stepdown(term)

    def become_candidate(self) -> None:
        if self.role is not Role.SECONDARY:
            return

        self._reset_election_deadline()
        self.current_term += 1
        _logger.info(f"{self} running for election in term {self.current_term}")
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
            logging.debug(f"{self} voting against {candidate_id}, stale term {term}")

        self._maybe_stepdown(term)
        if granted and self.voted_for.get(term) not in (None, candidate_id):
            granted = False
            logging.debug(
                f"{self} voting against {candidate_id}, already voted in {term}")

        # Raft paper 5.4.1: "If the logs have last entries with different terms, then
        # the log with the later term is more up-to-date. If the logs end with the same
        # term, then whichever log is longer is more up-to-date."
        if granted and self.log and last_log_term < self.log[-1].term:
            granted = False
            logging.debug(
                f"{self} voting against {candidate_id}, its last log term is stale")

        if granted and last_log_index < len(self.log) - 1:
            granted = False
            logging.debug(f"{self} voting against {candidate_id}, my log is longer")

        if granted:
            logging.debug(f"{self} voting for {candidate_id}")
            self.voted_for[term] = candidate_id
            self._reset_election_deadline()

        self.network.send(self.node_id,
                          self.nodes[candidate_id].receive_vote,
                          voter_id=self.node_id,
                          term=self.current_term,
                          vote_granted=granted)

    def receive_vote(self, voter_id: int, term: int, vote_granted: bool) -> None:
        logging.debug(f"{self} received {vote_granted} vote from {voter_id}")
        if term > self.current_term:
            self.current_term = term
            # Delayed vote reply reveals that we've been superseded.
            if self.role is Role.PRIMARY:
                logging.info(f"{self} stepping down, node {voter_id} has higher term")
                self.stepdown()
        elif term == self.current_term and vote_granted and self.role is Role.SECONDARY:
            self.votes_received[term].add(voter_id)
            if len(self.votes_received[term]) > len(self.nodes) / 2:
                self.role = Role.PRIMARY
                logging.info(f"{self} elected in term {self.current_term}")
                # Write a noop.
                self._write_internal(-1, -1)

    def update_secondary_position(self, node_id: int, term: int, log_index: int):
        self.monitor.received_ping(node_id=node_id,
                                   role=Role.SECONDARY,
                                   term=term,
                                   ts=self.clock.now())

        self._maybe_stepdown(term)
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
                                  node_id=self.node_id,
                                  term=self.current_term,
                                  index=self.commit_index)

    def update_commit_index(self, node_id: int, term: int, index: int):
        self.monitor.received_ping(
            node_id=node_id, role=Role.PRIMARY, term=term, ts=self.clock.now())
        if self._maybe_stepdown(term):
            return

        self.commit_index = max(self.commit_index, index)

    def has_lease(self, for_writes: bool) -> bool:
        if self.role is not Role.PRIMARY or len(self.log) == 0 or self.commit_index < 0:
            return False

        lease_timeout_with_slop = self.lease_timeout * (1 + self.clock.max_clock_error)
        lease_start = self.clock.now() - lease_timeout_with_slop
        committed = self.log[:self.commit_index + 1]
        if committed[-1].local_ts <= lease_start:
            # My newest committed entry is before lease timeout, same for older entries.
            return False

        if for_writes:
            if committed[-1].term != self.current_term:
                # I haven't committed an entry yet. This check fixes SERVER-53813.
                return False

            # Wait for past leader's lease to expire. (My last committed entry could
            # be a no-op, which I wrote without a lease.)
            prior_entry = next(
                (e for e in reversed(committed) if e.term != self.current_term), None)

            if prior_entry and prior_entry.local_ts >= lease_start:
                # Previous leader still has write lease.
                return False

        return True

    def _write_internal(self, key: int, value: int) -> None:
        """Append an oplog entry."""
        if self.role is not Role.PRIMARY:
            raise Exception("Not primary")

        now = self.clock.now()
        w = Write(key=key, value=value, term=self.current_term, ts=now, local_ts=now)
        self.log.append(w)
        self.match_index[self.node_id] = len(self.log) - 1

    async def write(self, key: int, value: int) -> Timestamp:
        """Append value to the list associated with key.

        In detail: append an oplog entry with 'value', wait for w:majority.

        Return absolute time write was committed on this node.
        """
        if self.role is not Role.PRIMARY:
            raise Exception("Not primary")

        while self.leases_enabled and not self.has_lease(for_writes=True):
            await sleep(10)
            if self.role is not Role.PRIMARY:
                raise Exception("Stepped down while waiting for lease")

        self._write_internal(key=key, value=value)
        write_index = len(self.log) - 1
        start_ts = get_current_ts()
        while self.commit_index < write_index:
            await sleep(10)
            if self.role is not Role.PRIMARY:
                raise Exception("Stepped down while waiting for w:majority")

        commit_latency = get_current_ts() - start_ts
        self.metrics.update("commit_latency", commit_latency)
        return get_current_ts()

    async def read(self, key: int, concern: ReadConcern) -> ReadReply:
        """Get a key's latest value, which is the list of values appended.

        Return absolute time read occurred, and list of values.
        """
        # We're not testing any consistency guarantees for secondary reads in this
        # simulation, so assume all queries have readPreference: "primary".
        if self.role is not Role.PRIMARY:
            raise Exception("Not primary")

        if self.leases_enabled and not self.has_lease(for_writes=False):
            raise Exception("Not leaseholder")

        log = (self.log if concern is ReadConcern.LOCAL
               else self.log[:self.commit_index + 1])
        return ReadReply(absolute_ts=get_current_ts(),
                         value=[e.value for e in log if e.key == key])

    def _maybe_stepdown(self, term: int) -> bool:
        if term > self.current_term:
            self.current_term = term
            if self.role is Role.PRIMARY:
                logging.info(f"{self} stepping down, saw higher term {term}")
                self.stepdown()
                return True

    def stepdown(self):
        """Tell this node to become secondary."""
        self.role = Role.SECONDARY

    def _reset_election_deadline(self):
        self.election_deadline = (self.clock.now() + self.election_timeout
                                  + self.prng.randint(0, self.election_timeout))

    def __str__(self) -> str:
        return f"Node {self.node_id} {self.role.name}"


def setup_logging(nodes: dict[int, Node]) -> None:
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
