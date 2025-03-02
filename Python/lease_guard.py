import copy
import enum
import logging
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Callable, Collection, Union

from omegaconf import DictConfig
from pandas import Interval

from prob import PRNG
from simulate import Timestamp, get_current_ts, get_event_loop, sleep

_logger = logging.getLogger("lease-raft")


class Role(enum.Enum):
    PRIMARY = enum.auto()
    SECONDARY = enum.auto()


class ReadConcern(enum.Enum):
    LOCAL = enum.auto()
    MAJORITY = enum.auto()
    LINEARIZABLE = enum.auto()


@dataclass
class Interval:
    earliest: Timestamp
    latest: Timestamp

    def __add__(self, other: Timestamp) -> Interval:
        return Interval(self.earliest + other, self.latest + other)

    def __sub__(self, other: Timestamp) -> Interval:
        return Interval(self.earliest - other, self.latest - other)


@dataclass
class Write:
    """All writes are list-appends, to make linearizability checking easy."""
    key: int
    value: int
    """The value appended to the list associated with key."""
    term: int
    ts: Interval
    """Timestamp from originating primary's clock."""
    created_at_absolute_ts: Timestamp
    """Absolute timestamp when primary created this write."""
    committed_at_absolute_ts: Timestamp | None = field(compare=False, default=None)
    """Absolute timestamp at which this node learned this write was committed."""

    def copy(self) -> "Write":
        entry = copy.copy(self)
        entry.committed_at_absolute_ts = None
        return entry


@dataclass
class ReadReply:
    execution_ts: Timestamp
    """Absolute time the read occurred."""
    value: list[int]


class Metrics:
    def __init__(self, metrics_start_ts: Timestamp = 0):
        self._totals = Counter()
        self._sample_counts = Counter()
        self._metrics_start_ts = metrics_start_ts

    def update(self, metric_name: str, sample: int) -> None:
        if get_current_ts() < self._metrics_start_ts:
            return
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
            # After a network delay, check again for partition then deliver the message.
            get_event_loop().call_later(delay=self.prng.one_way_latency_value(),
                                        callback=self._deliver,
                                        from_id=from_id,
                                        method=method,
                                        *args,
                                        **kwargs)
        else:
            _logger.debug(f"{from_id} -> {to_id}: {method.__name__} DROPPED")

    def make_partition(self,
                       left_partition: Collection[Union[int, "Node"]],
                       right_partition: Collection[Union[int, "Node"]]) -> None:
        lp = set(n.node_id if isinstance(n, Node) else n for n in left_partition)
        rp = set(n.node_id if isinstance(n, Node) else n for n in right_partition)
        assert lp.isdisjoint(rp)
        assert lp.union(rp) == set(self.node_ids)
        self.left_partition = lp
        self.right_partition = rp
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

    def _deliver(self, from_id: int, method: Callable, *args, **kwargs) -> None:
        to_id = method.__self__.node_id
        if self.reachable(from_id, to_id):
            get_event_loop().call_soon(callback=method, *args, **kwargs)


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

    def primaries(self, min_term: int, min_ts: Timestamp) -> list[int]:
        """Primaries we've heard from since min_ts with term>=min_term."""
        return [node_id for node_id, p in self.pings.items()
                if p.term >= min_term and p.ts >= min_ts and p.role == Role.PRIMARY]

    def ping_timestamps(self) -> list[Timestamp]:
        return [p.ts for p in self.pings.values()]


class _NodeClock:
    """One node's clock."""

    def __init__(self, cfg: DictConfig, prng: PRNG):
        self.previous_ts: Interval = Interval(get_current_ts(),
                                              get_current_ts() + cfg.max_clock_error)
        self.max_clock_error: int = cfg.max_clock_error
        self.prng = prng

    def now(self) -> Interval:
        if self.max_clock_error > 0:
            def epsilon() -> int:
                return int(self.prng.uniform(0, self.max_clock_error))

            now = get_current_ts()
            # Return monotonically increasing, jittery timestamps.
            next_ts = Interval(
                max(now - epsilon(), self.previous_ts.earliest),
                max(now + epsilon(), self.previous_ts.latest),
            )
            self.previous_ts = next_ts
            return next_ts
        else:
            return Interval(get_current_ts(), get_current_ts())

    def is_past(self, ts: Interval) -> bool:
        return ts.latest < self.now().earliest

    def is_future(self, ts: Interval) -> bool:
        return ts.earliest > self.now().latest


_NOOP = -1
_BUSY_WAIT = 10


class Node:
    def __init__(self,
                 node_id: int,
                 cfg: DictConfig,
                 prng: PRNG,
                 network: Network,
                 clock=None):
        if cfg.quorum_check_enabled:
            assert not cfg.lease_enabled, "Quorum check and leases are incompatible"
        if cfg.lease_enabled:
            assert not cfg.quorum_check_enabled, "Lease & quorum check are incompatible"
        if cfg.defer_commit_enabled:
            assert cfg.lease_enabled, "defer_commit_enabled requires leases"
        if cfg.inherit_lease_enabled:
            assert cfg.lease_enabled, "inherit_lease_enabled requires leases"

        self.node_id = node_id
        self.role = Role.SECONDARY
        self.prng = prng
        self.clock = _NodeClock(cfg, prng) if clock is None else clock
        self.network = network
        self.monitor = _Monitor()
        self.quorum_check_enabled: bool = cfg.quorum_check_enabled
        self.lease_enabled: bool = cfg.lease_enabled
        self.inherit_lease_enabled = cfg.inherit_lease_enabled
        self.defer_commit_enabled = cfg.defer_commit_enabled
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
        self.log_write_micros: int = cfg.log_write_micros
        self.metrics = Metrics(metrics_start_ts=cfg.get("metrics_start_ts", 0))

    def initiate(self, nodes: dict[int, "Node"]):
        self.nodes = nodes.copy()
        self.match_index = {n.node_id: -1 for n in nodes.values()}
        get_event_loop().create_task("no-op writer", self.noop_writer())
        get_event_loop().create_task("replication", self.replicate())
        get_event_loop().create_task("heartbeat", self.heartbeat())
        get_event_loop().create_task("commit index", self.commit_index_updater())

    async def noop_writer(self):
        """Write a periodic noop. Ensures lease extension for readonly workloads.

        The paper mentions an optimization for readonly workloads: only write a noop
        when a query arrives at a leaseless leader.
        """
        try:
            while True:
                await sleep(self.noop_rate)
                if self.role is Role.PRIMARY:
                    _logger.debug(f"{self} writing noop")
                    self._write_internal(_NOOP, _NOOP)
        except Exception as e:
            _logger.exception(e)
            raise

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
                    self.match_index[self.node_id] = index

                return

    async def replicate(self):
        """Eternal thread that replicates log entries."""
        try:
            self._reset_election_deadline()
            while True:
                if self.role is not Role.SECONDARY:
                    await sleep(_BUSY_WAIT)
                    continue

                now = self.clock.now()
                primary_ids = self.monitor.primaries(
                    min_term=self.current_term,
                    min_ts=now.latest - self.election_timeout)

                if primary_ids:
                    self._reset_election_deadline()
                else:
                    if self.election_deadline <= now.earliest:
                        _logger.info(f"{self} deadline {self.election_deadline} passed")
                        self.become_candidate()  # Resets election deadline.

                    await sleep(_BUSY_WAIT)
                    continue

                sync_source = self.nodes[self.prng.choice(primary_ids)]
                if not self.network.reachable(self.node_id, sync_source.node_id):
                    logging.debug(f"{self} can't sync from {sync_source}, unreachable")
                    await sleep(_BUSY_WAIT)
                    continue

                self.monitor.received_ping(node_id=sync_source.node_id,
                                           role=sync_source.role,
                                           term=sync_source.current_term,
                                           ts=now.earliest)
                if sync_source.current_term < self.current_term:
                    _logger.info(f"{sync_source} stepping down, {self} has higher term")
                    sync_source.stepdown()
                    continue

                self._maybe_rollback(sync_source)
                commit_index = sync_source.commit_index
                if len(self.log) >= len(sync_source.log):
                    # Empty batch of entries, but we learn the source's commit index.
                    await sleep(self.prng.one_way_latency_value())
                    if self.role is Role.SECONDARY:
                        self._update_commit_index(commit_index)
                    continue

                entry = sync_source.log[len(self.log)]
                # Simulate waiting for entry to arrive. It may have arrived already.
                apply_time = (
                    entry.created_at_absolute_ts + self.prng.one_way_latency_value())
                await sleep(max(0, apply_time - get_current_ts()))
                if self.role is not Role.SECONDARY:
                    # Elected while waiting.
                    continue

                self._update_commit_index(commit_index)
                lag = get_current_ts() - entry.created_at_absolute_ts
                _logger.debug(
                    f"{self} got entry {entry.key}+={entry.value}, lag {lag}")
                self.log.append(entry.copy())
                self.match_index[self.node_id] = len(self.log) - 1
                self.network.send(self.node_id,
                                  sync_source.update_secondary_position,
                                  node_id=self.node_id,
                                  term=self.current_term,
                                  log_index=len(self.log) - 1)
                self.metrics.update("replication_lag", lag)
                if self.log_write_micros is not None:
                    await sleep(self.log_write_micros)

        except Exception as e:
            _logger.exception(e)
            raise

    async def heartbeat(self):
        """Send heartbeat requests to all peers."""
        try:
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
                    min_ts=self.clock.now().latest - self.election_timeout)
                if self.role is not Role.PRIMARY and not primary_ids:
                    # Try harder to find a primary.
                    next_heartbeat = self.heartbeat_rate // 10

                await sleep(next_heartbeat)
        except Exception as e:
            _logger.exception(e)
            raise

    async def commit_index_updater(self):
        """Periodically check if we can advance the commit index.

        With the deferred commit optimization, a primary can write without a lease but
        can't advance the commit point. Since the primary acquires a lease purely due to
        time passing, we need an eternal task to check if we can advance commit index.
        """
        while True:
            if self.has_lease(for_writes=True):
                self._primary_update_commit_index()

            await sleep(_BUSY_WAIT)

    def request_heartbeat(self, node_id: int, term: int, role: Role) -> None:
        """I received a heartbeat request."""
        self.monitor.received_ping(
            node_id=node_id, role=role, term=term, ts=self.clock.now().earliest)
        node = self.nodes[node_id]
        logging.debug(f"{self} got heartbeat from {node}")
        self._maybe_stepdown(term)
        self.network.send(self.node_id,
                          node.reply_to_heartbeat,
                          node_id=self.node_id,
                          term=self.current_term,
                          role=self.role)

    def reply_to_heartbeat(self, node_id: int, term: int, role: Role) -> None:
        """I received a heartbeat reply."""
        self.monitor.received_ping(
            node_id=node_id, role=role, term=term, ts=self.clock.now().earliest)
        node = self.nodes[node_id]
        logging.debug(f"{self} got heartbeat reply from {node}")
        self._maybe_stepdown(term)

    def become_candidate(self) -> None:
        if self.role is not Role.SECONDARY:
            return

        self.current_term += 1
        _logger.info(f"{self} running for election in term {self.current_term}")
        self._reset_election_deadline()
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
        """I'm a voter, receiving a candidate's RequestVote message."""
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
        """I'm a candidate, receiving a yes or no vote."""
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
                if self.inherit_lease_enabled:
                    assert not self.log or self.log[-1].term < term
                    logging.debug(
                        f"Limbo region {self.commit_index} to {len(self.log) - 1}")
                    if self.commit_index > -1:
                        for e in self.log[self.commit_index:]:
                            logging.debug(f"{e}")
                # Write a noop.
                self._write_internal(_NOOP, _NOOP)

    def _update_commit_index(self, index: int) -> None:
        """Primary advances index, or secondary receives primary's index."""
        self.commit_index = max(self.commit_index, index)
        # A MongoDB secondary can learn of a commit index higher than it has replicated.
        # NOTE: this follows MongoDB, whereas our TLA+ follows Raft.
        start_i = min(len(self.log) - 1, self.commit_index)
        # Reverse-iter, mark when entries became visible to rc:majority on this node.
        # This info is used to check linearizability.
        for i in range(start_i, -1, -1):
            if self.log[i].committed_at_absolute_ts is not None:
                # This entry and all prior have been marked.
                break

            self.log[i].committed_at_absolute_ts = get_current_ts()

    def update_secondary_position(self, node_id: int, term: int, log_index: int):
        """I'm a primary, receiving a secondary's replication position."""
        self.monitor.received_ping(node_id=node_id,
                                   role=Role.SECONDARY,
                                   term=term,
                                   ts=self.clock.now().earliest)

        self._maybe_stepdown(term)
        if self.role is not Role.PRIMARY:
            return

        self.match_index[node_id] = log_index
        if (self.lease_enabled
            and self.defer_commit_enabled
            and not self.has_lease(for_writes=True)):
            _logger.info(f"{self} pinning commit index until I have a lease")
            return

        self._primary_update_commit_index()

    def _primary_update_commit_index(self) -> None:
        """I'm a primary, updating commit index from secondaries' known positions."""
        assert self.role is Role.PRIMARY
        old_commit_index = self.commit_index
        self._update_commit_index(statistics.median(self.match_index.values()))
        if self.commit_index != old_commit_index:
            for n in self.nodes.values():
                if n is not self:
                    self.network.send(self.node_id,
                                      n.update_commit_index,
                                      node_id=self.node_id,
                                      term=self.current_term,
                                      index=self.commit_index)

    def update_commit_index(self, node_id: int, term: int, index: int):
        """I'm a secondary. The primary tells me its commit index."""
        self.monitor.received_ping(
            node_id=node_id, role=Role.PRIMARY, term=term, ts=self.clock.now().earliest)
        if self.role is not Role.SECONDARY:
            # I was elected while this message was in flight.
            return
        self._update_commit_index(index)

    def has_lease(self, for_writes: bool) -> bool:
        """Decide if I can read, or write (or commit with defer_commit_enabled)."""
        if self.role is not Role.PRIMARY:
            return False

        if for_writes or not self.inherit_lease_enabled:
            # Wait for past leader's lease to expire.
            prior_entry = next(
                (e for e in reversed(self.log) if e.term != self.current_term), None)

            if (prior_entry
                    and not self.clock.is_past(prior_entry.ts + self.lease_timeout)):
                # Previous leader still has write lease and defer_commit is disabled
                # (otherwise write() doesn't call has_lease()).
                return False
        else:
            if self.commit_index == -1:
                return False

            # Raft guarantees this, but not MongoDB. We follow Raft in this case.
            assert self.commit_index < len(self.log)
            # We need a committed entry less than lease_timeout old, in *any* term.
            # "Inherited read lease" means this primary can serve reads before it gets a
            # lease, while a prior primary's lease is valid.
            if not self.clock.is_future(
                    self.log[self.commit_index].ts + self.lease_timeout):
                return False

        return True

    def _write_internal(self, key: int, value: int) -> None:
        """Append an oplog entry."""
        if self.role is not Role.PRIMARY:
            raise Exception("Not primary")

        now = self.clock.now()
        w = Write(key=key,
                  value=value,
                  term=self.current_term,
                  ts=now,
                  created_at_absolute_ts=get_current_ts())
        self.log.append(w)
        self.match_index[self.node_id] = len(self.log) - 1

    async def _await_commit_index(self, index: int) -> None:
        while self.commit_index < index:
            await sleep(_BUSY_WAIT)
            if self.role is not Role.PRIMARY:
                raise Exception("Stepped down while waiting for w:majority")

    async def write(self, key: int, value: int) -> Timestamp:
        """Append value to the list associated with key.

        In detail: append an oplog entry with 'value', wait for w:majority.

        Return absolute time write was committed on this node.
        """
        if self.role is not Role.PRIMARY:
            raise Exception("Not primary")

        while (self.lease_enabled
               and not self.defer_commit_enabled
               and not self.has_lease(for_writes=True)):
            await sleep(_BUSY_WAIT)
            if self.role is not Role.PRIMARY:
                raise Exception("Stepped down while waiting for lease")

        self._write_internal(key=key, value=value)
        write_index = len(self.log) - 1
        start_ts = get_current_ts()
        await self._await_commit_index(index=write_index)
        commit_latency = get_current_ts() - start_ts
        self.metrics.update("commit_latency", commit_latency)

        committed_at_absolute_ts = self.log[write_index].committed_at_absolute_ts
        assert committed_at_absolute_ts is not None
        return committed_at_absolute_ts

    async def read(self, key: int, concern: ReadConcern) -> ReadReply:
        """Get a key's latest value, which is the list of values appended.

        Return absolute time read occurred, and list of values.
        """
        # We're not testing any consistency guarantees for secondary reads in this
        # simulation, so assume all queries have readPreference: "primary".
        if self.role is not Role.PRIMARY:
            raise Exception("Not primary")

        if (concern is not ReadConcern.LOCAL
            and self.lease_enabled
            and not self.has_lease(for_writes=False)):
            raise Exception("Not leaseholder")

        awaited_limbo_read = 0
        if (concern is ReadConcern.LINEARIZABLE
            and self.lease_enabled
            and self.inherit_lease_enabled):
            # Limbo-read guard for inherited lease.
            while True:
                if self.log[self.commit_index].term == self.current_term:
                    # No other leader can commit, so I can read.
                    break
                if self._last_committed(key) == self._last_in_prior_term(key):
                    # My committed data for this key is guaranteed fresh.
                    break
                if awaited_limbo_read == 0:
                    _logger.info(f"Limbo read for key {key}")
                awaited_limbo_read = 1
                await sleep(_BUSY_WAIT)
                if self.role is not Role.PRIMARY:
                    raise Exception("Stepped down while awaiting limbo read")

        self.metrics.update("limbo_reads", awaited_limbo_read)

        # MAJORITY and LINEARIZABLE read at the majority-commit index.
        log = (self.log if concern is ReadConcern.LOCAL
               else self.log[:self.commit_index + 1])
        execution_ts = get_current_ts()

        if concern is ReadConcern.LINEARIZABLE and self.quorum_check_enabled:
            # Commit a noop.
            self._write_internal(key=_NOOP, value=_NOOP)
            noop_index = len(self.log) - 1
            await self._await_commit_index(index=noop_index)

        return ReadReply(execution_ts=execution_ts,
                         value=[e.value for e in log if e.key == key])

    def _last_committed(self, key: int) -> int:
        """Index of last committed entry that wrote to key, or -1."""
        for i in reversed(range(self.commit_index + 1)):
            if self.log[i].key == key:
                return i

        return -1

    def _last_in_prior_term(self, key: int) -> int:
        """Index of last entry that wrote to key in prior term, or -1."""
        for i in reversed(range(len(self.log))):
            if self.log[i].term < self.current_term and self.log[i].key == key:
                return i

        return -1

    def _maybe_stepdown(self, term: int) -> bool:
        if term > self.current_term:
            self.current_term = term
            if self.role is Role.PRIMARY:
                logging.info(f"{self} stepping down, saw higher term {term}")
                self.stepdown()
                return True

        return False

    def stepdown(self):
        """Tell this node to become secondary."""
        if self.role is Role.PRIMARY:
            self.role = Role.SECONDARY
            self._reset_election_deadline()

    def _reset_election_deadline(self):
        self.election_deadline = (
            self.clock.now().latest + self.election_timeout
            + self.prng.randint(0, self.prng._one_way_latency_mean * 2 * self.node_id))

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
