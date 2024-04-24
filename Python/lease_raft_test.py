import inspect
import logging
import unittest
from unittest import TestCase

from omegaconf import DictConfig

from run_raft_with_params import ClientLogEntry, do_linearizability_check
from lease_raft import Network, Node, ReadConcern, Role, setup_logging
from prob import PRNG
from simulate import get_event_loop, sleep

logging.basicConfig(level=logging.INFO)


def simulator_test_func(func):
    def wrapper(self, *args, **kwargs) -> None:
        task = self.loop.create_task(func.__name__, func(self, *args, **kwargs))
        self.loop.run_until_complete(task)

    return wrapper


class DetectCoroutines(type):
    def __new__(cls, name, bases, dct):
        new_dct = {}
        for attr_name, attr_value in dct.items():
            if inspect.iscoroutinefunction(attr_value) and attr_name.startswith("test"):
                new_dct[attr_name] = simulator_test_func(attr_value)
            else:
                new_dct[attr_name] = attr_value
        return type.__new__(cls, name, bases, new_dct)


class SimulatorTestCase(TestCase, metaclass=DetectCoroutines):
    def setUp(self) -> None:
        self.loop = get_event_loop()

    def tearDown(self) -> None:
        self.loop.reset()


async def await_predicate(predicate):
    while True:
        rv = predicate()
        if rv:
            return rv

        await sleep(1)


class LeaseRaftTest(SimulatorTestCase):
    async def replica_set_setup(self, **kwargs) -> None:
        self.cfg = DictConfig({
            "max_clock_error": 0.1,
            "election_timeout": 1000,
            "one_way_latency_mean": 125,
            "one_way_latency_variance": 100,
            "noop_rate": 1000,
            "heartbeat_rate": 500,
            "operations": 50,
            "interarrival": 50,
            "stepdown_rate": 50000,
            "partition_rate": 500,
            "heal_rate": 1000,
            "keyspace_size": 1,
            "leases_enabled": False,
            "lease_timeout": 1000,
            "seed": 1,
        })

        self.cfg.update(kwargs)
        self.prng = PRNG(cfg=self.cfg)
        self.network = Network(prng=self.prng, node_ids=[1, 2, 3])
        self.nodes = {
            1: Node(node_id=1, cfg=self.cfg, prng=self.prng, network=self.network),
            2: Node(node_id=2, cfg=self.cfg, prng=self.prng, network=self.network),
            3: Node(node_id=3, cfg=self.cfg, prng=self.prng, network=self.network),
        }

        setup_logging(self.nodes)
        for n in self.nodes.values():
            n.initiate(self.nodes)

    async def get_primary(self, nodes: set[Node] | None = None) -> Node:
        if nodes is None:
            nodes = set(self.nodes.values())

        return await await_predicate(
            lambda: next((n for n in nodes if n.role == Role.PRIMARY), None))

    async def read_from_stale_primary(self,
                                      leases_enabled: bool,
                                      concern: ReadConcern,
                                      expected_result: list[int]) -> None:
        await self.replica_set_setup(leases_enabled=leases_enabled)
        primary_A = await self.get_primary()
        # Make sure primary A has commit index > -1.
        await primary_A.write(key=0, value=0)
        secondaries = set(n for n in self.nodes.values() if n.role == Role.SECONDARY)
        self.network.make_partition(
            set([primary_A.node_id]), set(n.node_id for n in secondaries))
        primary_B = await self.get_primary(secondaries)
        self.assertIsNot(primary_A, primary_B)
        self.assertEqual(primary_A.role, Role.PRIMARY)
        await primary_B.write(key=1, value=1)
        reply = await primary_A.read(key=1, concern=concern)
        self.assertEqual(reply.value, expected_result)

    async def test_read_concern_local_fails_read_your_writes(self):
        # Today, a client using the default writeConcern of w: "majority" and default
        # readConcern of "local" won't always read its writes. Soon after an election,
        # there could be a stale primary A which hasn't yet stepped down, and a fresh
        # primary B. The client could write to B, then read from A, and not see its
        # write.
        await self.read_from_stale_primary(concern=ReadConcern.LOCAL,
                                           expected_result=[],
                                           leases_enabled=False)

    async def test_read_concern_majority_fails_linearizability(self):
        # Today, a client using w: "majority", rc: "majority" can fail to read its write
        # (hence fail linearizability) if it reads from a stale primary.
        await self.read_from_stale_primary(concern=ReadConcern.MAJORITY,
                                           expected_result=[],
                                           leases_enabled=False)

    async def test_read_concern_local_upholds_read_your_writes(self):
        with self.assertRaisesRegex(Exception, r"Not leaseholder"):
            await self.read_from_stale_primary(concern=ReadConcern.LOCAL,
                                               expected_result=[1],
                                               leases_enabled=True)

    async def test_read_concern_majority_upholds_linearizability(self):
        with self.assertRaisesRegex(Exception, r"Not leaseholder"):
            await self.read_from_stale_primary(concern=ReadConcern.MAJORITY,
                                               expected_result=[1],
                                               leases_enabled=True)

    async def test_await_lease(self):
        await self.replica_set_setup(leases_enabled=True, noop_rate=1e10)
        primary = await self.get_primary()
        self.assertFalse(primary.has_lease(for_writes=True))
        self.assertFalse(primary.has_lease(for_writes=False))
        # The primary buffers this write until it has acquired a lease.
        await primary.write(1, 1)
        self.assertTrue(primary.has_lease(for_writes=True))
        self.assertTrue(primary.has_lease(for_writes=False))
        reply = await primary.read(1, concern=ReadConcern.MAJORITY)
        self.assertEqual([1], reply.value)

    async def test_read_with_prior_leader_lease(self):
        # Lease timeout > election timeout, so we have a stale leaseholder.
        await self.replica_set_setup(leases_enabled=True,
                                     election_timeout=1000,
                                     lease_timeout=2000)
        primary_A = await self.get_primary()
        # Make sure primary A has commit index > -1.
        await primary_A.write(key=0, value=0)
        self.assertTrue(primary_A.has_lease(for_writes=True))
        self.assertTrue(primary_A.has_lease(for_writes=False))
        secondaries = set(n for n in self.nodes.values() if n.role == Role.SECONDARY)

        # Partition primary A from the majority, a new primary is elected.
        self.network.make_partition(
            set([primary_A.node_id]), set(n.node_id for n in secondaries))
        primary_B = await self.get_primary(secondaries)
        self.assertIsNot(primary_A, primary_B)
        self.assertEqual(primary_A.role, Role.PRIMARY)

        # The new primary can use the old one's lease for reads.
        self.assertTrue(primary_B.has_lease(for_writes=False))
        self.assertFalse(primary_B.has_lease(for_writes=True))
        reply = await primary_A.read(key=0, concern=ReadConcern.MAJORITY)
        self.assertEqual(reply.value, [0])

        # The write waits for primary B to acquire a writer lease.
        await primary_B.write(key=1, value=1)
        reply = await primary_B.read(key=1, concern=ReadConcern.MAJORITY)
        self.assertEqual(reply.value, [1])
        self.assertTrue(primary_B.has_lease(for_writes=True))
        self.assertTrue(primary_B.has_lease(for_writes=False))

        with self.assertRaisesRegex(Exception, r"Not leaseholder"):
            await primary_A.read(key=1, concern=ReadConcern.MAJORITY)


class LinearizabilityTest(unittest.TestCase):
    def test_invalid(self):
        with self.assertRaisesRegex(Exception, "not linearizable"):
            do_linearizability_check([
                ClientLogEntry(
                    client_id=1,
                    op_type=ClientLogEntry.OpType.ListAppend,
                    start_ts=1,
                    absolute_ts=2,
                    end_ts=3,
                    key=1,
                    value=1,
                    success=True
                ),
                ClientLogEntry(
                    client_id=1,
                    op_type=ClientLogEntry.OpType.ListAppend,
                    start_ts=4,
                    absolute_ts=5,
                    end_ts=6,
                    key=2,
                    value=1,
                    success=True
                ),
                ClientLogEntry(
                    client_id=3,
                    op_type=ClientLogEntry.OpType.Read,
                    start_ts=6,
                    absolute_ts=7,
                    end_ts=8,
                    key=1,
                    value=[2, 1],  # Wrong, should be [1, 2].
                    success=True
                )])

    def test_simultaneous_events(self):
        event_1 = ClientLogEntry(
            client_id=1,
            op_type=ClientLogEntry.OpType.ListAppend,
            start_ts=1,
            absolute_ts=2,
            end_ts=2,
            key=1,
            value=1,
            success=True
        )
        event_2 = ClientLogEntry(
            client_id=2,
            op_type=ClientLogEntry.OpType.ListAppend,
            start_ts=1,
            absolute_ts=2,  # Same time as event_1.
            end_ts=2,
            key=1,
            value=2,
            success=True
        )
        read_event = ClientLogEntry(
            client_id=3,
            op_type=ClientLogEntry.OpType.Read,
            start_ts=3,
            absolute_ts=4,
            end_ts=4,
            key=1,
            value=[1, 2],  # The value if event_1 happened first.
            success=True
        )
        log = [event_1, event_2, read_event]
        do_linearizability_check(log)  # History is valid if event_1 was first.
        read_event.value = [2, 1]  # The value if event_2 happened first.
        do_linearizability_check(log)  # History is valid if event_2 was first.


if __name__ == '__main__':
    unittest.main()