import inspect
import logging
import unittest
from unittest import TestCase

from omegaconf import DictConfig

from lease_raft import Network, Node, ReadConcern, Role, setup_logging
from prob import PRNG
from simulate import get_event_loop, sleep

logging.basicConfig(level=logging.DEBUG)


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
    def setUp(self) -> None:
        super().setUp()
        self.cfg = DictConfig({
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
            "check_linearizability": True,
            "keyspace_size": 1,
            "seed": 1,
        })

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
                                      concern: ReadConcern,
                                      expected_result: list[int]) -> None:
        primary_A = await self.get_primary()
        secondaries = set(n for n in self.nodes.values() if n.role == Role.SECONDARY)
        self.network.make_partition(
            set([primary_A.node_id]), set(n.node_id for n in secondaries))
        primary_B = await self.get_primary(secondaries)
        self.assertIsNot(primary_A, primary_B)
        self.assertEqual(primary_A.role, Role.PRIMARY)
        await primary_B.write(key=1, value=2)
        self.assertEqual(await primary_A.read(key=1, concern=concern), expected_result)

    async def test_read_concern_local_fails_read_your_writes(self):
        # Today, a client using the default writeConcern of w: "majority" and default
        # readConcern of "local" won't always read its writes. Soon after an election,
        # there could be a stale primary A which hasn't yet stepped down, and a fresh
        # primary B. The client could write to B, then read from A, and not see its
        # write.
        await self.read_from_stale_primary(concern=ReadConcern.LOCAL,
                                           expected_result=[])

    async def test_read_concern_majority_fails_linearizability(self):
        # Today, a client using w: "majority", rc: "majority" can fail to read its write
        # (hence fail linearizability) if it reads from a stale primary.
        await self.read_from_stale_primary(concern=ReadConcern.MAJORITY,
                                           expected_result=[])


if __name__ == '__main__':
    unittest.main()
