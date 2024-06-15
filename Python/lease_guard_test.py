import copy
import inspect
import logging
import unittest
from unittest import TestCase

from omegaconf import DictConfig

from run_with_params import ClientLogEntry, do_linearizability_check
from lease_guard import Network, Node, ReadConcern, Role, setup_logging
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


TEST_CFG = DictConfig({
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
    "zipf_skewness": 4,
    "lease_enabled": False,
    "inherit_lease_enabled": True,
    "defer_commit_enabled": True,
    "log_write_micros": None,
    "lease_timeout": 1000,
    "seed": 1,
})


class LeaseRaftTest(SimulatorTestCase):
    async def replica_set_setup(self, **kwargs) -> None:
        self.cfg = copy.deepcopy(TEST_CFG)
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

        # Kickstart. If by bad luck we have multiple candidates it messes with tests.
        self.nodes[1].become_candidate()

    async def get_primary(self, nodes: set[Node] | None = None) -> Node:
        if nodes is None:
            nodes = set(self.nodes.values())

        return await await_predicate(
            lambda: next((n for n in nodes if n.role == Role.PRIMARY), None))

    async def read_from_stale_primary(self,
                                      concern: ReadConcern,
                                      expected_result: list[int],
                                      **kwargs) -> None:
        await self.replica_set_setup(**kwargs)
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
                                           lease_enabled=False)

    async def test_read_concern_majority_fails_linearizability(self):
        # Today, a client using w: "majority", rc: "majority" can fail to read its write
        # (hence fail linearizability) if it reads from a stale primary.
        await self.read_from_stale_primary(concern=ReadConcern.MAJORITY,
                                           expected_result=[],
                                           lease_enabled=False)

    async def test_rc_linearizable_is_linearizable_with_lease(self):
        with self.assertRaisesRegex(Exception, r"Not leaseholder"):
            await self.read_from_stale_primary(concern=ReadConcern.LINEARIZABLE,
                                               expected_result=[1],
                                               lease_enabled=True)

    async def test_read_concern_majority_upholds_linearizability(self):
        with self.assertRaisesRegex(Exception, r"Not leaseholder"):
            await self.read_from_stale_primary(concern=ReadConcern.MAJORITY,
                                               expected_result=[1],
                                               lease_enabled=True)

    async def test_has_lease(self):
        await self.replica_set_setup(lease_enabled=True,
                                     inherit_lease_enabled=True,
                                     noop_rate=1e10)
        # The primary writes a no-op when it's elected, but that isn't committed yet.
        primary = await self.get_primary()
        self.assertEqual(primary.commit_index, -1)
        # Has read lease if last committed entry is newer than lease timeout.
        self.assertFalse(primary.has_lease(for_writes=False))
        # Has write lease if no entry from prior term is newer than lease timeout.
        self.assertTrue(primary.has_lease(for_writes=True))
        # Wait for the no-op to be committed.
        await await_predicate(lambda: primary.commit_index > -1)
        self.assertTrue(primary.has_lease(for_writes=False))
        self.assertTrue(primary.has_lease(for_writes=True))
        await sleep(int(self.cfg.lease_timeout * (1 + primary.clock.max_clock_error)))
        # Can't read: last committed entry is older than lease timeout.
        self.assertFalse(primary.has_lease(for_writes=False))
        # Still has write lease: there are no entries from prior term.
        self.assertTrue(primary.has_lease(for_writes=True))

    async def test_read_with_prior_leader_lease(self):
        # Lease timeout > election timeout, so we have a stale leaseholder.
        await self.replica_set_setup(lease_enabled=True,
                                     election_timeout=1000,
                                     lease_timeout=20000)
        primary_A = await self.get_primary()
        # Make sure all nodes have commit_index > -1.
        await primary_A.write(key=0, value=0)
        await await_predicate(lambda: all(n.commit_index == primary_A.commit_index
                                          for n in self.nodes.values()))

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

    async def test_limbo_read(self):
        # Key 0, value 0 is appended on primary A and all nodes learn it's committed.
        #
        # Key 0, value 1 is appended on A and replicated to all nodes, but only A
        # learns it's committed before A is partitioned. Test that node B prevents
        # reading key 0 until B is sure it's committed. During the limbo period, write
        # to key 0 on B. This is blocked until the limbo ends.
        #
        # Key 1, value 0 is appended on A and all nodes learn it's committed. Test that
        # B can read key 1 using inherited lease.
        #
        # Key 2 is written to B before B gets a write lease. Test that key 2 can be
        # read on B before it's written, or after B gets a write lease, but not between.

        async def read(node: Node, key: int) -> list[int]:
            reply = await node.read(key=key, concern=ReadConcern.LINEARIZABLE)
            return reply.value

        # Lease timeout > election timeout, so we have a stale leaseholder.
        await self.replica_set_setup(lease_enabled=True,
                                     inherit_lease_enabled=True,
                                     defer_commit_enabled=True,
                                     election_timeout=1000,
                                     lease_timeout=20000)
        primary_A = await self.get_primary()

        await primary_A.write(key=0, value=0)
        await primary_A.write(key=1, value=0)
        await await_predicate(lambda: all(n.commit_index == primary_A.commit_index
                                          for n in self.nodes.values()))
        await primary_A.write(key=0, value=1)
        secondaries = set(n for n in self.nodes.values() if n.role == Role.SECONDARY)
        # Partition primary A from the majority, a new primary is elected.
        self.network.make_partition(
            set([primary_A.node_id]), set(n.node_id for n in secondaries))
        primary_B = await self.get_primary(secondaries)
        self.assertIsNot(primary_A, primary_B)
        self.assertEqual(primary_A.role, Role.PRIMARY)

        # Key 2 hasn't been written on B, we can read it.
        self.assertEqual(await read(primary_B, 2), [])
        # Key 0 is now in limbo, can't be read.
        read_key_0_task = get_event_loop().create_task(
            "read 0", primary_B.read(key=0, concern=ReadConcern.LINEARIZABLE))
        # Start writing - primary_B can't commit and acknowledge yet.
        write_key_0_task = get_event_loop().create_task(
            "0+=1", primary_B.write(key=0, value=2))
        write_key_2_task = get_event_loop().create_task(
            "2+=0", primary_B.write(key=2, value=0))
        # Although we've started to write key 2, we can still read the prior term value.
        self.assertEqual(await read(primary_B, 2), [])
        # Key 1 is consistent, we can read it.
        self.assertEqual(await read(primary_B, 1), [0])
        self.assertFalse(read_key_0_task.resolved)
        self.assertFalse(write_key_0_task.resolved)
        self.assertFalse(write_key_2_task.resolved)
        # Once primary_B commits an entry in its term, it can read all data.
        await await_predicate(lambda: primary_B.commit_index > primary_A.commit_index)
        await sleep(100)
        self.assertTrue(read_key_0_task.resolved)
        self.assertTrue(write_key_0_task.resolved)
        self.assertTrue(write_key_2_task.resolved)
        # The limbo read completes with the result of the primary_B write.
        self.assertEqual((await read_key_0_task).value, [0, 1, 2])
        self.assertEqual(1, primary_B.metrics.total("limbo_reads"))

    async def test_advance_commit_index(self):
        await self.replica_set_setup(lease_enabled=True,
                                     lease_timeout=10 * TEST_CFG.election_timeout,
                                     noop_rate=1e10)
        primary_A = await self.get_primary()
        await primary_A.write(key=1, value=1)
        commit_index = primary_A.commit_index
        await await_predicate(
            lambda: all(n.log == primary_A.log for n in self.nodes.values()))
        secondaries = set(n for n in self.nodes.values() if n.role == Role.SECONDARY)
        self.network.make_partition(
            set([primary_A.node_id]), set(n.node_id for n in secondaries))
        primary_B = await self.get_primary(secondaries)
        self.assertIsNot(primary_A, primary_B)
        # New primary waits for prior term's entries to be > lease timeout old.
        self.assertFalse(primary_B.has_lease(for_writes=True))
        self.assertLessEqual(primary_B.commit_index, commit_index)
        await sleep(int(self.cfg.lease_timeout * (1 + primary_A.clock.max_clock_error)))
        self.assertGreater(primary_B.commit_index, commit_index)
        self.assertTrue(primary_B.has_lease(for_writes=True))

    async def test_rollback(self):
        # Nothing to do with leases, just make sure rollback logic works.
        await self.replica_set_setup(lease_enabled=False)
        primary_A = await self.get_primary()
        await primary_A.write(key=1, value=1)
        secondaries = set(n for n in self.nodes.values() if n.role == Role.SECONDARY)

        # Partition primary A from the majority, a new primary is elected.
        self.network.make_partition(
            set([primary_A.node_id]), set(n.node_id for n in secondaries))
        write_task = self.loop.create_task(
            "stale primary write", primary_A.write(key=1, value=2))
        primary_B = await self.get_primary(secondaries)

        # Write to the new primary, heal the partition, old primary steps down.
        await primary_B.write(key=1, value=3)
        reply = await primary_B.read(key=1, concern=ReadConcern.MAJORITY)
        self.assertEqual(reply.value, [1, 3])
        self.network.reset_partition()
        with self.assertRaisesRegex(Exception, "Stepped down"):
            await write_task

        await await_predicate(lambda: primary_A.log == primary_B.log)

    async def test_inherit_lease_disabled(self):
        await self.replica_set_setup(
            lease_enabled=True,
            inherit_lease_enabled=False,
            lease_timeout=10 * TEST_CFG.election_timeout)
        primary_A = await self.get_primary()
        await primary_A.write(key=0, value=0)
        self.assertTrue(primary_A.has_lease(for_writes=True))
        self.assertTrue(primary_A.has_lease(for_writes=False))
        primary_B = next(n for n in self.nodes.values() if n != primary_A)
        primary_B.become_candidate()
        await await_predicate(lambda: primary_B.role == Role.PRIMARY)
        self.assertFalse(primary_B.has_lease(for_writes=True))
        # The next test would be True if inherit_lease_enabled.
        self.assertFalse(primary_B.has_lease(for_writes=False))
        await sleep(self.cfg.lease_timeout * (1 + primary_A.clock.max_clock_error))
        self.assertTrue(primary_B.has_lease(for_writes=True))
        self.assertTrue(primary_B.has_lease(for_writes=False))

    async def test_inherited_lease_read_is_linearizable(self):
        await self.replica_set_setup(
            lease_enabled=True,
            inherit_lease_enabled=True,
            defer_commit_enabled=False,  # simplifies test
            lease_timeout=10 * TEST_CFG.election_timeout,
            noop_rate=1e10)
        primary_A = await self.get_primary()
        # All nodes learn this write is committed.
        await primary_A.write(key=0, value=0)
        await await_predicate(lambda: all(n.commit_index == primary_A.commit_index
                                          for n in self.nodes.values()))
        # Only primary_A learns this write is committed.
        await primary_A.write(key=0, value=1)
        secondaries = set(n for n in self.nodes.values() if n.role == Role.SECONDARY)
        self.network.make_partition(
            set([primary_A.node_id]), set(n.node_id for n in secondaries))
        # Promote a secondary that hasn't learned the new commit index.
        primary_B = min(secondaries, key=lambda n: n.commit_index)
        self.assertLess(primary_B.commit_index, primary_A.commit_index)
        primary_B.become_candidate()
        await await_predicate(lambda: primary_B.role == Role.PRIMARY)
        # primary_B has primary_A's write, visible with read concern "local".
        self.assertEqual(
            [0, 1],
            (await primary_B.read(key=0, concern=ReadConcern.LOCAL)).value)
        # This read is in the "limbo" zone between primary_B's commit index and its
        # newest log entry. It waits for its commit index to catch up before reading.
        self.assertLess(primary_B.commit_index, primary_A.commit_index)
        self.assertEqual(
            [0, 1],
            (await primary_B.read(key=0, concern=ReadConcern.LINEARIZABLE)).value)
        self.assertGreater(primary_B.commit_index, primary_A.commit_index)


class LinearizabilityTest(unittest.TestCase):
    def test_invalid(self):
        with self.assertRaisesRegex(Exception, "not linearizable"):
            do_linearizability_check([
                ClientLogEntry(
                    op_type=ClientLogEntry.OpType.ListAppend,
                    start_ts=1,
                    execution_ts=2,
                    end_ts=3,
                    key=1,
                    value=1,
                    success=True
                ),
                ClientLogEntry(
                    op_type=ClientLogEntry.OpType.ListAppend,
                    start_ts=4,
                    execution_ts=5,
                    end_ts=6,
                    key=2,
                    value=1,
                    success=True
                ),
                ClientLogEntry(
                    op_type=ClientLogEntry.OpType.Read,
                    start_ts=6,
                    execution_ts=7,
                    end_ts=8,
                    key=1,
                    value=[2, 1],  # Wrong, should be [1, 2].
                    success=True
                )], debug=False)

    def test_simultaneous_events(self):
        event_1 = ClientLogEntry(
            op_type=ClientLogEntry.OpType.ListAppend,
            start_ts=1,
            execution_ts=2,
            end_ts=2,
            key=1,
            value=1,
            success=True
        )
        event_2 = ClientLogEntry(
            op_type=ClientLogEntry.OpType.ListAppend,
            start_ts=1,
            execution_ts=2,  # Same time as event_1.
            end_ts=2,
            key=1,
            value=2,
            success=True
        )
        read_event = ClientLogEntry(
            op_type=ClientLogEntry.OpType.Read,
            start_ts=3,
            execution_ts=4,
            end_ts=4,
            key=1,
            value=[1, 2],  # The value if event_1 happened first.
            success=True
        )
        log = [event_1, event_2, read_event]
        # History is valid if event_1 was first.
        do_linearizability_check(log, debug=False)
        read_event.value = [2, 1]  # The value if event_2 happened first.
        # History is valid if event_2 was first.
        do_linearizability_check(log, debug=False)

    def test_failed_write(self):
        failed_write = ClientLogEntry(
            op_type=ClientLogEntry.OpType.ListAppend,
            start_ts=1,
            execution_ts=2,
            end_ts=2,
            key=1,
            value=1,
            success=False
        )
        read_1 = ClientLogEntry(
            op_type=ClientLogEntry.OpType.Read,
            start_ts=3,
            execution_ts=4,
            end_ts=4,
            key=1,
            value=[],  # Doesn't see failed write.
            success=True
        )
        read_2 = ClientLogEntry(
            op_type=ClientLogEntry.OpType.Read,
            start_ts=5,
            execution_ts=6,
            end_ts=6,
            key=1,
            value=[1],  # Sees failed write.
            success=True
        )
        log = [failed_write, read_1, read_2]
        do_linearizability_check(log, debug=False)


if __name__ == '__main__':
    unittest.main()
