import logging
import time
import sys

from omegaconf import DictConfig

from client import ClientLogEntry, client_read, client_write
from lease_guard import Network, Node, Role, setup_logging
from params import BASE_PARAMS
from prob import PRNG
from simulate import Timestamp, get_current_ts, get_event_loop, sleep

_logger = logging.getLogger("run")


async def reader(
    client_id: int,
    start_ts: Timestamp,
    nodes: list[Node],
    client_log: list[ClientLogEntry],
    prng: PRNG,
):
    await sleep(start_ts)
    primaries = [n for n in nodes if n.role == Role.PRIMARY]
    if not primaries:
        _logger.info(f"Reader {client_id} found no primary")
        return

    # Attempt to read from any primary.
    node = prng.choice(primaries)
    key = prng.random_key()
    _logger.info(f"Client {client_id} reading key {key} from {node}")
    entry = await client_read(node=node, key=key)
    if entry.success:
        _logger.info(f"Client {client_id} read key {key}={entry.value} from {node}"
                     f" ({entry.execution_ts})")
        client_log.append(entry)
    else:
        _logger.error(f"Failed to read key {key} from {node}: {entry.exception}")


async def writer(
    client_id: int,
    start_ts: Timestamp,
    nodes: list[Node],
    client_log: list[ClientLogEntry],
    prng: PRNG,
):
    await sleep(start_ts)
    primaries = [n for n in nodes if n.role == Role.PRIMARY]
    if not primaries:
        _logger.info(f"Writer {client_id} found no primary")
        return

    # Attempt to write to any primary.
    node = prng.choice(primaries)
    key = prng.random_key()
    _logger.info(f"Client {client_id} appending key {key}+={client_id} to {node}")
    entry = await client_write(node=node, key=key, value=client_id)
    client_log.append(entry)
    if entry.success:
        _logger.info(f"Client {client_id} appended key {key}+={client_id}"
                     f" ({entry.execution_ts})")
    else:
        _logger.error(
            f"Failed appending key {key}+={client_id} on {node}: {entry.exception}")


async def stepdown_nemesis(nodes: dict[int, Node],
                           prng: PRNG,
                           stepdown_rate: int):
    while True:
        await sleep(round(prng.exponential(stepdown_rate)))
        primaries = [n for n in nodes.values() if n.role is Role.PRIMARY]
        if not primaries:
            continue

        primary = prng.choice(primaries)
        _logger.info(f"Nemesis stepping down {primary}")
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


def do_linearizability_check(client_log: list[ClientLogEntry]) -> None:
    """Throw exception if "client_log" is not linearizable."""
    sys.setrecursionlimit(1000000)

    for entry in client_log:
        assert entry.start_ts <= entry.execution_ts
        assert entry.execution_ts <= entry.end_ts

    keys = list(sorted(set(e.key for e in client_log)))
    _logger.info(f"Checking linearizability for {len(keys)} keys")

    # We're omniscient, we know the absolute time each event occurred, so we don't need
    # a costly checking algorithm. Just sort by the execution times. In cases where lots
    # of writes have the same execution time (unavailability_experiment.py with
    # defer_commit_enabled, the new primary advances its commit point and commits many
    # writes at once), sorting also by start/end gives the checker a good start. The
    # closer to linearized is sorted_log, the faster the checker.
    sorted_log = sorted(client_log,
                        key=lambda e: (e.execution_ts, e.start_ts, e.end_ts))

    def linearize(log: list[ClientLogEntry],
                  value: list[int],
                  failed_writes: list[ClientLogEntry]) -> bool:
        """Try linearizing a suffix of the log with the KV store "model" in some state.

        Return a linearization if possible, else None.
        """
        if len(log) == 0:
            return True  # Empty history is already linearized.

        # If there are simultaneous events, try ordering any linearization of them.
        first_execution_ts = log[0].execution_ts
        for i, entry in enumerate(log):
            if entry.execution_ts != first_execution_ts:
                # We've tried all the entries with min exec times, linearization failed.
                break

            # Try linearizing "entry" at history's start. No other entry's end can
            # precede this entry's start.
            log_prime = log.copy()
            log_prime.pop(i)

            # Assume a failed write has no effect now, value' == value.
            if entry.op_type is ClientLogEntry.OpType.ListAppend and not entry.success:
                # While recursing, try letting the write take effect later.
                if linearize(log_prime, value, failed_writes + [entry]):
                    return True

            # Assume the write succeeded. Even if !success it might eventually commit.
            if entry.op_type is ClientLogEntry.OpType.ListAppend:
                # Try to linearize the rest of the log with this value.
                if linearize(log_prime, value + [entry.value], failed_writes):
                    return True

            if entry.op_type is ClientLogEntry.OpType.Read:
                # What would this query return if we ran it now?
                if value != entry.value:
                    continue  # "entry" can't be linearized first.

                # Try to linearize the rest of the log with this value.
                if linearize(log_prime, value, failed_writes):
                    return True

        # Linearization is failing so far, maybe one of the failed writes took effect?
        for f in failed_writes:
            failed_writes_prime = failed_writes.copy()
            failed_writes_prime.remove(f)
            if linearize(log, value + [f.value], failed_writes_prime):
                return True

        return False

    _logger.info("Checking linearizability")
    check_start = time.monotonic()
    for k in keys:
        _logger.info(f"Checking linearizability for key {k}:")
        # Ignore failed reads.
        filtered_log = [
            e for e in sorted_log if
            e.key == k and (e.op_type is ClientLogEntry.OpType.ListAppend or e.success)]
        for e in filtered_log:
            _logger.info(f"\t{e}")

        check_key_start = time.monotonic()
        result = linearize(log=filtered_log, value=[], failed_writes=[])
        if not result:
            _logger.info(f"Failed to linearize {len(filtered_log)} entries after"
                         f" {time.monotonic() - check_key_start:.2f} sec")

            raise Exception("not linearizable!")

    check_duration = time.monotonic() - check_start
    _logger.info(
        f"Linearization of {len(client_log)} entries took {check_duration:.2f} sec")


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


async def main_coro(params: DictConfig, jumpstart_election=False) -> dict:
    """Run the simulation, return metrics."""
    metrics = {}
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

    if jumpstart_election:
        nodes[1].become_candidate()

    while not any(n.commit_index >= 0 for n in nodes.values()):
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

    if params.stepdown_rate:
        lp.create_task(
            name="nemesis",
            coro=stepdown_nemesis(nodes=nodes, prng=prng,
                                  stepdown_rate=params.stepdown_rate)
        ).ignore_future()

    if params.partition_rate:
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

    return metrics


def main():
    logging.basicConfig(level=logging.INFO)
    event_loop = get_event_loop()
    metrics = event_loop.run_until_complete(event_loop.create_task("main", main_coro(
        params=BASE_PARAMS,
        jumpstart_election=BASE_PARAMS.get("jumpstart_election"))))
    _logger.info(f"metrics: {metrics}")


if __name__ == "__main__":
    main()
