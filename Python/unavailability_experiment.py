import csv
import logging
import os.path

from omegaconf import DictConfig

from client import ClientLogEntry
from experiment import BASE_PARAMS
from lease_raft import Network, Node, Role, setup_logging
from prob import PRNG
from run_raft_with_params import reader, writer
from simulate import get_current_ts, get_event_loop, sleep

_logger = logging.getLogger("experiment")


async def experiment_coro(params: DictConfig) -> list[ClientLogEntry]:
    """Run the simulation, return the log."""
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

    nodes[1].become_candidate()  # Jumpstart election.

    while not any(n.commit_index >= 0 for n in nodes.values()):
        await sleep(100)

    lp = get_event_loop()
    client_log: list[ClientLogEntry] = []
    tasks = []
    start_ts = get_current_ts()
    # Schedule operations at regular intervals. 66% reads, 33% writes.
    for i in range(int(params.operations)):
        start_ts += params.interarrival
        if i % 3 == 0:
            tasks.append(lp.create_task(name=f"writer {i}", coro=writer(
                client_id=i,
                start_ts=start_ts,
                nodes=list(nodes.values()),
                client_log=client_log,
                prng=prng,
            )))
        else:
            tasks.append(lp.create_task(name=f"reader {i + 1}", coro=reader(
                client_id=i + 1,
                start_ts=start_ts,
                nodes=list(nodes.values()),
                client_log=client_log,
                prng=prng,
            )))

    assert not params.stepdown_rate, "We control stepdowns explicitly"

    async def stepdown_nemesis():
        """After a while, step down the primary and elect another node."""
        await sleep(5 * 1000 * 1000)
        primaries = [n for n in nodes.values() if n.role is Role.PRIMARY]
        assert len(primaries) == 1
        primary = primaries[0]
        _logger.info(f"Nemesis stepping down {primary}")
        # After election timeout, the same or a different node will step up.
        primary.stepdown()

    tasks.append(lp.create_task("stepdown", coro=stepdown_nemesis()))

    for t in tasks:
        await t

    _logger.info(f"Finished after {get_current_ts()} ms (simulated)")
    return client_log


def main():
    logging.basicConfig(level=logging.INFO)
    event_loop = get_event_loop()
    csv_writer: None | csv.DictWriter = None
    csv_path = f"metrics/{os.path.splitext(os.path.basename(__file__))[0]}.csv"
    csv_file = open(csv_path, "w+")
    raw_params = BASE_PARAMS.copy()
    NUM_OPERATIONS = 5000
    raw_params.update({
        "election_timeout": 10 * 1000 * 1000,  # 10 seconds.
        "heartbeat_rate": 400 * 1000,  # 0.4 seconds.
        "lease_timeout": 20 * 1000 * 1000,  # 20 seconds.
        "operations": NUM_OPERATIONS,
        # Ensure operations continue before, during, after lease interregnum.
        "interarrival": (raw_params.lease_timeout * 3) // NUM_OPERATIONS,
        "keyspace_size": 1000,
    })

    _logger.info(f"Parameters: {raw_params}")
    _logger.info(f"Expected simulated duration per experiment:"
                 f" {raw_params.operations * raw_params.interarrival}")

    for leases_enabled, read_lease_optimization_enabled in [
        (False, False), (True, False), (True, True)
    ]:
        params = raw_params.copy()
        params.update({
            "leases_enabled": leases_enabled,
            "read_lease_optimization_enabled": read_lease_optimization_enabled})

        log = event_loop.run_until_complete(event_loop.create_task(
            "main", experiment_coro(params=params)))

        stats = [{
            "leases_enabled": leases_enabled,
            "read_lease_optimization_enabled": read_lease_optimization_enabled,
            "absolute_ts": entry.absolute_ts,
            "writes": 1 if entry.op_type is ClientLogEntry.OpType.ListAppend else 0,
            "reads": 1 if entry.op_type is ClientLogEntry.OpType.Read else 0,
        } for entry in log]

        if csv_writer is None:
            csv_writer = csv.DictWriter(csv_file, fieldnames=stats[0].keys())
            csv_writer.writeheader()

        csv_writer.writerows(stats)
        event_loop.reset()

    csv_file.close()
    _logger.info(csv_path)


if __name__ == "__main__":
    main()
