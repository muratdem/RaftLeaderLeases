import csv
import logging
import os.path

from omegaconf import DictConfig

from client import ClientLogEntry
from params import BASE_PARAMS
from lease_raft import Network, Node, Role, setup_logging
from prob import PRNG
from run_raft_with_params import reader, writer
from simulate import get_current_ts, get_event_loop, sleep

_logger = logging.getLogger("experiment")

NUM_OPERATIONS = 10 * 1000
# Long lease to explore read-lease optimization.
LEASE_TIMEOUT = 2 * BASE_PARAMS.election_timeout
# Ensure operations continue before, during, after lease interregnum.
INTERARRIVAL = (LEASE_TIMEOUT * 3) // NUM_OPERATIONS
# Replication is only a little faster than client write throughput, so we can see the
# effect of the speculative write optimization.
LOG_WRITE_SPEED_RATIO = 0.7

PARAMS = BASE_PARAMS.copy()
PARAMS.update({
    "lease_timeout": LEASE_TIMEOUT,
    "operations": NUM_OPERATIONS,
    # Ensure operations continue before, during, after lease interregnum.
    "interarrival": (LEASE_TIMEOUT * 3) // NUM_OPERATIONS,
    "interrival": INTERARRIVAL,
    # Slow replication so new leader must catch up gradually.
    "log_write_micros": int(LOG_WRITE_SPEED_RATIO * INTERARRIVAL),
    # No effect on performance stats, but keeps lists from growing too long.
    "keyspace_size": NUM_OPERATIONS,
    # Trigger the stepdown nemesis 1/3 of the way into the experiment.
    # Set each 1/3 of the experiment to the lease timeout.
    "stepdown_time": LEASE_TIMEOUT,
})

SUB_EXPERIMENT_PARAMS = []
for leases_enabled, read_lease_opt_enabled, speculative_write_opt_enabled, title in [
    (False, False, False, "Throughput without leases"),
    (True, False, False, "Throughput without read lease optimization"),
    (True, True, False, "Throughput with read lease optimization"),
    (True, True, True, "Throughput with speculative writes"),
]:
    sub_exp_params = PARAMS.copy()
    sub_exp_params.update({
        "leases_enabled": leases_enabled,
        "read_lease_opt_enabled": read_lease_opt_enabled,
        "speculative_write_opt_enabled": speculative_write_opt_enabled,
        "title": title,
    })
    SUB_EXPERIMENT_PARAMS.append(sub_exp_params)


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

    async def stepdown_nemesis():
        """After a while, step down the primary and elect another node."""
        await sleep(params.stepdown_time)
        primaries = [n for n in nodes.values() if n.role is Role.PRIMARY]
        assert len(primaries) == 1
        primary = primaries[0]
        _logger.info(f"Nemesis stepping down {primary}")
        # After election timeout, the same or a different node will step up.
        primary.stepdown()

    lp = get_event_loop()
    tasks = []
    tasks.append(lp.create_task("stepdown", coro=stepdown_nemesis()))

    while not any(n.commit_index >= 0 for n in nodes.values()):
        await sleep(100)

    client_log: list[ClientLogEntry] = []
    start_ts = get_current_ts()
    # Schedule operations at regular intervals. 66% reads, 33% writes.
    for i in range(int(params.operations)):
        start_ts += params.interarrival
        jitter = prng.randint(-params.interarrival // 2, params.interarrival // 2)
        if i % 3 == 0:
            tasks.append(lp.create_task(name=f"writer {i}", coro=writer(
                client_id=i,
                start_ts=start_ts + jitter,
                nodes=list(nodes.values()),
                client_log=client_log,
                prng=prng,
            )))
        else:
            tasks.append(lp.create_task(name=f"reader {i + 1}", coro=reader(
                client_id=i + 1,
                start_ts=start_ts + jitter,
                nodes=list(nodes.values()),
                client_log=client_log,
                prng=prng,
            )))

    assert not params.stepdown_rate, "We control stepdowns explicitly"

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

    _logger.info(f"Expected simulated duration per experiment:"
                 f" {PARAMS.operations * PARAMS.interarrival}")

    for sub_exp_params in SUB_EXPERIMENT_PARAMS:
        client_log = event_loop.run_until_complete(event_loop.create_task(
            "main", experiment_coro(params=sub_exp_params)))

        stats = [{
            "leases_enabled": sub_exp_params.leases_enabled,
            "read_lease_opt_enabled": sub_exp_params.read_lease_opt_enabled,
            "speculative_write_opt_enabled":
                sub_exp_params.speculative_write_opt_enabled,
            "execution_ts": entry.execution_ts,
            "writes": 1 if entry.op_type is ClientLogEntry.OpType.ListAppend else 0,
            "reads": 1 if entry.op_type is ClientLogEntry.OpType.Read else 0,
        } for entry in client_log]

        if csv_writer is None:
            csv_writer = csv.DictWriter(csv_file, fieldnames=stats[0].keys())
            csv_writer.writeheader()

        csv_writer.writerows(stats)
        event_loop.reset()

    csv_file.close()
    _logger.info(csv_path)


if __name__ == "__main__":
    main()
