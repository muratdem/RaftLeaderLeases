import csv
import logging
import os.path

from omegaconf import DictConfig

from client import ClientLogEntry
from params import BASE_PARAMS
from lease_guard import Network, Node, Role, setup_logging
from prob import PRNG
from run_with_params import reader, writer
from simulate import get_current_ts, get_event_loop, sleep

_logger = logging.getLogger("experiment")

NUM_OPERATIONS = 6500
# Long lease to explore inherited read lease optimization.
LEASE_TIMEOUT = 2 * BASE_PARAMS.election_timeout
# Ensure operations continue before, during, after lease interregnum.
INTERARRIVAL = (LEASE_TIMEOUT * 3) // NUM_OPERATIONS
# Replication is only a little faster than client write throughput, so we can see the
# effect of the deferred commit optimization.
LOG_WRITE_SPEED_RATIO = 0.7

PARAMS = BASE_PARAMS.copy()
PARAMS.update({
    # Trigger the stepdown nemesis part way into the experiment.
    "stepdown_time": LEASE_TIMEOUT // 2,
    "stepup_time": LEASE_TIMEOUT // 2 + BASE_PARAMS.election_timeout,
    "lease_timeout": LEASE_TIMEOUT,
    # We'll trigger a step-up manually, for reliability.
    "election_timeout": int(1e7),
    "heartbeat_rate": LEASE_TIMEOUT // 2,
    "operations": NUM_OPERATIONS,
    # Ensure operations continue before, during, after lease interregnum.
    "interarrival": 300,
    # SSD I/O latency.
    "log_write_micros": 250,
    "keyspace_size": 1000,
    "zipf_skewness": 1.5,
    "seed": 1,
    "max_clock_error": 0,  # Less variation between sub-experiments.
})

SUB_EXPERIMENT_PARAMS = []
for lease_enabled, inherit_lease_enabled, defer_commit_enabled, title in [
    (False, False, False, "no leases"),
    (True, False, False, "unoptimized\nleases"),
    (True, True, False, "inherited\nread lease"),
    (True, True, True, "deferred\ncommit"),
]:
    sub_exp_params = PARAMS.copy()
    sub_exp_params.update({
        "lease_enabled": lease_enabled,
        "inherit_lease_enabled": inherit_lease_enabled,
        "defer_commit_enabled": defer_commit_enabled,
        "title": title,
    })
    SUB_EXPERIMENT_PARAMS.append(sub_exp_params)


def get_primary(nodes: dict[int, Node]) -> Node:
    primaries = [n for n in nodes.values() if n.role is Role.PRIMARY]
    assert len(primaries) == 1
    return primaries[0]


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
        """After a while, step down the primary."""
        await sleep(params.stepdown_time)
        primary = get_primary(nodes)
        _logger.info(f"Nemesis stepping down {primary}")
        # After election timeout, the same or a different node will step up.
        primary.stepdown()

    async def stepup_task():
        """After a while, manually step up a node."""
        await sleep(params.stepup_time)
        assert not any(n for n in nodes.values() if n.role is Role.PRIMARY)
        secondary = nodes[2]
        _logger.info(f"Nemesis stepping up {secondary}")
        # After election timeout, the same or a different node will step up.
        secondary.become_candidate()

    lp = get_event_loop()
    tasks = [lp.create_task("stepdown", coro=stepdown_nemesis()),
             lp.create_task("stepup", coro=stepup_task())]

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
            tasks.append(lp.create_task(name=f"reader {i}", coro=reader(
                client_id=i,
                start_ts=start_ts + jitter,
                nodes=list(nodes.values()),
                client_log=client_log,
                prng=prng,
            )))

    assert not params.stepdown_rate, "We control stepdowns explicitly"

    for t in tasks:
        await t

    _logger.info(f"Finished after {get_current_ts()} ms (simulated)")
    primary = get_primary(nodes)
    _logger.info(f"{primary.metrics.total('limbo_reads')}"
                 f"/{primary.metrics.sample_count('limbo_reads')}"
                 f" reads were limbo reads from {primary}")
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
            "lease_enabled": sub_exp_params.lease_enabled,
            "inherit_lease_enabled": sub_exp_params.inherit_lease_enabled,
            "defer_commit_enabled": sub_exp_params.defer_commit_enabled,
            "end_ts": entry.end_ts,
            "writes": 1 if entry.op_type is ClientLogEntry.OpType.ListAppend else 0,
            "reads": 1 if entry.op_type is ClientLogEntry.OpType.Read else 0,
        } for entry in client_log if entry.success]

        if csv_writer is None:
            csv_writer = csv.DictWriter(csv_file, fieldnames=stats[0].keys())
            csv_writer.writeheader()

        csv_writer.writerows(stats)
        event_loop.reset()

    csv_file.close()
    _logger.info(csv_path)


if __name__ == "__main__":
    main()
