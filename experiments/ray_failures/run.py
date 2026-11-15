"""Phase 0 experiment, host side: break a worker, time what the caller sees.

Needs the cluster of docker/compose.yaml running. From the repository root:

    python experiments/ray_failures/run.py > experiments/ray_failures/results.jsonl

Each line of the output is one observation. See plan/06, section 3.
"""

import json
import subprocess
import sys
import time

COMPOSE = ["docker", "compose", "-f", "docker/compose.yaml"]
HEAD = "dpes-head"
WORKER = "dpes-worker-1"
NETWORK = "dpes_cluster"
WAIT_S = 180  # how long the caller waits before calling it silence
SETTLE_S = 45  # how long to watch the cluster after repairing the worker

# scenario -> (how to break the worker, how to repair it)
SCENARIOS = {
    "exit_code": (None, None),
    "die": (None, None),
    "kill": (["docker", "kill", WORKER], None),
    "pause": (["docker", "pause", WORKER], ["docker", "unpause", WORKER]),
    "disconnect": (
        ["docker", "network", "disconnect", NETWORK, WORKER],
        ["docker", "network", "connect", NETWORK, WORKER],
    ),
}


def quiet(command):
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL)


def in_head(code):
    return subprocess.check_output(
        ["docker", "exec", HEAD, "python", "-c", code], text=True
    ).strip()


def cluster_now():
    # The driver timestamps with the cluster clock, so the fault must too.
    return float(in_head("import time; print(time.time())"))


def worker_container():
    return subprocess.check_output(
        ["docker", "inspect", "-f", "{{.State.Status}}", WORKER], text=True
    ).strip()


def restore_cluster():
    quiet(["docker", "restart", WORKER])
    count = (
        "import ray; ray.init(address='auto', logging_level='ERROR');"
        "print(sum(n['Alive'] for n in ray.nodes()))"
    )
    for _ in range(40):
        if in_head(count).splitlines()[-1] == "3":
            return
        time.sleep(3)
    sys.exit("the cluster did not come back to three live nodes")


def run(scenario):
    fault, repair = SCENARIOS[scenario]
    settle_s = SETTLE_S if fault else 0
    driver = subprocess.Popen(
        COMPOSE
        + ["exec", "-T", "head", "python", "experiments/ray_failures/driver.py"]
        + [scenario, "worker-1", str(WAIT_S), str(settle_s)],
        stdout=subprocess.PIPE,
        text=True,
    )
    fault_t = None
    for line in driver.stdout:
        if not line.startswith("{"):
            continue
        event = {"scenario": scenario, **json.loads(line)}
        if event["event"] == "started" and fault:
            fault_t = cluster_now()
            quiet(fault)
        if event["event"] == "outcome":
            if fault_t:
                event["seconds_after_fault"] = round(event["t"] - fault_t, 1)
            event["worker_container"] = worker_container()
            if repair:
                quiet(repair)
        if event["event"] == "after_heal":
            event["worker_container"] = worker_container()
        print(json.dumps(event), flush=True)
    driver.wait()
    if fault:
        restore_cluster()


if __name__ == "__main__":
    for name in sys.argv[1:] or SCENARIOS:
        run(name)
