"""Phase 0 experiment, cluster side: what does the caller of a Ray task see?

Started by run.py inside the control node. Prints one JSON object per line.
See plan/06, section 3.
"""

import json
import os
import socket
import sys
import time

import ray


def emit(**event):
    print(json.dumps({"t": round(time.time(), 3), **event}), flush=True)


@ray.remote(num_cpus=0)
class Started:
    """Lets the task tell the driver that it is really running, and where."""

    def __init__(self):
        self.host = None

    def set(self, host):
        self.host = host

    def get(self):
        return self.host


@ray.remote(max_retries=0)
def task(mode, started):
    ray.get(started.set.remote(socket.gethostname()))
    if mode == "exit_code":
        return 1  # stands for a command that ended with an error
    if mode == "die":
        os._exit(1)
    time.sleep(3600)
    return "finished"


def outcome(ref, timeout):
    try:
        return {"returned": ray.get(ref, timeout=timeout)}
    except Exception as error:
        return {
            "raised": type(error).__name__,
            "bases": [base.__name__ for base in type(error).__mro__[1:-3]],
            "message": " ".join(str(error).split())[:240],
        }


def main(mode, resource, wait_s, settle_s):
    ray.init(address="auto", logging_level="ERROR")
    started = Started.remote()
    ref = task.options(resources={resource: 1}).remote(mode, started)

    host = None
    while host is None:
        time.sleep(0.2)
        host = ray.get(started.get.remote())
    emit(event="started", host=host)

    emit(event="outcome", **outcome(ref, wait_s))

    # run.py repairs the worker during this pause.
    time.sleep(settle_s)
    alive = sorted(n["NodeManagerHostname"] for n in ray.nodes() if n["Alive"])
    emit(event="after_heal", nodes_alive=alive, **outcome(ref, 5))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], float(sys.argv[3]), float(sys.argv[4]))
