"""Coordinator: the Ray actor that owns the scheduler (plan/07, D07.7).

The actor is a thin shell around `scheduler.Job`: it turns the attempts the
state machine asks for into Ray tasks and feeds back what Ray reports.
"""

import asyncio
import uuid

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from dpes.messages import ExecutorFailure, TaskResult
from dpes.scheduler import Job

NAMESPACE = "dpes"
NAME = "coordinator"
CALL_TIMEOUT_S = 10


@ray.remote(num_cpus=0, max_restarts=0)
class Coordinator:
    """Async actor: awaiting an attempt yields, so status calls stay served.

    `executor` is a Ray remote function taking an Attempt and returning a
    TaskResult or an ExecutorFailure.
    """

    def __init__(self, executor):
        self._executor = executor
        self._jobs = {}
        self._in_flight = set()  # asyncio keeps only weak references to tasks

    async def submit(self, spec) -> str:
        job = Job(uuid.uuid4().hex, spec)
        self._jobs[job.job_id] = job
        for attempt in job.start():
            watcher = asyncio.create_task(self._run(job, attempt))
            self._in_flight.add(watcher)
            watcher.add_done_callback(self._in_flight.discard)
        return job.job_id

    async def status(self, job_id: str):
        job = self._jobs.get(job_id)
        return job.view() if job else None

    async def _run(self, job, attempt):
        # Every path ends in an event: an exception lost in this background
        # task would leave the job RUNNING forever.
        eid = attempt.execution_id
        launch = self._executor.options(max_retries=0)
        try:
            outcome = await launch.remote(attempt)
        except Exception as error:  # Ray reports that there is no result
            job.on_failure(eid, type(error).__name__)
            return
        if isinstance(outcome, TaskResult):
            job.on_result(outcome)
        elif isinstance(outcome, ExecutorFailure):
            job.on_failure(eid, f"{outcome.evidence.value}: {outcome.detail}")
        else:
            job.on_failure(eid, f"executor returned {type(outcome).__name__}")


class _Client:
    """What the gateway calls: blocking calls with a deadline."""

    def __init__(self, actor):
        self._actor = actor

    def submit(self, spec):
        return ray.get(self._actor.submit.remote(spec), timeout=CALL_TIMEOUT_S)

    def status(self, job_id):
        reply = self._actor.status.remote(job_id)
        return ray.get(reply, timeout=CALL_TIMEOUT_S)


def connect() -> _Client:
    """Join the cluster and get, or create, the coordinator on this node."""
    from dpes.executor import execute

    ray.init(address="auto", namespace=NAMESPACE, logging_level="ERROR")
    here = NodeAffinitySchedulingStrategy(
        ray.get_runtime_context().get_node_id(), soft=False
    )
    actor = Coordinator.options(
        name=NAME,
        lifetime="detached",  # survives a gateway restart
        get_if_exists=True,
        scheduling_strategy=here,
    ).remote(ray.remote(execute))
    return _Client(actor)
