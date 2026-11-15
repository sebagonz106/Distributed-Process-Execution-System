"""Coordinator: the Ray actor that owns the scheduler (plan/07, D07.7).

The actor is a thin shell around `scheduler.Job`: it turns the launches the
state machine asks for into Ray tasks, keeps each attempt's deadlines
(plan/09, D09.4), feeds back what it observed, classified (D09.1), and
describes all of it in the logs (plan/10).
"""

import asyncio
import logging
import time
import uuid

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from dpes.failures import DEADLINE, classify
from dpes.log import event, setup
from dpes.messages import ExecutorFailure, TaskResult
from dpes.scheduler import Job, State
from dpes.spec import SpecError, demand, unschedulable

NAMESPACE = "dpes"
NAME = "coordinator"
CALL_TIMEOUT_S = 10
QUEUE_TIMEOUT_S = 3600.0  # launch to start signal (D09.4)
RUN_MARGIN_S = 5.0  # past the task's own time limit, silence is a failure
_FINISHED = {State.SUCCEEDED, State.FAILED, State.ERROR}
_log = logging.getLogger(__name__)


@ray.remote(num_cpus=0)  # the real resources are set per launch
def _attempt(execute, attempt, coordinator):
    """One attempt as a Ray task: say it started, then run it."""
    setup()  # this worker process logs like every other (D10.7)
    coordinator.started.remote(attempt.execution_id)
    return execute(attempt)


@ray.remote(num_cpus=0, max_restarts=0)
class Coordinator:
    """Async actor: awaiting an attempt yields, so status calls stay served.

    `execute` takes an Attempt and returns a TaskResult or an ExecutorFailure.
    """

    def __init__(self, execute, queue_timeout_s=QUEUE_TIMEOUT_S):
        setup()
        self._execute = execute
        self._queue_timeout_s = queue_timeout_s
        self._me = ray.get_runtime_context().current_actor
        self._jobs = {}
        self._submitted_at = {}  # job id -> monotonic time, until it finishes
        self._started = {}  # execution id -> event set by the start signal
        self._in_flight = set()  # asyncio keeps only weak references to tasks
        event(_log, logging.INFO, "coordinator started",
              queue_timeout_s=queue_timeout_s)

    async def submit(self, spec) -> str:
        job = Job(uuid.uuid4().hex, spec)
        self._jobs[job.job_id] = job
        self._submitted_at[job.job_id] = time.monotonic()
        launches = job.start()
        event(_log, logging.INFO, "job started", job=job.job_id,
              tasks=len(spec.tasks), ready=len(launches))
        self._launch(job, launches)
        return job.job_id

    async def status(self, job_id: str):
        job = self._jobs.get(job_id)
        return job.view() if job else None

    async def started(self, execution_id: str):
        event_ = self._started.get(execution_id)
        if event_:  # absent: the attempt was already given up
            event(_log, logging.DEBUG, "attempt started",
                  execution=execution_id)
            event_.set()

    def _launch(self, job, launches):
        for launch in launches:
            watcher = asyncio.create_task(self._run(job, launch))
            self._in_flight.add(watcher)
            watcher.add_done_callback(self._in_flight.discard)

    async def _run(self, job, launch):
        # Every path ends in an event: an exception lost in this background
        # task would leave the job RUNNING forever.
        attempt = launch.attempt
        eid = attempt.execution_id
        if launch.delay_s:
            await asyncio.sleep(launch.delay_s)
        job.running(eid)
        try:
            outcome = await self._outcome(job, attempt)
        except _Silent:
            observed = DEADLINE
        except Exception as error:  # Ray reports that there is no result
            observed = type(error).__name__
        else:
            if isinstance(outcome, (TaskResult, ExecutorFailure)):
                observed = outcome
            else:  # the executor broke its contract
                observed = f"executor returned {type(outcome).__name__}"

        if isinstance(observed, TaskResult) and observed.succeeded:
            event(_log, logging.INFO, "attempt succeeded", execution=eid)
            unblocked = job.on_result(observed)
        elif isinstance(observed, TaskResult):
            event(_log, logging.WARNING, "attempt failed", execution=eid,
                  failure_class=classify(observed).value,
                  exit_code=observed.exit_code, timed_out=observed.timed_out)
            unblocked = job.on_result(observed)
        else:
            failure_class = classify(observed)
            reason = _describe(observed)
            event(_log, logging.WARNING, "attempt failed", execution=eid,
                  failure_class=failure_class.value, reason=reason)
            unblocked = job.on_failure(eid, failure_class, reason)

        for retry in (l for l in unblocked if l.attempt.task == attempt.task):
            event(_log, logging.WARNING, "retry scheduled",
                  execution=retry.attempt.execution_id, after=eid,
                  delay_s=round(retry.delay_s, 3))
        self._launch(job, unblocked)
        self._log_if_finished(job)

    async def _outcome(self, job, attempt):
        """What the attempt returned. Raises _Silent past its deadlines, or
        whatever Ray raised."""
        eid = attempt.execution_id
        started = self._started[eid] = asyncio.Event()
        task = job.spec.tasks[attempt.task]
        ref = _attempt.options(max_retries=0, **_placement(task)).remote(
            self._execute, attempt, self._me
        )
        event(_log, logging.INFO, "attempt launched", execution=eid,
              resources=_compact(task.resources))
        result = asyncio.wrap_future(ref.future())
        signal = asyncio.ensure_future(started.wait())
        try:
            await asyncio.wait(
                {result, signal},
                timeout=self._queue_timeout_s,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not result.done() and started.is_set():
                await asyncio.wait(
                    {result}, timeout=attempt.timeout_s + RUN_MARGIN_S
                )
            if result.done():
                return result.result()
            event(_log, logging.WARNING, "attempt silent past its deadline",
                  execution=eid,
                  deadline="run" if started.is_set() else "queue")
            # Give up on it, and do not let its late end be logged as unseen.
            result.add_done_callback(lambda f: f.cancelled() or f.exception())
            try:
                ray.cancel(ref, force=True)
            except Exception:
                pass  # best effort: a lost node cannot be reached anyway
            raise _Silent()
        finally:
            signal.cancel()
            self._started.pop(eid, None)

    def _log_if_finished(self, job):
        state = job.state
        if state not in _FINISHED or job.job_id not in self._submitted_at:
            return
        took = time.monotonic() - self._submitted_at.pop(job.job_id)
        counts = {}
        for task in job.view()["tasks"].values():
            counts[task["state"].lower()] = counts.get(
                task["state"].lower(), 0) + 1
        level = logging.INFO if state is State.SUCCEEDED else logging.ERROR
        event(_log, level, "job finished", job=job.job_id, state=state.value,
              duration_s=round(took, 3), **counts)


class _Silent(Exception):
    """An attempt said nothing before its deadline (plan/09, D09.4)."""


def _describe(observed) -> str:
    if isinstance(observed, ExecutorFailure):
        return f"{observed.evidence.value}: {observed.detail}"
    return observed


def _compact(resources) -> str:
    return ",".join(f"{key}:{amount:g}" for key, amount in resources.items())


def _placement(task):
    """Ray task options asking for what `task` needs (plan/08, D08.3)."""
    wanted = demand(task)
    options = {"num_cpus": wanted.pop("CPU")}
    if "memory" in wanted:
        options["memory"] = wanted.pop("memory")
    if wanted:
        options["resources"] = wanted
    return options


class _Client:
    """What the gateway calls: blocking calls with a deadline."""

    def __init__(self, actor):
        self._actor = actor

    def submit(self, spec):
        # Refused here, not queued forever in Ray (plan/08, D08.3).
        live = [node["Resources"] for node in ray.nodes() if node["Alive"]]
        errors = unschedulable(spec, live)
        if errors:
            raise SpecError(errors)
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
    ).remote(execute)
    return _Client(actor)
