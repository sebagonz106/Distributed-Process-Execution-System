"""Coordinator: the Ray actor that owns the scheduler (plan/07, D07.7).

The actor is a thin shell around `scheduler.Job`: it turns the launches the
state machine asks for into Ray tasks, keeps each attempt's deadlines
(plan/09, D09.4), feeds back what it observed, classified (D09.1), and
describes all of it in the logs (plan/10). Before it hands an attempt out, it
asks the store whether that work already succeeded, and serves the stored
result instead (plan/16, D16.3). Each call it makes to the state machine goes
to a journal first; when Ray restarts the actor, replaying the journal rebuilds
it (plan/14).
"""

import asyncio
import dataclasses
import logging
import os
import time
import uuid

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from dpes.cas import BlobDir
from dpes.failures import DEADLINE, RESTARTED, classify
from dpes.hashing import action_key, digest
from dpes.log import event, setup
from dpes.logstore import IDLE_S, POLL_S, LogDir, LogTruncated, tail
from dpes.messages import ExecutorFailure, TaskResult
from dpes.scheduler import Job, Launch, State
from dpes.spec import SpecError, demand, unschedulable
from dpes.store import Journal, ResultLog

NAMESPACE = "dpes"
NAME = "coordinator"
CALL_TIMEOUT_S = 10
STORE_NAME = "store"
STORE_ROOT = "/var/lib/dpes/blobs"  # a Docker volume on the control node
RESULTS_PATH = "/var/lib/dpes/results.sqlite"  # same volume (plan/12)
JOURNAL_PATH = "/var/lib/dpes/coordinator.sqlite"  # same volume (plan/14)
LOG_NAME = "logs"
LOG_ROOT = "/var/lib/dpes/logs"  # same volume (plan/15, D15.2)
STORE_TIMEOUT_S = 60  # a 16 MiB blob crosses the network
QUEUE_TIMEOUT_S = 3600.0  # launch to start signal (D09.4)
RUN_MARGIN_S = 5.0  # past the task's own time limit, silence is a failure
# What the work runs on: the toolchain of the workers' image, not this host
# (plan/16, D16.1, D16.7). Keys of another toolchain must never match, so this
# goes up whenever the image's compilers change.
PLATFORM_ID = os.environ.get("DPES_PLATFORM_ID", "linux/amd64:gcc")
_FINISHED = {State.SUCCEEDED, State.FAILED, State.ERROR}
# The state machine method that each kind of journal entry calls (plan/14).
_METHODS = {"running": "running", "result": "on_result",
            "failure": "on_failure", "finished": "finished"}
_log = logging.getLogger(__name__)
_log_store = logging.getLogger("dpes.logstore")


class _Clock:
    """Wall-clock time for the events of the state machine (D15.16).

    Live, it is `time.time`. While a journaled call is being made or
    replayed, it is the time recorded with that call: the events a restart
    rebuilds keep the time they happened, not the time of the restart.
    """

    def __init__(self):
        self.at = None

    def __call__(self):
        return time.time() if self.at is None else self.at


@ray.remote(num_cpus=0)  # the real resources are set per launch
def _attempt(execute, attempt, coordinator, store, logs):
    """One attempt as a Ray task: say it started, then run it."""
    setup()  # this worker process logs like every other (D10.7)
    coordinator.started.remote(attempt.execution_id)
    return execute(attempt, _StoreClient(store, logs))


@ray.remote(num_cpus=0, max_restarts=0)
class BlobStore:
    """The store on the control node: content-addressed blobs (plan/11,
    D11.5) and confirmed results, first writer wins (plan/12, D12.1), which
    are also the content cache (plan/16, D16.2)."""

    def __init__(self, root, results_path):
        setup()
        self._blobs = BlobDir(root)
        self._results = ResultLog(results_path)
        event(logging.getLogger("dpes.store"), logging.INFO, "store started",
              blobs=root, results=results_path)

    def put(self, name, data):
        return self._blobs.put(name, data)

    def get(self, name):
        return self._blobs.get(name)

    def missing(self, names):
        return self._blobs.missing(names)

    def save_result(self, key, result, cache_key=None):
        return self._results.save(key, result, cache_key)

    def result(self, key):
        return self._results.get(key)

    def cached(self, cache_key):
        """The confirmed success registered under `cache_key`, or None."""
        return self._results.get_by_cache_key(cache_key)


# It rebuilds its index from disk, so Ray may restart it (D15.12); async, so a
# tail that waits gives the event loop back and appends are served meanwhile.
@ray.remote(num_cpus=0, max_restarts=-1)
class LogStore:
    """The log store on the control node: each attempt's output in numbered
    fragments, on disk under the data volume (plan/15, D15.2). A worker's
    death cannot take away what already arrived here (F5.4)."""

    def __init__(self, root, max_bytes=None):
        setup()
        self._logs = LogDir(root, max_bytes)
        event(_log_store, logging.INFO,
              "log store started", root=root,
              max_mib=self._logs.limit >> 20)

    def append_fragment(self, execution_id, seq, data):
        """Store `data` as fragment `seq`; False if that seq already is.
        Raises `LogTruncated` when the attempt is full, with its own event."""
        try:
            return self._logs.append(execution_id, seq, data)
        except LogTruncated as full:
            event(_log_store, logging.WARNING, "log truncated",
                  execution=execution_id, side="control", bytes=full.size,
                  limit=full.limit, seq=seq)
            raise

    def read_fragments(self, execution_id, offset, limit=None):
        """(seq, data) pieces covering bytes [offset, offset+limit)."""
        return self._logs.read(execution_id, offset, limit)

    def close_fragments(self, execution_id):
        """Say the attempt wrote its last fragment (D15.14)."""
        return self._logs.close(execution_id)

    def log_size(self, execution_id):
        """Bytes of output stored for the attempt (D15.15)."""
        return self._logs.size(execution_id)

    def drop(self, execution_ids):
        """Delete the logs of attempts a finished job no longer serves;
        returns the bytes freed (D15.13)."""
        return self._logs.drop(execution_ids)

    async def tail_fragments(self, execution_id, offset, poll_s=POLL_S,
                             idle_s=IDLE_S):
        """(seq, data) as they arrive, for a client following the log."""
        async for piece in tail(self._logs, execution_id, offset,
                                poll_s, idle_s):
            yield piece


class _StoreClient:
    """What the executor gets: the blob store and the log store, as plain
    calls. It is the only thing that knows where they are (D15.5)."""

    def __init__(self, actor, logs):
        self._actor = actor
        self._logs = logs

    def get(self, name):
        return ray.get(self._actor.get.remote(name), timeout=STORE_TIMEOUT_S)

    def put(self, data):
        reply = self._actor.put.remote(digest(data), data)
        return ray.get(reply, timeout=STORE_TIMEOUT_S)

    def save_result(self, key, result, cache_key=None):
        reply = self._actor.save_result.remote(key, result, cache_key)
        return ray.get(reply, timeout=CALL_TIMEOUT_S)

    def append_fragment(self, execution_id, seq, data):
        reply = self._logs.append_fragment.remote(execution_id, seq, data)
        return ray.get(reply, timeout=CALL_TIMEOUT_S)

    def close_fragments(self, execution_id):
        reply = self._logs.close_fragments.remote(execution_id)
        return ray.get(reply, timeout=CALL_TIMEOUT_S)


# When its process dies, Ray starts it again and it rebuilds itself from its
# journal (plan/14, D14.3).
@ray.remote(num_cpus=0, max_restarts=-1)
class Coordinator:
    """Async actor: awaiting an attempt yields, so status calls stay served.

    `execute` takes an Attempt and returns a TaskResult or an ExecutorFailure.
    """

    def __init__(self, execute, store, logs, journal_path,
                 queue_timeout_s=QUEUE_TIMEOUT_S, platform_id=PLATFORM_ID):
        setup()
        self._execute = execute
        self._store = store
        self._logs = logs
        self._queue_timeout_s = queue_timeout_s
        self._platform_id = platform_id
        self._me = ray.get_runtime_context().current_actor
        self._jobs = {}
        self._by_key = {}  # idempotency key -> job id (plan/12, D12.7)
        # job id -> monotonic time, until it finishes; None if recovered
        self._submitted_at = {}
        self._started = {}  # execution id -> event set by the start signal
        self._in_flight = set()  # asyncio keeps only weak references to tasks
        self._clock = _Clock()  # stamps the events of each journaled call
        self._journal = Journal(journal_path)
        launched = self._replay()
        # A crash between a result and the end of the job leaves a journal
        # without its `finished` call: record it now, so the feed of a job
        # that is over ends where the job did (D15.17).
        for job in self._jobs.values():
            if job.state in _FINISHED and not job.done:
                self._submitted_at[job.job_id] = None
                self._log_if_finished(job)
        unfinished = [job for job in self._jobs.values()
                      if job.state not in _FINISHED]
        self._orphans = [(job, attempt, attempt.execution_id in launched)
                         for job in unfinished for attempt in job.unsettled()]
        for job in unfinished:
            self._submitted_at[job.job_id] = None
        event(_log, logging.INFO, "coordinator started",
              queue_timeout_s=queue_timeout_s, jobs=len(self._jobs),
              unfinished=len(unfinished), platform=platform_id)
        if unfinished:
            self._me.recover.remote()  # runs once this constructor returns

    async def submit(self, spec, key=None) -> str:
        """Start `spec` as a new job, unless `key` already started it."""
        # No await before the key is recorded: two submissions with the same
        # key cannot interleave here.
        if key in self._by_key:
            known = self._jobs[self._by_key[key]]
            if known.spec != spec:
                raise ValueError(
                    f"idempotency key {key!r} was used for a different job"
                )
            event(_log, logging.INFO, "job resubmitted", job=known.job_id,
                  key=key)
            return known.job_id
        job_id = uuid.uuid4().hex
        # Journaled before replying: from the journal on, the job exists.
        at = self._journal.append(job_id, "accepted", spec, key)
        job = self._jobs[job_id] = Job(job_id, spec, clock=self._clock)
        if key:
            self._by_key[key] = job_id
        self._submitted_at[job_id] = time.monotonic()
        launches = self._journaled(at, job.start)
        event(_log, logging.INFO, "job started", job=job_id,
              tasks=len(spec.tasks), ready=len(launches))
        self._launch(job, launches)
        return job_id

    async def recover(self):
        """Settle or relaunch what the previous instance left in flight
        (plan/14, D14.4)."""
        orphans, self._orphans = self._orphans, []
        for job, attempt, launched in orphans:
            event(_log, logging.WARNING, "attempt orphaned by a restart",
                  execution=attempt.execution_id, launched=launched)
            if launched:  # it may have run: reconcile, else a WORKER failure
                self._spawn(self._settle(job, attempt, RESTARTED))
            else:
                self._launch(job, [Launch(attempt)])

    async def status(self, job_id: str):
        job = self._jobs.get(job_id)
        return job.view() if job else None

    async def events(self, job_id: str):
        """The job's events, oldest first (D15.5); None if there is no job.

        Read from the state machine's in-memory feed, which the journal
        rebuilds after a restart (D15.16).
        """
        job = self._jobs.get(job_id)
        return [dataclasses.asdict(e) for e in job.events()] if job else None

    async def started(self, execution_id: str):
        event_ = self._started.get(execution_id)
        if event_:  # absent: the attempt was already given up
            event(_log, logging.DEBUG, "attempt started",
                  execution=execution_id)
            event_.set()

    def _replay(self):
        """Rebuild every job from the journal (plan/14, D14.1); return the
        execution ids that were launched. Each call is made at the time it was
        recorded, so its events keep that time (D15.16)."""
        launched = set()
        for job_id, kind, args, at in self._journal.events():
            if kind == "accepted":
                spec, key = args
                job = self._jobs[job_id] = Job(job_id, spec, clock=self._clock)
                self._journaled(at, job.start)
                if key:
                    self._by_key[key] = job_id
                continue
            if kind == "running":
                launched.add(args[0])
            self._journaled(at, getattr(self._jobs[job_id], _METHODS[kind]),
                            *args)
        return launched

    def _apply(self, job, kind, *args):
        """Journal a call to the state machine, then make it: whatever the
        outside sees follows from the journal (plan/14, D14.2)."""
        at = self._journal.append(job.job_id, kind, *args)
        return self._journaled(at, getattr(job, _METHODS[kind]), *args)

    def _journaled(self, at, call, *args):
        """Make `call` with the events it records stamped `at` (D15.16)."""
        self._clock.at = at
        try:
            return call(*args)
        finally:
            self._clock.at = None

    def _launch(self, job, launches):
        for launch in launches:
            self._spawn(self._run(job, launch))

    def _spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        self._in_flight.add(task)
        task.add_done_callback(self._in_flight.discard)

    async def _run(self, job, launch):
        # Every path ends in an event: an exception lost in this background
        # task would leave the job RUNNING forever.
        attempt = launch.attempt
        if launch.delay_s:
            await asyncio.sleep(launch.delay_s)
        key = self._action_key(job, attempt)
        if key and not launch.retry:  # the round may already have succeeded
            cached = await self._cached(attempt, key)
            if cached is not None:
                await self._settle(job, attempt, cached, hit=True,
                                   note=f"cache hit: {cached.execution_id}")
                return
        # Journaled before the task exists: from here on the attempt is ours.
        self._apply(job, "running", attempt.execution_id)
        try:
            outcome = await self._outcome(job, attempt, key)
        except _Silent:
            observed = DEADLINE
        except Exception as error:  # Ray reports that there is no result
            observed = type(error).__name__
        else:
            if isinstance(outcome, (TaskResult, ExecutorFailure)):
                observed = outcome
            else:  # the executor broke its contract
                observed = f"executor returned {type(outcome).__name__}"
        await self._settle(job, attempt, observed)

    def _action_key(self, job, attempt):
        """The key of this attempt's work, or "" if it may not use the cache.

        Computed here, from what the attempt already carries: the inputs it
        resolved (dependency outputs included, which chains the keys), its
        command, its environment and its declared outputs (plan/16, D16.1).
        """
        if not job.spec.tasks[attempt.task].cache:  # F9.5: the task opted out
            return ""
        return action_key(attempt.command, attempt.env, attempt.inputs,
                          attempt.outputs, self._platform_id)

    async def _cached(self, attempt, key):
        """The success already stored for `key`, or None (plan/16, D16.3).

        A store that does not answer is a miss: the cache may save work, it
        never holds a job up.
        """
        reply = self._store.cached.remote(key)
        try:
            return await asyncio.wait_for(
                asyncio.wrap_future(reply.future()), CALL_TIMEOUT_S
            )
        except Exception as error:
            event(_log, logging.WARNING, "cache lookup failed",
                  execution=attempt.execution_id,
                  error=type(error).__name__)
            return None

    async def _settle(self, job, attempt, observed, note="", hit=False):
        """Give the state machine what was observed of `attempt`, then
        launch what that unblocks."""
        eid = attempt.execution_id
        if not isinstance(observed, TaskResult):
            # The task may have ended and been saved, with only its answer
            # lost: ask the store before calling it a failure (D12.4).
            saved = await self._confirmed(attempt)
            if saved is not None:
                note = f"adopted by reconciliation after {_describe(observed)}"
                event(_log, logging.INFO, "result adopted by reconciliation",
                      execution=eid, producer=saved.execution_id,
                      after=_describe(observed))
                observed = saved

        if isinstance(observed, TaskResult) and observed.succeeded:
            if hit:  # nothing ran, so there is no attempt to speak of
                event(_log, logging.INFO, "cache hit", execution=eid,
                      original=observed.execution_id)
            else:
                event(_log, logging.INFO, "attempt succeeded", execution=eid)
            unblocked = self._apply(job, "result", observed, eid, note)
        elif isinstance(observed, TaskResult):
            event(_log, logging.WARNING, "attempt failed", execution=eid,
                  failure_class=classify(observed).value,
                  exit_code=observed.exit_code, timed_out=observed.timed_out)
            unblocked = self._apply(job, "result", observed, eid, note)
        else:
            failure_class = classify(observed)
            reason = _describe(observed)
            event(_log, logging.WARNING, "attempt failed", execution=eid,
                  failure_class=failure_class.value, reason=reason)
            unblocked = self._apply(job, "failure", eid, failure_class, reason)

        for retry in (l for l in unblocked if l.attempt.task == attempt.task):
            event(_log, logging.WARNING, "retry scheduled",
                  execution=retry.attempt.execution_id, after=eid,
                  delay_s=round(retry.delay_s, 3))
        self._launch(job, unblocked)
        self._log_if_finished(job)

    async def _confirmed(self, attempt):
        """The result saved under the attempt's key, or None (D12.4)."""
        reply = self._store.result.remote(attempt.result_key)
        try:
            return await asyncio.wait_for(
                asyncio.wrap_future(reply.future()), CALL_TIMEOUT_S
            )
        except Exception as error:  # store unreachable: the failure stands
            event(_log, logging.WARNING, "reconciliation failed",
                  execution=attempt.execution_id,
                  error=type(error).__name__)
            return None

    async def _outcome(self, job, attempt, key=""):
        """What the attempt returned. Raises _Silent past its deadlines, or
        whatever Ray raised."""
        eid = attempt.execution_id
        started = self._started[eid] = asyncio.Event()
        task = job.spec.tasks[attempt.task]
        # The key travels with the attempt so the executor writes it in the
        # same statement that confirms the result (plan/16, D16.6).
        dispatched = (dataclasses.replace(attempt, cache_key=key) if key
                      else attempt)
        ref = _attempt.options(max_retries=0, **_placement(task)).remote(
            self._execute, dispatched, self._me, self._store, self._logs
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
        """Record that the job is over, once (D15.17), and log it."""
        state = job.state
        if state not in _FINISHED:
            return
        if not job.done:
            self._apply(job, "finished")  # journaled: a restart replays it
        if job.job_id not in self._submitted_at:
            return
        started = self._submitted_at.pop(job.job_id)
        took = {} if started is None else {  # None: recovered (plan/14)
            "duration_s": round(time.monotonic() - started, 3)}
        counts = {}
        for task in job.view()["tasks"].values():
            counts[task["state"].lower()] = counts.get(
                task["state"].lower(), 0) + 1
        level = logging.INFO if state is State.SUCCEEDED else logging.ERROR
        event(_log, level, "job finished", job=job.job_id, state=state.value,
              **took, **counts)
        self._spawn(self._sweep(job))

    async def _sweep(self, job):
        """Free the logs of the attempts this finished job no longer serves
        (plan/15, D15.13). No journal: it is space, not state, and it is
        idempotent; a store that cannot take it leaks, it never fails a job."""
        tasks = job.view()["tasks"]
        keep = {task["execution_id"] for task in tasks.values()
                if task["execution_id"]}
        every = {attempt["execution_id"] for task in tasks.values()
                 for attempt in task["attempts"]}
        dropped = sorted(every - keep)
        if not dropped:
            return
        reply = self._logs.drop.remote(dropped)
        try:
            freed = await asyncio.wait_for(
                asyncio.wrap_future(reply.future()), CALL_TIMEOUT_S
            )
        except Exception as error:  # the store or the way to it failed
            event(_log, logging.WARNING, "log sweep failed", job=job.job_id,
                  attempts=len(dropped), error=type(error).__name__)
            return
        event(_log, logging.INFO, "logs swept", job=job.job_id,
              attempts=len(dropped), freed_bytes=freed)


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

    def __init__(self, actor, store, logs):
        self._actor = actor
        self._store = store
        self._logs = logs

    def submit(self, spec, key=None):
        # Refused here, not queued forever in Ray (plan/08, D08.3).
        live = [node["Resources"] for node in ray.nodes() if node["Alive"]]
        errors = unschedulable(spec, live)
        wanted = [(name, path, blob) for name, task in spec.tasks.items()
                  for path, blob in task.inputs.items()]
        absent = set(ray.get(
            self._store.missing.remote([blob for _, _, blob in wanted]),
            timeout=CALL_TIMEOUT_S,
        ))
        errors += [f"tasks.{name}.inputs.{path}: {blob} was never uploaded"
                   for name, path, blob in wanted if blob in absent]
        if errors:
            raise SpecError(errors)
        reply = self._actor.submit.remote(spec, key)
        try:
            return ray.get(reply, timeout=CALL_TIMEOUT_S)
        except ray.exceptions.RayTaskError as error:
            if isinstance(error.cause, ValueError):  # reused key (D12.7)
                raise SpecError([str(error.cause)]) from None
            raise

    def status(self, job_id):
        reply = self._actor.status.remote(job_id)
        return ray.get(reply, timeout=CALL_TIMEOUT_S)

    def events(self, job_id):
        """The job's event feed, oldest first, or None (plan/15, D15.5)."""
        reply = self._actor.events.remote(job_id)
        return ray.get(reply, timeout=CALL_TIMEOUT_S)

    def put_blob(self, name, data):
        reply = self._store.put.remote(name, data)
        return ray.get(reply, timeout=STORE_TIMEOUT_S)

    def get_blob(self, name):
        return ray.get(self._store.get.remote(name), timeout=STORE_TIMEOUT_S)

    def read_fragments(self, execution_id, offset, limit=None):
        reply = self._logs.read_fragments.remote(execution_id, offset, limit)
        return ray.get(reply, timeout=CALL_TIMEOUT_S)

    def log_size(self, execution_id):
        """Bytes of output an attempt has stored so far (D15.15)."""
        reply = self._logs.log_size.remote(execution_id)
        return ray.get(reply, timeout=CALL_TIMEOUT_S)

    def tail_fragments(self, execution_id, offset):
        """(seq, data) as the attempt writes them, for `logs -f` (D15.4)."""
        generator = self._logs.tail_fragments.remote(execution_id, offset)
        for ref in generator:  # Ray stays here: the gateway sees plain data
            yield ray.get(ref)


def connect() -> _Client:
    """Join the cluster and get, or create, the coordinator on this node."""
    from dpes.executor import execute

    ray.init(address="auto", namespace=NAMESPACE, logging_level="ERROR")
    here = NodeAffinitySchedulingStrategy(
        ray.get_runtime_context().get_node_id(), soft=False
    )
    store = BlobStore.options(
        name=STORE_NAME,
        lifetime="detached",
        get_if_exists=True,
        scheduling_strategy=here,  # its folder is this node's volume (D11.5)
    ).remote(STORE_ROOT, RESULTS_PATH)
    logs = LogStore.options(
        name=LOG_NAME,
        lifetime="detached",  # its files outlive a gateway restart (D15.2)
        get_if_exists=True,
        scheduling_strategy=here,  # its folder is this node's volume
    ).remote(LOG_ROOT)
    actor = Coordinator.options(
        name=NAME,
        lifetime="detached",  # survives a gateway restart
        get_if_exists=True,
        scheduling_strategy=here,
    ).remote(execute, store, logs, JOURNAL_PATH)
    return _Client(actor, store, logs)
