"""The LogStore actor on the cluster (plan/15, D15.2).

Run inside the control node:

    docker compose -f docker/compose.yaml exec -T head \
        python -m unittest discover -s tests/integration
"""

import os
import tempfile
import time
import unittest

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from dpes.coordinator import BlobStore, LogStore, _StoreClient
from dpes.executor import execute
from dpes.messages import Attempt

EID = "job-1/compile/0/1"


class StoreFixture(unittest.TestCase):
    """The log store and the blob store of one class, on this node."""

    @classmethod
    def setUpClass(cls):
        # Quiet: these check what the store returns; tests/host checks logs.
        ray.init(address="auto", logging_level="ERROR",
                 ignore_reinit_error=True, log_to_driver=False)
        cls.folder = tempfile.mkdtemp(prefix="dpes-logs-")
        cls.logs = cls.actor()
        cls.blobs = BlobStore.options(
            scheduling_strategy=cls.on_this_node()
        ).remote(os.path.join(cls.folder, "blobs"),
                 os.path.join(cls.folder, "results.sqlite"))

    @staticmethod
    def on_this_node():
        """Stay on this node, as the control node's actors do: their folder is
        that node's volume."""
        return NodeAffinitySchedulingStrategy(
            ray.get_runtime_context().get_node_id(), soft=False
        )

    @classmethod
    def actor(cls):
        """A store on this node over the folder the class shares, as on the
        control node (its folder is that node's volume)."""
        return LogStore.options(
            scheduling_strategy=cls.on_this_node()
        ).remote(cls.folder)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def append(self, seq, data, execution_id=EID):
        return ray.get(self.logs.append_fragment.remote(
            execution_id, seq, data))

    def read(self, offset=0, limit=None, execution_id=EID):
        return ray.get(self.logs.read_fragments.remote(
            execution_id, offset, limit))


class LogStoreTest(StoreFixture):
    """What the actor stores, refuses, serves and recovers."""

    def test_a_fragment_written_through_the_actor_comes_back(self):
        self.assertTrue(self.append(1, b"hola\n"))

        self.assertEqual([(1, b"hola\n")], self.read())

    def test_a_duplicate_seq_is_ignored(self):
        execution_id = "job-2/compile/0/1"

        self.assertTrue(self.append(1, b"first\n", execution_id))
        self.assertFalse(self.append(1, b"again\n", execution_id))

        self.assertEqual([(1, b"first\n")],
                         self.read(execution_id=execution_id))

    def test_the_tail_streams_fragments_that_arrive_later(self):
        execution_id = "job-3/compile/0/1"
        stream = self.logs.tail_fragments.remote(execution_id, 0, 0.05, 0.3)

        self.append(1, b"one\n", execution_id)
        self.assertEqual((1, b"one\n"), ray.get(next(stream)))

        self.append(2, b"two\n", execution_id)
        self.assertEqual((2, b"two\n"), ray.get(next(stream)))
        self.assertEqual([], [ray.get(ref) for ref in stream])  # then idle

    def test_another_actor_on_the_same_folder_reads_what_was_written(self):
        execution_id = "job-4/compile/0/1"
        self.append(1, b"kept\n", execution_id)

        other = self.actor()
        try:
            self.assertEqual(
                [(1, b"kept\n")],
                ray.get(other.read_fragments.remote(execution_id, 0, None)),
            )
        finally:
            ray.kill(other)  # it starts and stops without the first one

    def test_the_store_comes_back_after_its_process_is_killed(self):
        execution_id = "job-5/compile/0/1"
        actor = self.actor()  # of its own, so the kill cannot disturb others
        try:
            self.assertTrue(ray.get(
                actor.append_fragment.remote(execution_id, 1, b"kept\n")))

            ray.kill(actor, no_restart=False)  # max_restarts brings it back

            pieces = self.until(actor, execution_id)
        finally:
            ray.kill(actor)

        self.assertEqual([(1, b"kept\n")], pieces)

    def until(self, actor, execution_id, timeout_s=30):
        """What the store holds, once it is back: the restart window refuses
        calls with `RayActorError`, and the caller retries (D15.12)."""
        deadline = time.monotonic() + timeout_s
        while True:
            try:
                return ray.get(
                    actor.read_fragments.remote(execution_id, 0, None))
            except ray.exceptions.RayActorError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.1)


class ExecutorEmissionTest(StoreFixture):
    """The executor shipping a real attempt's output, as a worker does
    (plan/15, D15.3 and D15.8): the fragments come out of the actor."""

    def run_executor(self, execution_id, script):
        attempt = Attempt(
            job_id=execution_id.split("/")[0], task="compile", round=0,
            execution_id=execution_id, command=("sh", "-c", script),
            env={}, inputs={}, outputs=(), timeout_s=10.0,
        )
        return execute(attempt, _StoreClient(self.blobs, self.logs))

    def test_the_output_of_an_attempt_reaches_the_store(self):
        execution_id = "job-6/compile/0/1"

        result = self.run_executor(execution_id, "echo one; echo two >&2")

        pieces = self.read(execution_id=execution_id)
        self.assertEqual(0, result.exit_code)
        self.assertEqual(b"one\ntwo\n", b"".join(data for _, data in pieces))

    def test_a_tail_ends_as_soon_as_the_attempt_does(self):
        execution_id = "job-7/compile/0/1"
        # An idle limit of a minute: only the end marker can close it sooner.
        stream = self.logs.tail_fragments.remote(execution_id, 0, 0.02, 60.0)

        self.run_executor(execution_id, "echo done")

        began = time.monotonic()
        self.assertEqual(b"done\n", b"".join(
            data for _, data in (ray.get(ref) for ref in stream)))
        self.assertLess(time.monotonic() - began, 30)


if __name__ == "__main__":
    unittest.main()
