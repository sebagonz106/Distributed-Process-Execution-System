"""The LogStore actor on the cluster (plan/15, D15.2).

Run inside the control node:

    docker compose -f docker/compose.yaml exec -T head \
        python -m unittest discover -s tests/integration
"""

import tempfile
import time
import unittest

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from dpes.coordinator import LogStore

EID = "job-1/compile/0/1"


class LogStoreTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Quiet: these check what the store returns; tests/host checks logs.
        ray.init(address="auto", logging_level="ERROR",
                 ignore_reinit_error=True, log_to_driver=False)
        cls.folder = tempfile.mkdtemp(prefix="dpes-logs-")
        cls.logs = cls.actor()

    @classmethod
    def actor(cls):
        """A store on this node over the folder the class shares, as on the
        control node (its folder is that node's volume)."""
        here = NodeAffinitySchedulingStrategy(
            ray.get_runtime_context().get_node_id(), soft=False
        )
        return LogStore.options(scheduling_strategy=here).remote(cls.folder)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def append(self, seq, data, execution_id=EID):
        return ray.get(self.logs.append_fragment.remote(
            execution_id, seq, data))

    def read(self, offset=0, limit=None, execution_id=EID):
        return ray.get(self.logs.read_fragments.remote(
            execution_id, offset, limit))

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


if __name__ == "__main__":
    unittest.main()
