"""The LogStore actor on the cluster (plan/15, D15.2).

Run inside the control node:

    docker compose -f docker/compose.yaml exec -T head \
        python -m unittest discover -s tests/integration
"""

import http.client
import os
import socket
import tempfile
import threading
import time
import unittest

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from dpes.coordinator import BlobStore, LogStore, _Client, _StoreClient
from dpes.executor import execute
from dpes.gateway import make_server
from dpes.messages import Attempt

EID = "job-1/compile/0/1"


class _AttemptView:
    """What the gateway uses of the coordinator (plan/15, D15.15): the view
    that points the task at one attempt, and the calls that reach the real
    store through `_Client`. The coordinator actor takes no part here."""

    def __init__(self, client, logs, execution_id):
        self._client = client
        self._logs = logs  # only for a tail with a shorter idle limit
        self._execution_id = execution_id
        self._job_id = execution_id.split("/")[0]

    def status(self, job_id):
        if job_id != self._job_id:
            return None
        return {"job_id": job_id, "state": "RUNNING",
                "tasks": {"compile": {"state": "RUNNING",
                                      "execution_id": self._execution_id}}}

    def log_size(self, execution_id):
        return self._client.log_size(execution_id)

    def read_fragments(self, execution_id, offset, limit=None):
        return self._client.read_fragments(execution_id, offset, limit)

    def tail_fragments(self, execution_id, offset):
        # The default idle limit (300 s) would outlive the test. A short one
        # also lets a quiet attempt's tail end on its own, so the follower's
        # thread goes away instead of outliving the fixture.
        generator = self._logs.tail_fragments.remote(execution_id, offset,
                                                     0.05, 5.0)
        for ref in generator:
            yield ray.get(ref)


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
        self.assertEqual(5, ray.get(self.logs.log_size.remote(EID)))

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


class GatewayLogRoutingTest(StoreFixture):
    """The gateway's log route over the real store (plan/15, D15.4 and
    D15.15): an attempt's bytes reach an HTTP client, by offset and in vivo,
    and a client that leaves stops being followed."""

    def serve(self, job_id):
        """A gateway for `job_id`, whose only task is attempt 1 of compile."""
        execution_id = f"{job_id}/compile/0/1"
        self.server = make_server(
            _AttemptView(_Client(None, None, self.logs), self.logs,
                         execution_id),
            "127.0.0.1", 0)
        threading.Thread(target=self.server.serve_forever, daemon=True,
                         name="dpes-gateway").start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        return execution_id

    def get(self, job_id, query="", headers=None):
        connection = http.client.HTTPConnection(*self.server.server_address)
        self.addCleanup(connection.close)
        path = f"/jobs/{job_id}/tasks/compile/logs" + query
        connection.request("GET", path, headers=headers or {})
        response = connection.getresponse()
        return response.status, response.headers, response.read()

    def until_logged(self, caught, message, timeout_s=10):
        """The records of `message` once they appear: the handler that follows
        a log runs, and logs, in a thread of its own."""
        deadline = time.monotonic() + timeout_s
        while True:
            records = [r for r in caught.records if r.getMessage() == message]
            if records or time.monotonic() > deadline:
                return records
            time.sleep(0.05)

    def test_the_gateway_serves_the_bytes_the_attempt_wrote(self):
        execution_id = self.serve("job-log-1")
        self.append(1, b"one\n", execution_id)
        self.append(2, b"two\n", execution_id)

        status, headers, body = self.get("job-log-1", "?offset=4&limit=4")

        self.assertEqual((200, b"two\n"), (status, body))
        self.assertEqual("text/plain; charset=utf-8",
                         headers["Content-Type"])

    def test_a_follower_sees_the_output_as_the_attempt_writes_it(self):
        execution_id = self.serve("job-log-2")
        threading.Thread(target=self.write_over_time,
                         args=(execution_id,), daemon=True).start()

        status, headers, body = self.get("job-log-2", "?follow=1")

        self.assertEqual(200, status)
        self.assertEqual("chunked", headers["Transfer-Encoding"])
        # The end marker the executor writes ends the stream by itself.
        self.assertEqual(b"one\ntwo\n", body)

    def write_over_time(self, execution_id):
        """One fragment, then the other and the end: a live attempt."""
        self.append(1, b"one\n", execution_id)
        time.sleep(0.2)
        self.append(2, b"two\n", execution_id)
        ray.get(self.logs.close_fragments.remote(execution_id))

    def test_a_client_that_leaves_is_not_followed_any_more(self):
        execution_id = self.serve("job-log-3")
        connection = socket.create_connection(self.server.server_address,
                                              timeout=10)
        self.addCleanup(connection.close)
        connection.sendall(
            b"GET /jobs/job-log-3/tasks/compile/logs?follow=1 HTTP/1.1\r\n"
            b"Host: dpes\r\n\r\n"
        )
        # Once the head is out the handler is following, and the attempt is
        # silent: only the client leaving can free the handler before IDLE_S.
        head, _ = self.read_head(connection).split(b"\r\n\r\n", 1)
        self.assertIn(b"chunked", head)

        with self.assertLogs("dpes.gateway", "INFO") as caught:
            connection.close()
            began = time.monotonic()
            records = self.until_logged(caught, "log followed", timeout_s=10)
            elapsed = time.monotonic() - began

        self.assertTrue(records, "the handler never noticed the client left")
        self.assertEqual("client", records[0].fields["stopped"])
        self.assertLess(elapsed, 3)  # the tail itself would wait 300 s
        ray.get(self.logs.close_fragments.remote(execution_id))  # end the tail

    @staticmethod
    def read_head(connection):
        """Everything up to the blank line that ends the response head."""
        head = b""
        while b"\r\n\r\n" not in head:
            piece = connection.recv(4096)
            if not piece:
                break
            head += piece
        return head


if __name__ == "__main__":
    unittest.main()
