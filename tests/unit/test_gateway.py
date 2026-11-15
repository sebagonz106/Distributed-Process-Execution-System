import contextlib
import http.client
import io
import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from dpes import cli
from dpes.gateway import MAX_BODY, make_server
from dpes.spec import JobSpec

HELLO_JOB = {"tasks": {"hello": {"command": ["echo", "hi"]}}}


class FakeCoordinator:
    """In-memory stand-in for the Ray actor (plan/07, D07.9)."""

    def __init__(self):
        self.submitted = []
        self.views = {}  # job id -> snapshots returned in order, last repeats
        self.broken = False

    def submit(self, spec):
        if self.broken:
            raise ConnectionError("coordinator unreachable")
        self.submitted.append(spec)
        return "job-1"

    def status(self, job_id):
        if self.broken:
            raise ConnectionError("coordinator unreachable")
        snapshots = self.views.get(job_id)
        if not snapshots:
            return None
        return snapshots.pop(0) if len(snapshots) > 1 else snapshots[0]


def view(state):
    return {"job_id": "job-1", "state": state, "tasks": {}}


class ServerTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # One server per class: stopping one waits for its poll interval.
        cls.server = make_server(None, "127.0.0.1", 0)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        self.coordinator = self.server.coordinator = FakeCoordinator()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def request(self, method, path, body=None):
        request = urllib.request.Request(self.url + path, body, method=method)
        try:
            with urllib.request.urlopen(request) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            with error:
                return error.code, json.load(error)


class GatewayTest(ServerTestCase):
    def post_job(self, document):
        return self.request("POST", "/jobs", json.dumps(document).encode())

    def test_a_valid_job_is_accepted_and_handed_over_validated(self):
        self.assertEqual((201, {"job_id": "job-1"}), self.post_job(HELLO_JOB))
        self.assertIsInstance(self.coordinator.submitted[0], JobSpec)

    def test_an_invalid_job_is_rejected_with_every_error(self):
        document = {"tasks": {"hello": {"command": [], "timeout_s": 0}}}

        status, body = self.post_job(document)

        self.assertEqual(400, status)
        self.assertEqual(2, len(body["errors"]))
        self.assertEqual([], self.coordinator.submitted)

    def test_a_body_that_is_not_json_is_rejected(self):
        status, body = self.request("POST", "/jobs", b"{not json")

        self.assertEqual(400, status)
        self.assertIn("errors", body)

    def test_a_body_over_the_limit_is_refused_unread(self):
        # Only the headers are sent: the answer must come without the body.
        connection = http.client.HTTPConnection(*self.server.server_address)
        self.addCleanup(connection.close)
        connection.putrequest("POST", "/jobs")
        connection.putheader("Content-Length", str(MAX_BODY + 1))
        connection.endheaders()

        self.assertEqual(413, connection.getresponse().status)

    def test_the_status_of_a_known_job_is_returned(self):
        self.coordinator.views["job-1"] = [view("RUNNING")]

        self.assertEqual((200, view("RUNNING")), self.request("GET", "/jobs/job-1"))

    def test_an_unknown_job_is_not_found(self):
        self.assertEqual(404, self.request("GET", "/jobs/nope")[0])

    def test_an_unknown_route_is_not_found(self):
        self.assertEqual(404, self.request("GET", "/other")[0])
        self.assertEqual(404, self.request("POST", "/other", b"{}")[0])

    def test_an_unreachable_coordinator_is_reported_as_unavailable(self):
        self.coordinator.broken = True

        self.assertEqual(503, self.post_job(HELLO_JOB)[0])
        self.assertEqual(503, self.request("GET", "/jobs/job-1")[0])


class CliTest(ServerTestCase):
    def run_cli(self, *args):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli.main(["--url", self.url, *args])
        return code, out.getvalue(), err.getvalue()

    def write_job(self, document):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = os.path.join(directory.name, "job.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(document, handle)
        return path

    def test_submit_prints_the_job_id(self):
        code, out, _ = self.run_cli("submit", self.write_job(HELLO_JOB))

        self.assertEqual((0, "job-1\n"), (code, out))

    def test_a_rejected_job_prints_the_errors(self):
        code, _, err = self.run_cli("submit", self.write_job({"tasks": {}}))

        self.assertEqual(cli.EXIT_REQUEST_FAILED, code)
        self.assertIn("exactly one task", err)

    def test_status_prints_the_job_as_json(self):
        self.coordinator.views["job-1"] = [view("RUNNING")]

        code, out, _ = self.run_cli("status", "job-1")

        self.assertEqual((0, view("RUNNING")), (code, json.loads(out)))

    def test_wait_exits_with_a_code_for_each_way_of_ending(self):
        for final, expected in (("SUCCEEDED", 0), ("FAILED", 1), ("ERROR", 2)):
            with self.subTest(final=final):
                self.coordinator.views["job-1"] = [
                    view("PENDING"), view("RUNNING"), view(final)
                ]

                code, out, _ = self.run_cli("wait", "job-1", "--interval", "0")

                self.assertEqual(expected, code)
                self.assertEqual(final, json.loads(out)["state"])

    def test_an_unknown_job_is_a_failed_request(self):
        code, _, err = self.run_cli("status", "nope")

        self.assertEqual(cli.EXIT_REQUEST_FAILED, code)
        self.assertIn("404", err)

    def test_an_unreachable_gateway_is_a_failed_request(self):
        self.url = "http://127.0.0.1:9"  # nothing listens there

        code, _, err = self.run_cli("status", "job-1")

        self.assertEqual(cli.EXIT_REQUEST_FAILED, code)
        self.assertIn("gateway", err)


if __name__ == "__main__":
    unittest.main()
