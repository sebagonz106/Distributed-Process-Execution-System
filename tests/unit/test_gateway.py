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
from unittest import mock

from dpes import cli
from dpes.gateway import MAX_BLOB, MAX_BODY, make_server
from dpes.hashing import digest
from dpes.spec import JobSpec, SpecError

HELLO_JOB = {"tasks": {"hello": {"command": ["echo", "hi"]}}}


class FakeCoordinator:
    """In-memory stand-in for the Ray actor (plan/07, D07.9)."""

    def __init__(self):
        self.submitted = []
        self.views = {}  # job id -> snapshots returned in order, last repeats
        self.broken = False
        self.unschedulable = []  # errors the cluster check would report
        self.keys = []  # idempotency key of each submission (plan/12)
        self.blobs = {}

    def submit(self, spec, key=None):
        self.keys.append(key)
        if self.broken:
            raise ConnectionError("coordinator unreachable")
        if self.unschedulable:
            raise SpecError(self.unschedulable)
        absent = [f"{name}: {d} never uploaded"
                  for name, task in spec.tasks.items()
                  for d in task.inputs.values() if d not in self.blobs]
        if absent:
            raise SpecError(absent)
        self.submitted.append(spec)
        return "job-1"

    def put_blob(self, name, data):
        self.blobs[name] = data

    def get_blob(self, name):
        return self.blobs.get(name)

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

    def test_a_job_no_node_can_run_is_rejected(self):
        self.coordinator.unschedulable = ["tasks.hello.resources: no node"]

        status, body = self.post_job(HELLO_JOB)

        self.assertEqual((400, ["tasks.hello.resources: no node"]),
                         (status, body.get("errors")))

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


class BlobTest(ServerTestCase):
    def raw(self, method, path, body=None):
        request = urllib.request.Request(self.url + path, body, method=method)
        try:
            with urllib.request.urlopen(request) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            with error:
                return error.code, error.read()

    def test_a_blob_put_under_its_digest_is_got_back(self):
        name = digest(b"hola")

        self.assertEqual(201, self.raw("PUT", f"/blobs/{name}", b"hola")[0])
        self.assertEqual((200, b"hola"), self.raw("GET", f"/blobs/{name}"))

    def test_content_that_does_not_match_its_digest_is_refused(self):
        status, _ = self.raw("PUT", f"/blobs/{digest(b'x')}", b"hola")

        self.assertEqual(400, status)
        self.assertEqual({}, self.coordinator.blobs)

    def test_a_blob_over_the_limit_is_refused_unread(self):
        connection = http.client.HTTPConnection(*self.server.server_address)
        self.addCleanup(connection.close)
        connection.putrequest("PUT", f"/blobs/{digest(b'big')}")
        connection.putheader("Content-Length", str(MAX_BLOB + 1))
        connection.endheaders()

        self.assertEqual(413, connection.getresponse().status)

    def test_an_unknown_or_malformed_blob_is_not_found(self):
        self.assertEqual(404, self.raw("GET", f"/blobs/{digest(b'no')}")[0])
        self.assertEqual(404, self.raw("GET", "/blobs/../etc/passwd")[0])

    def test_a_job_whose_inputs_were_never_uploaded_is_rejected(self):
        job = {"tasks": {"t": {"command": ["true"],
                               "inputs": {"a.c": digest(b"never")}}}}

        status, _ = self.request("POST", "/jobs", json.dumps(job).encode())

        self.assertEqual(400, status)


class IdempotencyKeyTest(ServerTestCase):
    """plan/12, D12.7."""

    def post_with_key(self, key):
        request = urllib.request.Request(
            self.url + "/jobs", json.dumps(HELLO_JOB).encode(),
            headers={"Idempotency-Key": key}, method="POST",
        )
        try:
            with urllib.request.urlopen(request) as response:
                return response.status
        except urllib.error.HTTPError as error:
            with error:
                return error.code

    def test_the_key_reaches_the_coordinator(self):
        self.assertEqual(201, self.post_with_key("retry-safe_1"))
        self.assertEqual(["retry-safe_1"], self.coordinator.keys)

    def test_a_job_sent_without_a_key_has_none(self):
        self.request("POST", "/jobs", json.dumps(HELLO_JOB).encode())

        self.assertEqual([None], self.coordinator.keys)

    def test_a_malformed_key_is_rejected_before_submitting(self):
        for key in ("has space", "x" * 65, "a/b"):
            with self.subTest(key=key):
                self.assertEqual(400, self.post_with_key(key))
        self.assertEqual([], self.coordinator.keys)

    def test_the_client_sends_a_key_and_retries_with_the_same_one(self):
        job_file = CliTest.write_job(self, HELLO_JOB)
        answers = [cli.GatewayUnreachable("refused"), b'{"job_id": "job-1"}']
        with mock.patch.object(cli, "_request", side_effect=answers) as sent:
            with mock.patch.object(cli.time, "sleep"):
                code = cli.main(["--url", self.url, "submit", job_file])

        keys = [call.kwargs["headers"]["Idempotency-Key"]
                for call in sent.call_args_list]
        self.assertEqual(0, code)
        self.assertEqual(2, len(keys))
        self.assertEqual(keys[0], keys[1])

    def test_the_client_gives_up_after_three_tries(self):
        job_file = CliTest.write_job(self, HELLO_JOB)
        down = cli.GatewayUnreachable("refused")
        with mock.patch.object(cli, "_request", side_effect=[down] * 3):
            with mock.patch.object(cli.time, "sleep"):
                with contextlib.redirect_stderr(io.StringIO()):
                    code = cli.main(["--url", self.url, "submit", job_file])

        self.assertEqual(cli.EXIT_REQUEST_FAILED, code)


class GatewayLogTest(ServerTestCase):
    def logged(self, level, *request):
        with self.assertLogs("dpes.gateway", level) as caught:
            self.request(*request)
        return caught.records

    def test_an_accepted_job_is_logged_with_its_id(self):
        (record,) = self.logged("INFO", "POST", "/jobs",
                                json.dumps(HELLO_JOB).encode())

        self.assertEqual("job accepted", record.getMessage())
        self.assertEqual({"job": "job-1", "tasks": 1}, record.fields)

    def test_a_rejected_job_is_a_warning_naming_the_first_error(self):
        body = json.dumps({"tasks": {}}).encode()

        (record,) = self.logged("WARNING", "POST", "/jobs", body)

        self.assertEqual("job rejected", record.getMessage())
        self.assertIn("at least one task", record.fields["first"])

    def test_an_unreachable_coordinator_is_an_error(self):
        self.coordinator.broken = True

        (record,) = self.logged("ERROR", "GET", "/jobs/job-1")

        self.assertEqual("coordinator unavailable", record.getMessage())

    def test_an_idempotency_key_is_logged_with_the_job(self):
        request = urllib.request.Request(
            self.url + "/jobs", json.dumps(HELLO_JOB).encode(),
            headers={"Idempotency-Key": "retry-7"}, method="POST",
        )
        with self.assertLogs("dpes.gateway", "INFO") as caught:
            urllib.request.urlopen(request).close()

        (record,) = caught.records
        self.assertEqual("retry-7", record.fields["idempotency_key"])

    def test_a_downloaded_blob_is_logged(self):
        name = digest(b"bin")
        self.coordinator.blobs[name] = b"bin"

        with self.assertLogs("dpes.gateway", "INFO") as caught:
            urllib.request.urlopen(f"{self.url}/blobs/{name}").close()

        (record,) = caught.records
        self.assertEqual(("blob served", name, 3),
                         (record.getMessage(), record.fields["digest"],
                          record.fields["bytes"]))

    def test_status_polling_stays_below_info(self):
        self.coordinator.views["job-1"] = [view("RUNNING")]

        with self.assertNoLogs("dpes.gateway", "INFO"):
            self.request("GET", "/jobs/job-1")


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
        self.assertIn("at least one task", err)

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

    def test_submit_uploads_the_listed_inputs_and_sends_their_digests(self):
        path = self.write_job({"tasks": {"t": {"command": ["cat", "a.txt"],
                                                 "inputs": ["a.txt"]}}})
        with open(os.path.join(os.path.dirname(path), "a.txt"), "wb") as f:
            f.write(b"hola")

        code, _, _ = self.run_cli("submit", path)

        self.assertEqual(0, code)
        self.assertEqual({digest(b"hola"): b"hola"}, self.coordinator.blobs)
        self.assertEqual({"a.txt": digest(b"hola")},
                         self.coordinator.submitted[0].tasks["t"].inputs)

    def test_a_listed_input_that_does_not_exist_is_a_failed_request(self):
        path = self.write_job({"tasks": {"t": {"command": ["true"],
                                                 "inputs": ["absent.c"]}}})

        code, _, err = self.run_cli("submit", path)

        self.assertEqual(cli.EXIT_REQUEST_FAILED, code)
        self.assertIn("absent.c", err)

    def finished(self, outputs):
        state = {"state": "SUCCEEDED", "outputs": outputs}
        return {"job_id": "job-1", "state": "SUCCEEDED",
                "tasks": {"t": state}}

    def test_artifacts_are_downloaded_to_their_paths_and_checked(self):
        self.coordinator.blobs[digest(b"bin")] = b"bin"
        self.coordinator.views["job-1"] = [
            self.finished({"out/app": digest(b"bin")})
        ]
        target = tempfile.TemporaryDirectory()
        self.addCleanup(target.cleanup)

        code, _, _ = self.run_cli("artifacts", "job-1", "t", target.name)

        with open(os.path.join(target.name, "out", "app"), "rb") as handle:
            self.assertEqual((0, b"bin"), (code, handle.read()))

    def test_an_artifact_whose_content_does_not_match_is_refused(self):
        self.coordinator.blobs[digest(b"bin")] = b"tampered"
        self.coordinator.views["job-1"] = [
            self.finished({"app": digest(b"bin")})
        ]
        target = tempfile.TemporaryDirectory()
        self.addCleanup(target.cleanup)

        code, _, err = self.run_cli("artifacts", "job-1", "t", target.name)

        self.assertEqual(cli.EXIT_REQUEST_FAILED, code)
        self.assertEqual([], os.listdir(target.name))

    def test_an_artifact_path_leaving_the_folder_is_refused(self):
        self.coordinator.blobs[digest(b"x")] = b"x"
        self.coordinator.views["job-1"] = [
            self.finished({"../escape": digest(b"x")})
        ]
        target = tempfile.TemporaryDirectory()
        self.addCleanup(target.cleanup)

        code, _, _ = self.run_cli("artifacts", "job-1", "t", target.name)

        self.assertEqual(cli.EXIT_REQUEST_FAILED, code)

    def test_an_unreachable_gateway_is_a_failed_request(self):
        self.url = "http://127.0.0.1:9"  # nothing listens there

        code, _, err = self.run_cli("status", "job-1")

        self.assertEqual(cli.EXIT_REQUEST_FAILED, code)
        self.assertIn("gateway", err)


if __name__ == "__main__":
    unittest.main()
