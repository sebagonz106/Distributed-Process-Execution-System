"""Cut C3: inputs go in, artifacts flow between workers and come back.

Uses the demonstration project in examples/hola (plan/11).

    docker compose -f docker/compose.yaml exec -T head \
        python -m unittest discover -s tests/e2e
"""

import json
import os
import shutil
import subprocess
import tempfile
import unittest

from gateway_client import cli, wait, wait_for_gateway

from dpes.hashing import digest

EXAMPLE = os.path.join(os.path.dirname(__file__), "..", "..", "examples",
                       "hola")


class ArtifactTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        wait_for_gateway()

    def project(self, tasks=None):
        """A copy of the example, with every task set to run for real.

        These tests watch the build happen: workers named, artifacts crossing
        them, a file left behind. A task served from the content cache runs
        nothing and would test none of it (plan/16, F9.5).
        """
        folder = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, folder, True)
        for name in os.listdir(EXAMPLE):
            shutil.copy(os.path.join(EXAMPLE, name), folder)
        if tasks is None:
            with open(os.path.join(folder, "job.json")) as handle:
                tasks = json.load(handle)["tasks"]
        tasks = {name: {**task, "cache": False}
                 for name, task in tasks.items()}
        with open(os.path.join(folder, "job.json"), "w") as handle:
            json.dump({"tasks": tasks}, handle)
        return folder

    def run_project(self, folder):
        code, job_id, err = cli("submit", os.path.join(folder, "job.json"))
        self.assertEqual(0, code, err)
        code, view = wait(job_id.strip())
        return code, view

    def test_the_example_builds_and_its_test_passes(self):
        code, view = self.run_project(self.project())

        states = {name: t["state"] for name, t in view["tasks"].items()}
        self.assertEqual(0, code, states)
        self.assertEqual({"SUCCEEDED"}, set(states.values()))

    def test_objects_cross_workers_and_the_binary_comes_back_intact(self):
        def on(worker, command, **fields):
            script = f"{command} && hostname"
            return {"command": ["sh", "-c", script],
                    "resources": {worker: 1}, **fields}

        folder = self.project({
            "compile-main": on("worker-1", "gcc -c main.c -o main.o",
                               inputs=["main.c", "greet.h"],
                               outputs=["main.o"]),
            "compile-greet": on("worker-1", "gcc -c greet.c -o greet.o",
                                inputs=["greet.c", "greet.h"],
                                outputs=["greet.o"]),
            "link": on("worker-2", "gcc main.o greet.o -o hola",
                       needs=["compile-main", "compile-greet"],
                       outputs=["hola"]),
        })

        code, view = self.run_project(folder)
        tasks = view["tasks"]
        self.assertEqual(0, code)
        self.assertEqual("worker-1\n", tasks["compile-main"]["output"])
        self.assertEqual("worker-2\n", tasks["link"]["output"])

        out = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, out, True)
        code, _, err = cli("artifacts", view["job_id"], "link", out)
        binary = os.path.join(out, "hola")
        with open(binary, "rb") as handle:
            self.assertEqual(tasks["link"]["outputs"]["hola"],
                             digest(handle.read()))
        os.chmod(binary, 0o755)
        self.assertEqual("hola, mundo\n",
                         subprocess.check_output([binary], text=True))

    def test_a_file_that_is_not_declared_is_not_there(self):
        folder = self.project({
            "only-main": {"command": ["cat", "greet.h"],
                          "inputs": ["main.c"]},
            "leaves-junk": {"command": ["sh", "-c", "touch junk a.o"],
                            "outputs": ["a.o"]},
            "looks-for-junk": {"command": ["test", "-e", "junk"],
                               "needs": ["leaves-junk"]},
        })

        _, view = self.run_project(folder)

        states = {name: t["state"] for name, t in view["tasks"].items()}
        self.assertEqual(
            {"only-main": "FAILED", "leaves-junk": "SUCCEEDED",
             "looks-for-junk": "FAILED"},
            states,
        )


if __name__ == "__main__":
    unittest.main()
