"""Cut C0: the container cluster is up. Run it inside the control node:

    docker compose -f docker/compose.yaml exec -T head \
        python -m unittest discover -s tests/e2e
"""

import time
import unittest

import ray


class ClusterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(address="auto", logging_level="ERROR")

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_control_node_and_two_workers_are_alive(self):
        deadline = time.monotonic() + 60
        while True:
            alive = [node for node in ray.nodes() if node["Alive"]]
            if len(alive) == 3 or time.monotonic() > deadline:
                break
            time.sleep(1)

        self.assertEqual(3, len(alive))

    def test_tasks_run_on_workers_never_on_the_control_node(self):
        @ray.remote(max_retries=0)
        def node_running_this_task():
            return ray.get_runtime_context().get_node_id()

        control_node = ray.get_runtime_context().get_node_id()
        used = ray.get(
            [node_running_this_task.remote() for _ in range(8)], timeout=60
        )

        self.assertNotIn(control_node, used)


if __name__ == "__main__":
    unittest.main()
