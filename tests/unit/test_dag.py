import unittest

from dpes.dag import find_cycle

RHOMBUS = {"a": [], "b": ["a"], "c": ["a"], "d": ["b", "c"]}


class FindCycleTest(unittest.TestCase):
    def test_a_dag_has_no_cycle(self):
        self.assertIsNone(find_cycle(RHOMBUS))

    def test_a_task_needing_itself_is_a_cycle(self):
        self.assertEqual(["a", "a"], find_cycle({"a": ["a"]}))

    def test_the_cycle_is_reported_as_a_path(self):
        graph = {"a": ["b"], "b": ["c"], "c": ["a"], "d": ["a"]}

        self.assertEqual(["a", "b", "c", "a"], find_cycle(graph))

    def test_the_same_graph_always_reports_the_same_cycle(self):
        graph = {"c": ["a"], "b": ["c"], "a": ["b"]}

        self.assertEqual(["a", "b", "c", "a"], find_cycle(graph))

    def test_a_cycle_away_from_the_first_task_is_found(self):
        graph = {"a": [], "x": ["y"], "y": ["x"]}

        self.assertEqual(["x", "y", "x"], find_cycle(graph))

    def test_a_dependency_on_an_unknown_name_is_not_a_cycle(self):
        self.assertIsNone(find_cycle({"a": ["ghost"]}))

    def test_a_long_chain_does_not_exhaust_the_stack(self):
        chain = {f"t{i}": [f"t{i + 1}"] for i in range(20_000)}
        chain["t20000"] = []

        self.assertIsNone(find_cycle(chain))


if __name__ == "__main__":
    unittest.main()
