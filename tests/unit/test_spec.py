import unittest

from dpes.hashing import digest
from dpes.spec import (
    DEFAULT_TIMEOUT_S,
    SpecError,
    TaskSpec,
    demand,
    parse_job,
    unschedulable,
)


def job(**task_fields):
    return {"tasks": {"hello": {"command": ["echo", "hi"], **task_fields}}}


class ValidJobTest(unittest.TestCase):
    def test_a_minimal_task_gets_the_defaults(self):
        spec = parse_job(job())

        self.assertEqual(
            {"hello": TaskSpec(("echo", "hi"), {}, DEFAULT_TIMEOUT_S)},
            spec.tasks,
        )

    def test_declared_fields_are_kept(self):
        task = parse_job(job(env={"NAME": "world"}, timeout_s=1.5)).tasks["hello"]

        self.assertEqual({"NAME": "world"}, task.env)
        self.assertEqual(1.5, task.timeout_s)

    def test_an_empty_argument_after_the_program_is_allowed(self):
        task = parse_job(job(command=["printf", ""])).tasks["hello"]

        self.assertEqual(("printf", ""), task.command)


class SpecTestCase(unittest.TestCase):
    def assertRejected(self, document, *fragments):
        with self.assertRaises(SpecError) as caught:
            parse_job(document)
        joined = "\n".join(caught.exception.errors)
        for fragment in fragments:
            self.assertIn(fragment, joined)
        return caught.exception.errors


class CacheTest(SpecTestCase):
    """F9.5: a task may opt out of the content cache (plan/16, D16.4)."""

    def test_a_task_uses_the_cache_by_default(self):
        self.assertTrue(parse_job(job()).tasks["hello"].cache)

    def test_a_task_may_opt_out(self):
        self.assertFalse(parse_job(job(cache=False)).tasks["hello"].cache)

    def test_the_opt_out_must_be_a_boolean(self):
        self.assertRejected(job(cache="no"), "tasks.hello.cache")


class RejectedJobTest(SpecTestCase):
    def test_the_document_must_be_an_object(self):
        self.assertRejected(["not", "a", "job"], "job")

    def test_tasks_is_required(self):
        self.assertRejected({}, "tasks")

    def test_a_job_needs_at_least_one_task(self):
        self.assertRejected({"tasks": {}}, "at least one task")

    def test_task_names_are_limited_to_a_safe_alphabet(self):
        for name in ("", "has space", "a/b", "x" * 65):
            with self.subTest(name=name):
                document = {"tasks": {name: {"command": ["true"]}}}
                self.assertRejected(document, "name")

    def test_command_is_required(self):
        self.assertRejected({"tasks": {"hello": {}}}, "tasks.hello.command")

    def test_command_must_be_a_non_empty_list_of_strings(self):
        for command in ("echo hi", [], ["echo", 1], [""]):
            with self.subTest(command=command):
                self.assertRejected(job(command=command), "tasks.hello.command")

    def test_env_must_map_valid_names_to_strings(self):
        for env in (["A=1"], {"A": 1}, {"1A": "x"}, {"A-B": "x"}):
            with self.subTest(env=env):
                self.assertRejected(job(env=env), "tasks.hello.env")

    def test_timeout_must_be_a_positive_number_up_to_one_day(self):
        for timeout in (0, -1, 86401, "60", True):
            with self.subTest(timeout=timeout):
                self.assertRejected(job(timeout_s=timeout), "tasks.hello.timeout_s")

    def test_unknown_fields_are_rejected_to_catch_typos(self):
        self.assertRejected(job(timout_s=60), "tasks.hello.timout_s")
        self.assertRejected({**job(), "extra": 1}, "extra")

    def test_every_error_is_reported_at_once(self):
        errors = self.assertRejected(
            job(command=[], env={"A": 1}, timeout_s=0),
            "tasks.hello.command",
            "tasks.hello.env",
            "tasks.hello.timeout_s",
        )
        self.assertEqual(3, len(errors))


def tasks(**needs):
    """A job whose tasks are `name=[dependencies]`."""
    return {
        "tasks": {
            name: {"command": ["true"], "needs": deps}
            for name, deps in needs.items()
        }
    }


class DependenciesTest(SpecTestCase):
    def test_dependencies_are_kept(self):
        spec = parse_job(tasks(a=[], b=["a"], c=["a"], d=["b", "c"]))

        self.assertEqual(4, len(spec.tasks))
        self.assertEqual(("b", "c"), spec.tasks["d"].needs)
        self.assertEqual((), spec.tasks["a"].needs)

    def test_needs_must_be_a_list_of_names(self):
        for needs in ("a", [1], {"a": True}):
            with self.subTest(needs=needs):
                document = tasks(a=[], b=needs)
                self.assertRejected(document, "tasks.b.needs")

    def test_a_dependency_must_be_a_task_of_the_job(self):
        self.assertRejected(tasks(a=["ghost"]), "tasks.a.needs", "ghost")

    def test_a_dependency_cannot_be_repeated(self):
        self.assertRejected(tasks(a=[], b=["a", "a"]), "tasks.b.needs")

    def test_a_cycle_is_rejected_naming_its_path(self):
        document = tasks(a=["c"], b=["a"], c=["b"], d=[])

        self.assertRejected(document, "dependency cycle a -> c -> b -> a")

    def test_a_task_cannot_need_itself(self):
        self.assertRejected(tasks(a=["a"]), "dependency cycle a -> a")


class ResourcesTest(SpecTestCase):
    def test_one_cpu_is_asked_for_by_default(self):
        self.assertEqual({"cpu": 1.0}, parse_job(job()).tasks["hello"].resources)

    def test_declared_resources_are_kept(self):
        resources = {"cpu": 0.5, "memory_mb": 256, "gcc": 1}

        task = parse_job(job(resources=resources)).tasks["hello"]

        self.assertEqual(resources, task.resources)

    def test_amounts_must_be_finite_numbers_above_zero(self):
        for resources in (
            {"cpu": 0}, {"cpu": "2"}, {"cpu": True}, {"cpu": float("inf")},
            {"memory_mb": 0}, {"memory_mb": 1.5}, {"memory_mb": True},
            {"gcc": 0}, [],
        ):
            with self.subTest(resources=resources):
                document = job(resources=resources)
                self.assertRejected(document, "tasks.hello.resources")

    def test_capability_names_cannot_clash_with_ray_own_names(self):
        for name in ("GPU", "has space", "memory", "object_store_memory"):
            with self.subTest(name=name):
                document = job(resources={name: 1})
                self.assertRejected(document, f"tasks.hello.resources.{name}")


D = digest(b"content")
BAD_PATHS = ("/etc/x", "../x", "a/../b", "./a", "a//b", "a\\b", "", "a/",
             ".", "x" * 256)


class InputsOutputsTest(SpecTestCase):
    def test_inputs_and_outputs_are_kept(self):
        task = parse_job(job(inputs={"src/main.c": D},
                             outputs=["main.o"])).tasks["hello"]

        self.assertEqual({"src/main.c": D}, task.inputs)
        self.assertEqual(("main.o",), task.outputs)

    def test_a_path_must_stay_inside_the_workspace_in_canonical_form(self):
        for path in BAD_PATHS:
            with self.subTest(path=path):
                self.assertRejected(job(inputs={path: D}),
                                    "tasks.hello.inputs")
                self.assertRejected(job(outputs=[path]),
                                    "tasks.hello.outputs")

    def test_an_input_must_name_a_digest(self):
        self.assertRejected(job(inputs={"a.c": "not-a-digest"}),
                            "tasks.hello.inputs")
        self.assertRejected(job(inputs=["a.c"]), "tasks.hello.inputs")

    def test_an_output_cannot_be_declared_twice(self):
        self.assertRejected(job(outputs=["a.o", "a.o"]),
                            "tasks.hello.outputs")

    def test_a_path_cannot_arrive_from_two_sources(self):
        made = {"command": ["true"], "outputs": ["x"]}
        self.assertRejected(
            {"tasks": {"a": made,
                       "b": {"command": ["true"], "needs": ["a"],
                             "inputs": {"x": D}}}},
            "tasks.b", "'x'",
        )
        self.assertRejected(
            {"tasks": {"a": made, "c": made,
                       "b": {"command": ["true"], "needs": ["a", "c"]}}},
            "tasks.b", "'x'",
        )


class RetriesTest(SpecTestCase):
    def test_the_default_budgets_apply_when_none_is_declared(self):
        retries = parse_job(job()).tasks["hello"].retries

        self.assertEqual({"task": 0, "worker": 3, "network": 4}, retries)

    def test_a_declared_budget_replaces_only_its_own_class(self):
        retries = parse_job(job(retries={"task": 2})).tasks["hello"].retries

        self.assertEqual({"task": 2, "worker": 3, "network": 4}, retries)

    def test_a_budget_is_a_whole_number_from_zero_to_ten(self):
        for retries in ({"task": -1}, {"worker": 11}, {"network": 1.5},
                        {"task": True}, {"other": 1}, [3]):
            with self.subTest(retries=retries):
                document = job(retries=retries)
                self.assertRejected(document, "tasks.hello.retries")


WORKER_1 = {"CPU": 2.0, "memory": 4 * 2**30, "worker-1": 1.0}
WORKER_2 = {"CPU": 2.0, "memory": 4 * 2**30, "worker-2": 1.0}
CONTROL_NODE = {"memory": 4 * 2**30}


class PlacementTest(unittest.TestCase):
    def task(self, **resources):
        return parse_job(job(resources=resources)).tasks["hello"]

    def test_demand_is_expressed_in_ray_resource_names(self):
        wanted = demand(self.task(cpu=2, memory_mb=1, gcc=1))

        self.assertEqual({"CPU": 2, "memory": 2**20, "gcc": 1}, wanted)

    def assertSchedulable(self, expected, **resources):
        spec = parse_job(job(resources=resources))
        errors = unschedulable(spec, [CONTROL_NODE, WORKER_1, WORKER_2])
        self.assertEqual(expected, not errors, errors)

    def test_a_task_that_fits_one_live_node_is_schedulable(self):
        self.assertSchedulable(True, cpu=2)
        self.assertSchedulable(True, cpu=1, **{"worker-2": 1})

    def test_a_task_that_fits_no_node_is_not(self):
        self.assertSchedulable(False, cpu=3)
        self.assertSchedulable(False, gpu=1)
        self.assertSchedulable(False, memory_mb=8 * 1024)

    def test_resources_spread_over_two_nodes_do_not_add_up(self):
        self.assertSchedulable(False, **{"worker-1": 1, "worker-2": 1})

    def test_the_error_names_the_task_and_what_it_asks_for(self):
        spec = parse_job(job(resources={"gpu": 1}))

        (error,) = unschedulable(spec, [WORKER_1])

        self.assertIn("tasks.hello.resources", error)
        self.assertIn("gpu", error)


if __name__ == "__main__":
    unittest.main()
