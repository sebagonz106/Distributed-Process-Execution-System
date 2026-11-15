import unittest

from dpes.spec import DEFAULT_TIMEOUT_S, SpecError, TaskSpec, parse_job


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


class RejectedJobTest(unittest.TestCase):
    def assertRejected(self, document, *fragments):
        with self.assertRaises(SpecError) as caught:
            parse_job(document)
        joined = "\n".join(caught.exception.errors)
        for fragment in fragments:
            self.assertIn(fragment, joined)
        return caught.exception.errors

    def test_the_document_must_be_an_object(self):
        self.assertRejected(["not", "a", "job"], "job")

    def test_tasks_is_required(self):
        self.assertRejected({}, "tasks")

    def test_a_job_needs_exactly_one_task_in_phase_1(self):
        self.assertRejected({"tasks": {}}, "exactly one task")
        two = {"tasks": {"a": {"command": ["true"]}, "b": {"command": ["true"]}}}
        self.assertRejected(two, "exactly one task")

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

    def test_fields_of_later_phases_are_rejected_until_they_exist(self):
        self.assertRejected(job(needs=["build"]), "tasks.hello.needs")

    def test_every_error_is_reported_at_once(self):
        errors = self.assertRejected(
            job(command=[], env={"A": 1}, timeout_s=0),
            "tasks.hello.command",
            "tasks.hello.env",
            "tasks.hello.timeout_s",
        )
        self.assertEqual(3, len(errors))


if __name__ == "__main__":
    unittest.main()
