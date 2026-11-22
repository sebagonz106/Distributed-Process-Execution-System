import re
import unittest

from dpes.hashing import ACTION_KEY_VERSION, action_key, digest, is_digest

SOURCE = digest(b"int main(void) { return 0; }\n")
HEADER = digest(b"#define ANSWER 42\n")

# One unit of work, written the way the coordinator would describe it.
WORK = {
    "command": ["gcc", "-O2", "-c", "a.c", "-o", "a.o"],
    "env": {"CFLAGS": "-O2", "PATH": "/usr/bin:/bin"},
    "inputs": {"a.c": SOURCE},
    "outputs": ["a.o"],
    "platform_id": "linux/amd64:gcc-13",
}


def key(**changes):
    """The key of WORK with the named fields replaced."""
    return action_key(**{**WORK, **changes})


class ActionKeyTest(unittest.TestCase):
    def test_the_same_work_always_gets_the_same_key(self):
        self.assertEqual(key(), key())

    def test_the_key_is_a_bare_hex_sha256(self):
        # Bare hex, not the `sha256:` form: a cache key is not a blob name.
        self.assertTrue(re.fullmatch(r"[0-9a-f]{64}", key()))
        self.assertFalse(is_digest(key()))

    def test_everything_that_changes_the_work_changes_the_key(self):
        changes = {
            "another command": {"command": ["gcc", "-c", "b.c", "-o", "a.o"]},
            "another flag order": {
                "command": ["gcc", "-c", "-O2", "a.c", "-o", "a.o"]
            },
            "a variable removed": {"env": {"PATH": "/usr/bin:/bin"}},
            "a variable changed": {
                "env": {"CFLAGS": "-O1", "PATH": "/usr/bin:/bin"}
            },
            "an input digest changed": {"inputs": {"a.c": HEADER}},
            "an input added": {"inputs": {"a.c": SOURCE, "a.h": HEADER}},
            "an output added": {"outputs": ["a.o", "a.d"]},
            "another platform": {"platform_id": "linux/arm64:gcc-13"},
            "another format version": {"key_format_version": 2},
        }
        baseline = key()
        for what, change in changes.items():
            with self.subTest(change=what):
                self.assertNotEqual(baseline, key(**change))

    def test_the_order_of_the_mapped_keys_does_not_matter(self):
        reordered = key(
            env={"PATH": "/usr/bin:/bin", "CFLAGS": "-O2"},
            inputs={"a.c": SOURCE},
        )

        self.assertEqual(key(), reordered)

    def test_the_order_of_the_declared_outputs_does_not_matter(self):
        # Declaring the same set of outputs in another order is the same work.
        self.assertEqual(
            key(outputs=["a.o", "a.d"]), key(outputs=["a.d", "a.o"])
        )

    def test_the_order_of_the_command_arguments_does_matter(self):
        # argv is a sequence: swapping arguments changes what runs (D01.3).
        self.assertNotEqual(
            key(command=["gcc", "-O2", "-c"]), key(command=["-O2", "gcc", "-c"])
        )

    def test_work_without_a_platform_is_refused(self):
        # An empty platform would let a key from another toolchain hit.
        with self.assertRaises(ValueError):
            key(platform_id="")

    def test_the_default_version_is_the_published_one(self):
        self.assertEqual(ACTION_KEY_VERSION, 1)
        self.assertEqual(key(), key(key_format_version=ACTION_KEY_VERSION))


if __name__ == "__main__":
    unittest.main()
