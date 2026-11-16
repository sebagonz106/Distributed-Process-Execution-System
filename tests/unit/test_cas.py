import os
import tempfile
import unittest

from dpes.cas import BlobDir
from dpes.hashing import digest, is_digest

HELLO = b"hola\n"


class DigestTest(unittest.TestCase):
    def test_a_digest_is_sha256_of_the_content(self):
        import hashlib

        expected = "sha256:" + hashlib.sha256(HELLO).hexdigest()

        self.assertEqual(expected, digest(HELLO))

    def test_only_the_canonical_form_is_a_digest(self):
        good = digest(HELLO)
        self.assertTrue(is_digest(good))
        for text in ("", "sha256:", good.upper(), good[:-1],
                     "md5:" + good[7:], "sha256:../../etc/passwd", 5):
            with self.subTest(text=text):
                self.assertFalse(is_digest(text))


class BlobDirTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = directory.name
        self.store = BlobDir(self.root)

    def test_what_is_put_is_got_back(self):
        name = self.store.put(digest(HELLO), HELLO)

        self.assertEqual(HELLO, self.store.get(name))

    def test_putting_the_same_blob_twice_is_harmless(self):
        self.store.put(digest(HELLO), HELLO)
        self.store.put(digest(HELLO), HELLO)

        self.assertEqual(1, len(os.listdir(self.root)))

    def test_content_that_does_not_match_its_digest_is_refused(self):
        with self.assertRaises(ValueError):
            self.store.put(digest(b"other"), HELLO)
        self.assertEqual([], os.listdir(self.root))

    def test_a_name_that_is_not_a_digest_is_refused(self):
        with self.assertRaises(ValueError):
            self.store.put("sha256:../../escape", HELLO)
        with self.assertRaises(ValueError):
            self.store.get("../../escape")

    def test_an_unknown_blob_is_none(self):
        self.assertIsNone(self.store.get(digest(b"never stored")))

    def test_missing_lists_what_is_not_stored(self):
        self.store.put(digest(HELLO), HELLO)
        absent = digest(b"absent")

        self.assertEqual([absent],
                         self.store.missing([digest(HELLO), absent]))


if __name__ == "__main__":
    unittest.main()
