"""Tests for ZIP validation; no Docker or Superset required."""
import pathlib
import sys
import tempfile
import unittest
from zipfile import ZipFile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from validate_superset_export import validate


class TestSupersetExport(unittest.TestCase):
    def make_zip(self, files):
        tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
        tmp.close()
        self.addCleanup(lambda: pathlib.Path(tmp.name).unlink(missing_ok=True))
        with ZipFile(tmp.name, "w") as z:
            for path, content in files.items():
                z.writestr(path, content)
        return tmp.name

    def test_valid_export(self):
        path = self.make_zip({
            "dashboard_export_test/dashboards/pge.yaml": "dashboard_title: PGE",
            "dashboard_export_test/charts/total.yaml": "slice_name: Total",
            "dashboard_export_test/datasets/clickhouse/fato.yaml": "table_name: fato",
        })
        self.assertEqual(validate(path)["dashboards"], 1)
        self.assertEqual(validate(path)["charts"], 1)

    def test_empty_dashboard_export_is_rejected(self):
        path = self.make_zip({"dashboard_export_test/metadata.yaml": "version: 1"})
        with self.assertRaisesRegex(ValueError, "sem dashboards"):
            validate(path)

    def test_traversal_is_rejected(self):
        path = self.make_zip({"../dashboards/private.yaml": "bad"})
        with self.assertRaisesRegex(ValueError, "inseguro"):
            validate(path)

    def test_corrupt_file_is_rejected(self):
        f = tempfile.NamedTemporaryFile(delete=False)
        f.write(b"not a zip")
        f.close()
        self.addCleanup(lambda: pathlib.Path(f.name).unlink(missing_ok=True))
        with self.assertRaisesRegex(ValueError, "invalido"):
            validate(f.name)


if __name__ == "__main__":
    unittest.main()
