import gzip
import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.ingestion.load_mlb_warehouse import (
    _parquet_is_readable,
    _write_gzip_json_atomic,
    _write_parquet_atomic,
)


class IngestArtifactTests(unittest.TestCase):
    def test_gzip_json_is_published_complete(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "feed.json.gz"
            _write_gzip_json_atomic(output, {"gamePk": 123, "plays": [1, 2]})
            with gzip.open(output, "rt", encoding="utf-8") as handle:
                payload = json.load(handle)
            self.assertEqual(payload["gamePk"], 123)
            self.assertEqual(list(Path(tmp).glob(".*.tmp.gz")), [])

    def test_parquet_is_validated_before_publication(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "pitches.parquet"
            _write_parquet_atomic(pd.DataFrame({"pitch_number": [1, 2]}), output)
            self.assertTrue(_parquet_is_readable(output))
            self.assertEqual(list(Path(tmp).glob(".*.tmp.parquet")), [])

    def test_corrupt_parquet_is_not_complete(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "pitches.parquet"
            output.write_bytes(b"interrupted write")
            self.assertFalse(_parquet_is_readable(output))


if __name__ == "__main__":
    unittest.main()
