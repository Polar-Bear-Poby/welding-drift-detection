import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import spark_batch


def write_signal(path: Path, values: list[float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "value\n" + "\n".join(str(value) for value in values) + "\n",
        encoding="utf-8",
    )


class SparkBatchUnitTests(unittest.TestCase):
    def test_parse_source_metadata_reads_line_channel_and_product(self):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = (
                Path(tmp)
                / "20220417"
                / "out"
                / "3_20220417_battery_10_laser_a.csv"
            )
            write_signal(csv_path, [0.1, 0.2])

            meta = spark_batch.parse_source_metadata(csv_path)

            self.assertEqual(meta.line_number, 3)
            self.assertEqual(meta.line_id, "LINE_03")
            self.assertEqual(meta.channel, 1)
            self.assertEqual(meta.product_id, "battery_10")
            self.assertEqual(meta.event_date, "2022-04-17")

    def test_build_segment_rows_creates_16_ordered_odd_even_patterns(self):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = (
                Path(tmp)
                / "20220417"
                / "reflect"
                / "2_20220417_battery_22_laser_b.csv"
            )
            write_signal(csv_path, [float(i) for i in range(32)])

            rows = spark_batch.build_segment_rows(
                source_path=csv_path,
                run_id="00000000-0000-0000-0000-000000000001",
                processed_at=datetime.now(timezone.utc),
                pattern_count=16,
            )

            self.assertEqual(len(rows), 16)
            self.assertEqual(rows[0]["segment_index"], 1)
            self.assertEqual(rows[0]["parity_group"], "odd")
            self.assertEqual(rows[0]["parity_order"], 1)
            self.assertEqual(rows[1]["segment_index"], 2)
            self.assertEqual(rows[1]["parity_group"], "even")
            self.assertEqual(rows[1]["parity_order"], 1)
            self.assertEqual(rows[14]["segment_index"], 15)
            self.assertEqual(rows[14]["parity_group"], "odd")
            self.assertEqual(rows[14]["parity_order"], 8)
            self.assertEqual(rows[15]["segment_index"], 16)
            self.assertEqual(rows[15]["parity_group"], "even")
            self.assertEqual(rows[15]["parity_order"], 8)
            self.assertEqual(rows[0]["line_id"], "LINE_02")
            self.assertEqual(rows[0]["channel"], 0)

    def test_discover_csv_files_applies_max_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for idx in range(5):
                write_signal(root / f"20220417_battery_{idx}_laser_b.csv", [0.1, 0.2])

            files = spark_batch.discover_csv_files(str(root), max_files=3)

            self.assertEqual(len(files), 3)


if __name__ == "__main__":
    unittest.main()
