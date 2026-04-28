import tempfile
import unittest
from pathlib import Path

import numpy as np

import producer
import spark_streaming


def write_signal(path: Path, values: list[float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "value\n" + "\n".join(str(value) for value in values) + "\n",
        encoding="utf-8",
    )


class StreamingSchemaCompatTests(unittest.TestCase):
    def test_streaming_schema_accepts_producer_samples_payload(self):
        schema_fields = {field.name for field in spark_streaming.SIGNAL_MESSAGE_SCHEMA.fields}
        self.assertIn("samples", schema_fields)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            signal_b = root / "20220417_battery_10_laser_b.csv"
            signal_a = root / "20220417_battery_10_laser_a.csv"
            write_signal(signal_b, [0.1, 0.2, 0.3, 0.4])
            write_signal(signal_a, [0.5, 0.6, 0.7, 0.8])

            record = producer.scan_data_dir(str(root))[0]
            item = producer.make_publish_items([record], 0, 1, 1, 10)[0]
            message = producer.make_message(
                item=item,
                lead_num=1,
                channel=0,
                file_path=signal_b,
                samples=np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32),
                chunk_index=0,
                chunk_size=2,
                total_chunks=2,
            )

            self.assertIn("samples", message)
            self.assertNotIn("signal", message)


if __name__ == "__main__":
    unittest.main()
