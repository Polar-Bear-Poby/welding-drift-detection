import tempfile
import unittest
from pathlib import Path

import numpy as np

import producer


def write_signal(path: Path, values: list[float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "value\n" + "\n".join(str(value) for value in values) + "\n",
        encoding="utf-8",
    )


class FakeFuture:
    def add_callback(self, callback):
        self.callback = callback
        return self

    def add_errback(self, errback):
        self.errback = errback
        return self


class FakeProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, value):
        self.sent.append({"topic": topic, "key": key, "value": value})
        return FakeFuture()


class ProducerUnitTests(unittest.TestCase):
    def test_scan_groups_laser_files_and_maps_channels(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_signal(
                root / "laser_b" / "20220417" / "20220417_battery_10_laser_b.csv",
                [0.1, 0.2],
            )
            write_signal(
                root / "laser_a" / "20220417" / "20220417_battery_10_laser_a.csv",
                [0.3, 0.4],
            )

            records = producer.scan_data_dir(str(root))

            self.assertEqual(len(records), 1)
            record = records[0]
            self.assertEqual(record.product_instance_id, "20220417_battery_10")
            self.assertEqual(record.line_id, "LINE_01")
            self.assertEqual(record.files[0][1].name, "20220417_battery_10_laser_b.csv")
            self.assertEqual(record.files[1][1].name, "20220417_battery_10_laser_a.csv")
            self.assertEqual(producer.channel_code(0), "LB")
            self.assertEqual(producer.channel_code(1), "LA")
            self.assertEqual(producer.channel_name(0), "LaserB")
            self.assertEqual(producer.channel_name(1), "LaserA")

    def test_scan_supports_date_out_reflect_storage_layout(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_signal(root / "20220417" / "reflect" / "battery_10.csv", [0.1, 0.2])
            write_signal(root / "20220417" / "out" / "battery_10.csv", [0.3, 0.4])

            records = producer.scan_data_dir(str(root))

            self.assertEqual(len(records), 1)
            record = records[0]
            self.assertEqual(record.product_instance_id, "20220417_battery_10")
            self.assertEqual(record.files[0][1].parts[-2:], ("reflect", "battery_10.csv"))
            self.assertEqual(record.files[1][1].parts[-2:], ("out", "battery_10.csv"))

    def test_publish_items_simulate_multiple_lines_every_10_seconds(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for battery_id in (10, 11):
                write_signal(
                    root / f"20220417_battery_{battery_id}_laser_b.csv",
                    [0.1, 0.2],
                )
                write_signal(
                    root / f"20220417_battery_{battery_id}_laser_a.csv",
                    [0.3, 0.4],
                )

            records = producer.scan_data_dir(str(root))
            items = producer.make_publish_items(
                records=records,
                target_products=0,
                max_products=2,
                line_count=3,
                line_interval_seconds=10,
            )

            self.assertEqual(len(items), 6)
            self.assertEqual([item.line_id for item in items[:3]], ["LINE_01", "LINE_02", "LINE_03"])
            self.assertEqual([item.line_id for item in items[3:]], ["LINE_01", "LINE_02", "LINE_03"])
            self.assertEqual([item.line_number for item in items[:3]], [1, 2, 3])
            self.assertEqual([item.line_number for item in items[3:]], [1, 2, 3])

            first_event_time = items[0].event_time
            offsets = [int((item.event_time - first_event_time).total_seconds()) for item in items]
            self.assertEqual(offsets, [0, 0, 0, 10, 10, 10])

    def test_make_message_contains_deterministic_id_and_chunk_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            signal_path = root / "20220417_battery_10_laser_b.csv"
            write_signal(signal_path, [0.1, 0.2, 0.3, 0.4])
            write_signal(root / "20220417_battery_10_laser_a.csv", [0.5, 0.6])

            record = producer.scan_data_dir(str(root))[0]
            item = producer.make_publish_items([record], 0, 1, 1, 10)[0]
            message = producer.make_message(
                item=item,
                lead_num=1,
                channel=0,
                file_path=signal_path,
                samples=np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32),
                chunk_index=1,
                chunk_size=2,
                total_chunks=2,
            )

            self.assertEqual(message["message_id"], "20220417_battery_10:L01:LB:000001")
            self.assertEqual(message["line_id"], "LINE_01")
            self.assertEqual(message["channel"], 0)
            self.assertEqual(message["channel_name"], "LaserB")
            self.assertEqual(message["start_sample"], 2)
            self.assertEqual(message["end_sample"], 4)
            self.assertTrue(message["is_last_chunk"])
            self.assertEqual(message["metadata"]["file_name"], "1_20220417_battery_10_laser_b.csv")
            self.assertIn("chunk_checksum", message["metadata"])

    def test_split_signal_into_byte_chunks_is_contiguous(self):
        signal = np.array([float(i) for i in range(25)], dtype=np.float32)
        ranges = producer.split_signal_into_byte_chunks(signal, target_chunk_bytes=80)

        self.assertGreater(len(ranges), 1)
        self.assertEqual(ranges[0][0], 0)
        self.assertEqual(ranges[-1][1], len(signal))
        for i in range(1, len(ranges)):
            self.assertEqual(ranges[i - 1][1], ranges[i][0])
            self.assertLess(ranges[i][0], ranges[i][1])

    def test_publish_product_sends_chunked_messages_with_partition_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_signal(root / "20220417_battery_10_laser_b.csv", [float(i) for i in range(20)])
            write_signal(root / "20220417_battery_10_laser_a.csv", [float(i) for i in range(20, 40)])

            record = producer.scan_data_dir(str(root))[0]
            item = producer.make_publish_items([record], 0, 1, 1, 10)[0]
            fake_producer = FakeProducer()

            sent_count = producer.publish_product(
                producer=fake_producer,
                item=item,
                topic="test.topic",
                topic_laser_a=None,
                topic_laser_b=None,
                chunk_size=3,
                target_chunk_bytes=80,
                speed=0,
            )

            expected_chunks_per_channel = len(
                producer.split_signal_into_byte_chunks(
                    np.array([float(i) for i in range(20)], dtype=np.float32),
                    target_chunk_bytes=80,
                )
            )
            self.assertEqual(sent_count, expected_chunks_per_channel * 2)
            self.assertEqual(len(fake_producer.sent), expected_chunks_per_channel * 2)
            keys = [sent["key"] for sent in fake_producer.sent]
            self.assertEqual(
                keys[:expected_chunks_per_channel],
                ["LINE_01_20220417_battery_10_L01_LB"] * expected_chunks_per_channel,
            )
            self.assertEqual(
                keys[expected_chunks_per_channel:],
                ["LINE_01_20220417_battery_10_L01_LA"] * expected_chunks_per_channel,
            )
            self.assertEqual(fake_producer.sent[0]["value"]["chunk_index"], 0)
            self.assertEqual(fake_producer.sent[1]["value"]["chunk_index"], 1)
            self.assertEqual(
                fake_producer.sent[0]["value"]["total_chunks"], expected_chunks_per_channel
            )
            self.assertEqual(
                fake_producer.sent[expected_chunks_per_channel - 1]["value"]["chunk_index"],
                expected_chunks_per_channel - 1,
            )
            self.assertEqual(
                fake_producer.sent[expected_chunks_per_channel]["value"]["chunk_index"], 0
            )

    def test_publish_product_routes_messages_to_channel_topics(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_signal(root / "20220417_battery_10_laser_b.csv", [0.1, 0.2, 0.3, 0.4])
            write_signal(root / "20220417_battery_10_laser_a.csv", [0.5, 0.6, 0.7, 0.8])

            record = producer.scan_data_dir(str(root))[0]
            item = producer.make_publish_items([record], 0, 1, 1, 10)[0]
            fake_producer = FakeProducer()

            producer.publish_product(
                producer=fake_producer,
                item=item,
                topic="test.topic.fallback",
                topic_laser_a="test.topic.laser_a",
                topic_laser_b="test.topic.laser_b",
                chunk_size=3,
                target_chunk_bytes=80,
                speed=0,
            )

            topics = {sent["topic"] for sent in fake_producer.sent}
            self.assertIn("test.topic.laser_a", topics)
            self.assertIn("test.topic.laser_b", topics)
            self.assertNotIn("test.topic.fallback", topics)


if __name__ == "__main__":
    unittest.main()
