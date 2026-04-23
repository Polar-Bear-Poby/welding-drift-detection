import os
import unittest

from kafka.errors import KafkaError

import producer


@unittest.skipUnless(
    os.getenv("RUN_KAFKA_INTEGRATION") == "1",
    "set RUN_KAFKA_INTEGRATION=1 to run Kafka integration tests",
)
class KafkaIntegrationTests(unittest.TestCase):
    def test_real_kafka_accepts_producer_message(self):
        """Smoke test: the broker accepts one Producer message and returns metadata."""
        bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
        topic = os.getenv("KAFKA_TEST_TOPIC", "welding.raw.v1")

        try:
            kafka_producer = producer.build_producer(bootstrap)
        except Exception as exc:
            raise unittest.SkipTest(f"Kafka is not available at {bootstrap}: {exc}") from exc

        try:
            future = kafka_producer.send(
                topic,
                key="TEST_LINE_01_TEST_PRODUCT_L01_LB",
                value={
                    "message_id": "integration-test:L01:LB:000000",
                    "product_instance_id": "integration-test",
                    "product_id": "TEST_PRODUCT",
                    "line_id": "TEST_LINE_01",
                    "lead_num": 1,
                    "lead_type": "AL_CU",
                    "channel": 0,
                    "channel_name": "LaserB",
                    "chunk_index": 0,
                    "total_chunks": 1,
                    "is_last_chunk": True,
                    "sample_rate_hz": 25000,
                    "samples": [0.1, 0.2],
                    "event_time": "2022-04-17T00:00:00Z",
                    "metadata": {
                        "source": "integration_test",
                        "version": "v1",
                        "file_name": "integration_test_laser_b.csv",
                        "original_product_instance_id": "integration-test",
                        "is_duplicate": False,
                        "replay_iteration": 0,
                    },
                },
            )
            metadata = future.get(timeout=10)
            kafka_producer.flush(timeout=10)
        except KafkaError as exc:
            self.fail(f"Kafka did not acknowledge the Producer message: {exc}")
        finally:
            kafka_producer.close(timeout=5)

        self.assertEqual(metadata.topic, topic)
        self.assertGreaterEqual(metadata.partition, 0)
        self.assertGreaterEqual(metadata.offset, 0)


if __name__ == "__main__":
    unittest.main()
