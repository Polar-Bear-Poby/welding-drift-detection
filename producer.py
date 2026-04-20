"""
Kafka producer for welding sensor CSV replay.

Scenario
--------
Multiple welding lines create CSV files after each product finishes welding.
This producer scans a file directory, groups Laser A and Laser B
files into product instances, splits each signal into chunks, and publishes
the chunks to Kafka topic `welding.raw.v1`.

Run examples
------------
python producer.py --data-dir ./data --kafka localhost:29092 --speed 50
python producer.py --data-dir ./data --target-products 2000 --speed 100
python producer.py --data-dir ./data --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import numpy as np
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt="%H:%M:%S")
logger = logging.getLogger("welding.producer")


TOPIC_RAW = os.getenv("TOPIC_RAW", "welding.raw.v1")
DEFAULT_KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
DEFAULT_CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "5000"))
SAMPLE_RATE_HZ = int(os.getenv("SAMPLE_RATE_HZ", "25000"))
MAX_REQUEST_SIZE = int(os.getenv("MAX_REQUEST_SIZE", "5242880"))
PRODUCER_VERSION = "v1"


# Expected file name:
# 20220417_000442_1_WLINE_01_04_PROD_001_01_LB.csv
FILE_RE = re.compile(
    r"(?P<date>\d{8})_(?P<time>\d{6})_(?P<seq>\d+)_"
    r"(?P<line>[A-Za-z0-9]+)_(?P<batch>\d{2})_(?P<product_id>[A-Za-z0-9_-]+)_"
    r"(?P<lead_num>\d{2})_(?:(?:CH(?P<channel>[01]))|(?P<laser_id>L[AB]))\.csv$"
)
SIMPLE_FILE_RE = re.compile(
    r"(?P<date>\d{8})_battery_(?P<battery_id>\d+)_"
    r"(?:(?:CH(?P<channel>[01]))|(?P<laser>laser_[ab]))\.csv$"
)
LASER_NAME_TO_CHANNEL = {"laser_b": 0, "laser_a": 1}
LASER_ID_TO_CHANNEL = {"LB": 0, "LA": 1}


def parse_event_time(date_text: str, time_text: str) -> datetime:
    return datetime.strptime(
        f"{date_text}{time_text}", "%Y%m%d%H%M%S"
    ).replace(tzinfo=timezone.utc)


def isoformat_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def lead_type_from_num(lead_num: int) -> str:
    return "AL_CU" if lead_num % 2 == 1 else "CU_CU"


def channel_code(channel: int) -> str:
    return "LB" if channel == 0 else "LA"


def channel_name(channel: int) -> str:
    return "LaserB" if channel == 0 else "LaserA"


@dataclass
class ProductRecord:
    """One physical product instance produced by one welding line."""

    product_instance_id: str
    product_id: str
    line_id: str
    batch_id: str
    sequence_id: str
    event_time: datetime
    files: dict[int, dict[int, Path]] = field(
        default_factory=lambda: {0: {}, 1: {}}
    )

    def add_file(self, lead_num: int, channel: int, path: Path) -> None:
        self.files[channel][lead_num] = path

    @property
    def leads_present(self) -> set[int]:
        return set(self.files[0].keys()) | set(self.files[1].keys())

    @property
    def channel_pairs(self) -> int:
        return len(set(self.files[0].keys()) & set(self.files[1].keys()))

    @property
    def is_complete(self) -> bool:
        return self.channel_pairs >= 16


@dataclass
class PublishItem:
    record: ProductRecord
    product_instance_id: str
    event_time: datetime
    is_duplicate: bool = False
    original_product_instance_id: str | None = None
    replay_iteration: int = 0


def build_instance_id(parts: dict[str, str]) -> str:
    return (
        f"{parts['date']}_{parts['time']}_{parts['seq']}_"
        f"{parts['line']}_{parts['batch']}_{parts['product_id']}"
    )


def scan_data_dir(data_dir: str) -> list[ProductRecord]:
    """Scan CSV files and group them into line/product instances."""
    root = Path(data_dir)
    if not root.exists():
        raise FileNotFoundError(f"data directory does not exist: {data_dir}")

    records: dict[str, ProductRecord] = {}
    ignored = 0

    for csv_path in sorted(root.glob("**/*.csv")):
        match = FILE_RE.match(csv_path.name)
        if match:
            parts = match.groupdict()
            instance_id = build_instance_id(parts)
            lead_num = int(parts["lead_num"])
            channel = (
                LASER_ID_TO_CHANNEL[parts["laser_id"]]
                if parts.get("laser_id")
                else int(parts["channel"])
            )
            product_id = parts["product_id"]
            line_id = parts["line"]
            batch_id = parts["batch"]
            sequence_id = parts["seq"]
            event_time = parse_event_time(parts["date"], parts["time"])
        else:
            simple_match = SIMPLE_FILE_RE.match(csv_path.name)
            if not simple_match:
                ignored += 1
                continue

            parts = simple_match.groupdict()
            channel = (
                LASER_NAME_TO_CHANNEL[parts["laser"].lower()]
                if parts.get("laser")
                else int(parts["channel"])
            )

            lead_num = 1
            product_id = f"battery_{int(parts['battery_id'])}"
            line_id = "LINE_01"
            batch_id = parts["date"]
            sequence_id = parts["battery_id"]
            instance_id = f"{parts['date']}_{product_id}"
            event_time = parse_event_time(parts["date"], "000000")

        if instance_id not in records:
            records[instance_id] = ProductRecord(
                product_instance_id=instance_id,
                product_id=product_id,
                line_id=line_id,
                batch_id=batch_id,
                sequence_id=sequence_id,
                event_time=event_time,
            )

        records[instance_id].add_file(lead_num, channel, csv_path)

    result = sorted(records.values(), key=lambda r: (r.event_time, r.line_id, r.product_instance_id))
    logger.info(
        "Scanned %s product instances from %s. complete=%s ignored_files=%s",
        len(result),
        data_dir,
        sum(1 for r in result if r.is_complete),
        ignored,
    )
    return result


def load_signal(path: Path) -> np.ndarray:
    """
    Load one CSV signal as float32.

    The expected format is one numeric column. If multiple numeric columns are
    present, the last column is used because exported measurement files often
    include an index/time column before the signal column.
    """
    data = np.genfromtxt(str(path), delimiter=",", dtype=np.float32)
    if data.size == 0:
        raise ValueError("empty signal file")

    if data.ndim == 2:
        data = data[:, -1]

    data = np.asarray(data, dtype=np.float32).reshape(-1)
    data = data[np.isfinite(data)]
    if data.size == 0:
        raise ValueError("no finite numeric samples")
    return data


def make_message(
    item: PublishItem,
    lead_num: int,
    channel: int,
    file_path: Path,
    samples: np.ndarray,
    chunk_index: int,
    chunk_size: int,
    total_chunks: int,
) -> dict:
    start = chunk_index * chunk_size
    end = min(start + chunk_size, len(samples))
    chan_code = channel_code(channel)
    message_id = (
        f"{item.product_instance_id}:L{lead_num:02d}:"
        f"{chan_code}:{chunk_index:06d}"
    )

    return {
        "message_id": message_id,
        "product_instance_id": item.product_instance_id,
        "product_id": item.record.product_id,
        "line_id": item.record.line_id,
        "batch_id": item.record.batch_id,
        "sequence_id": item.record.sequence_id,
        "lead_num": lead_num,
        "lead_type": lead_type_from_num(lead_num),
        "channel": channel,
        "channel_name": channel_name(channel),
        "chunk_index": chunk_index,
        "total_chunks": total_chunks,
        "is_last_chunk": chunk_index == total_chunks - 1,
        "sample_rate_hz": SAMPLE_RATE_HZ,
        "start_sample": start,
        "end_sample": end,
        "samples": samples[start:end].tolist(),
        "event_time": isoformat_z(item.event_time),
        "metadata": {
            "source": "file_replay_producer",
            "version": PRODUCER_VERSION,
            "file_name": file_path.name,
            "original_product_instance_id": item.original_product_instance_id
            or item.record.product_instance_id,
            "is_duplicate": item.is_duplicate,
            "replay_iteration": item.replay_iteration,
        },
    }


def build_producer(kafka_bootstrap: str, retries: int = 8) -> KafkaProducer:
    """Create KafkaProducer with at-least-once delivery settings."""
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap,
                acks="all",
                retries=10,
                retry_backoff_ms=500,
                request_timeout_ms=30000,
                max_in_flight_requests_per_connection=1,
                compression_type=os.getenv("KAFKA_COMPRESSION", "gzip"),
                linger_ms=20,
                batch_size=65536,
                max_request_size=MAX_REQUEST_SIZE,
                key_serializer=lambda value: value.encode("utf-8"),
                value_serializer=lambda value: json.dumps(
                    value, ensure_ascii=False, separators=(",", ":")
                ).encode("utf-8"),
            )
            logger.info("Connected to Kafka bootstrap=%s", kafka_bootstrap)
            return producer
        except NoBrokersAvailable as exc:
            last_error = exc
            logger.warning(
                "Kafka is not ready. attempt=%s/%s bootstrap=%s",
                attempt,
                retries,
                kafka_bootstrap,
            )
            time.sleep(3)

    raise RuntimeError(f"failed to connect to Kafka: {last_error}")


def on_send_success(metadata) -> None:
    logger.debug(
        "sent topic=%s partition=%s offset=%s",
        metadata.topic,
        metadata.partition,
        metadata.offset,
    )


def on_send_error(exc: KafkaError) -> None:
    logger.error("Kafka send failed: %s", exc)


def make_publish_items(
    records: list[ProductRecord],
    target_products: int,
    max_products: int | None,
    duplicate_interval_seconds: int = 3,
) -> list[PublishItem]:
    """Create original and replay items for demo/load-test scenarios."""
    selected = records[:max_products] if max_products else records
    items = [
        PublishItem(
            record=record,
            product_instance_id=record.product_instance_id,
            event_time=record.event_time,
            is_duplicate=False,
            original_product_instance_id=record.product_instance_id,
            replay_iteration=0,
        )
        for record in selected
    ]

    if target_products <= 0 or target_products <= len(items):
        return items

    if not selected:
        return items

    needed = target_products - len(items)
    for index in range(needed):
        source = selected[index % len(selected)]
        replay_no = (index // len(selected)) + 1
        duplicate_id = f"{source.product_instance_id}_R{index + 1:05d}"
        event_time = source.event_time + timedelta(
            seconds=(index + 1) * duplicate_interval_seconds
        )
        items.append(
            PublishItem(
                record=source,
                product_instance_id=duplicate_id,
                event_time=event_time,
                is_duplicate=True,
                original_product_instance_id=source.product_instance_id,
                replay_iteration=replay_no,
            )
        )

    return sorted(items, key=lambda item: (item.event_time, item.record.line_id, item.product_instance_id))


def publish_product(
    producer: KafkaProducer,
    item: PublishItem,
    topic: str,
    chunk_size: int,
    speed: float,
) -> int:
    """Publish all available leads/channels for one product instance."""
    sent = 0
    record = item.record

    for lead_num in sorted(record.leads_present):
        for channel in (0, 1):
            file_path = record.files[channel].get(lead_num)
            if file_path is None:
                logger.warning(
                    "Missing channel. product=%s lead=%s channel=%s",
                    item.product_instance_id,
                    lead_num,
                    channel,
                )
                continue

            try:
                signal = load_signal(file_path)
            except Exception as exc:
                logger.error("Failed to load %s: %s", file_path, exc)
                continue

            total_chunks = max(1, int(np.ceil(len(signal) / chunk_size)))
            chan_code = channel_code(channel)
            partition_key = (
                f"{record.line_id}_{item.product_instance_id}_"
                f"L{lead_num:02d}_{chan_code}"
            )

            for chunk_index in range(total_chunks):
                message = make_message(
                    item=item,
                    lead_num=lead_num,
                    channel=channel,
                    file_path=file_path,
                    samples=signal,
                    chunk_index=chunk_index,
                    chunk_size=chunk_size,
                    total_chunks=total_chunks,
                )
                producer.send(topic, key=partition_key, value=message).add_callback(
                    on_send_success
                ).add_errback(on_send_error)
                sent += 1

                if speed > 0:
                    delay_seconds = (chunk_size / SAMPLE_RATE_HZ) / speed
                    if delay_seconds >= 0.002:
                        time.sleep(delay_seconds)

    return sent


def run(args: argparse.Namespace) -> int:
    records = scan_data_dir(args.data_dir)
    if args.only_complete:
        records = [record for record in records if record.is_complete]
        logger.info("Filtered to complete product instances: %s", len(records))

    if not records:
        logger.error("No matching product files found.")
        return 2

    items = make_publish_items(
        records=records,
        target_products=args.target_products,
        max_products=args.max_products,
    )

    if args.dry_run:
        original_count = sum(1 for item in items if not item.is_duplicate)
        duplicate_count = sum(1 for item in items if item.is_duplicate)
        lines = sorted({item.record.line_id for item in items})
        print("Dry run summary")
        print(f"- data_dir: {args.data_dir}")
        print(f"- product_instances: {len(items)}")
        print(f"- originals: {original_count}")
        print(f"- duplicates: {duplicate_count}")
        print(f"- lines: {', '.join(lines)}")
        print(f"- topic: {args.topic}")
        return 0

    producer = build_producer(args.kafka)

    try:
        while True:
            total_messages = 0
            for index, item in enumerate(items, start=1):
                sent = publish_product(
                    producer=producer,
                    item=item,
                    topic=args.topic,
                    chunk_size=args.chunk_size,
                    speed=args.speed,
                )
                total_messages += sent
                logger.info(
                    "[%s/%s] product=%s line=%s duplicate=%s messages=%s",
                    index,
                    len(items),
                    item.product_instance_id,
                    item.record.line_id,
                    item.is_duplicate,
                    sent,
                )

            producer.flush()
            logger.info("Run complete. total_messages=%s", total_messages)

            if not args.loop:
                break
            logger.info("Loop mode enabled. Restarting replay in 5 seconds.")
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed.")

    return 0


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Welding CSV to Kafka producer",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--data-dir", required=True, help="CSV root directory")
    parser.add_argument("--kafka", default=DEFAULT_KAFKA, help="Kafka bootstrap server")
    parser.add_argument("--topic", default=TOPIC_RAW, help="Kafka topic")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--speed", type=float, default=50.0, help="Replay speed multiplier")
    parser.add_argument(
        "--target-products",
        type=int,
        default=0,
        help="Replay duplicate product instances until this count is reached",
    )
    parser.add_argument(
        "--max-products",
        type=int,
        default=None,
        help="Limit original product instances for a short demo",
    )
    parser.add_argument(
        "--only-complete",
        action="store_true",
        help="Publish only products with 16 Laser A/B lead pairs",
    )
    parser.add_argument("--loop", action="store_true", help="Replay continuously")
    parser.add_argument("--dry-run", action="store_true", help="Scan and print summary only")
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(run(parse_args()))
