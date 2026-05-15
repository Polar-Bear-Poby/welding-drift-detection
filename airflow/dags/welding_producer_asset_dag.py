"""
welding_producer_asset_dag.py
=============================
Producer domain DAG (Asset-based).

Responsibilities:
- Validate run parameters.
- Run per-line producer containers (1 line = 1 producer).
- Verify broker offsets increased.
- Write producer heartbeat.
- Publish PRODUCER_DONE_ASSET.
"""

from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
import uuid
from datetime import datetime, timedelta

import psycopg2
from airflow.sdk import Param, dag, task
from welding_assets import PRODUCER_DONE_ASSET

log = logging.getLogger(__name__)

_PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
_PG_PORT = os.getenv("POSTGRES_PORT", "5432")
_PG_DB = os.getenv("POSTGRES_DB", "welding_drift")
_PG_USER = os.getenv("POSTGRES_USER", "welding")
_PG_PASS = os.getenv("POSTGRES_PASSWORD", "")
DB_CONN_STR = (
    f"host={_PG_HOST} port={_PG_PORT} dbname={_PG_DB} "
    f"user={_PG_USER} password={_PG_PASS}"
)

DEFAULT_LINE_COUNT = int(os.getenv("REPLAY_LINE_COUNT", "3"))
DEFAULT_LINE_SEEDS = os.getenv("REPLAY_LINE_SEEDS", "42,73,128")
DEFAULT_REPLAY_SPEED = float(os.getenv("REPLAY_SPEED", "1.0"))
PRODUCER_TIMEOUT_SEC = int(os.getenv("REPLAY_PRODUCER_TIMEOUT_SEC", "7200"))
DEFAULT_TARGET_PRODUCTS_TOTAL = int(os.getenv("REPLAY_TARGET_PRODUCTS_TOTAL", "0"))
DEFAULT_EXPECTED_CHANNELS = int(os.getenv("EXPECTED_CHANNELS_PER_PRODUCT", "2"))
DEFAULT_CHANNEL_SCOPE = os.getenv("CHANNEL_SCOPE", "combined").strip() or "combined"

PRODUCER_CONTAINER = os.getenv("PRODUCER_CONTAINER", "welding-producer")
PRODUCER_IMAGE = os.getenv("PRODUCER_IMAGE", "welding-kafka-submission-producer:latest")
NETWORK_NAME = os.getenv("NETWORK_NAME", "welding-kafka-submission_welding-net")
KAFKA_CONTAINER = os.getenv("KAFKA_CONTAINER", "welding-kafka")
DATA_DIR_CONTAINER = os.getenv("DATA_DIR_CONTAINER", "/data")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

TOPIC_RAW = os.getenv("TOPIC_RAW", "welding.raw.v1")
TOPIC_RAW_LASER_A = os.getenv("TOPIC_RAW_LASER_A", "welding.raw.laser_a.v1").strip()
TOPIC_RAW_LASER_B = os.getenv("TOPIC_RAW_LASER_B", "welding.raw.laser_b.v1").strip()


def _as_bool(value, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _topics_for_delivery_check() -> list[str]:
    topics: list[str] = []
    for topic in (TOPIC_RAW_LASER_A, TOPIC_RAW_LASER_B, TOPIC_RAW):
        if topic and topic not in topics:
            topics.append(topic)
    return topics


def _topic_offset_sum(topic: str) -> int:
    cmd = (
        f"docker exec {KAFKA_CONTAINER} sh -lc "
        f"\"kafka-get-offsets --bootstrap-server {KAFKA_BOOTSTRAP} --topic {topic}\""
    )
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=60)
    if proc.returncode != 0:
        return 0
    total = 0
    for line in (proc.stdout or "").strip().splitlines():
        parts = line.strip().split(":")
        if len(parts) != 3:
            continue
        try:
            total += int(parts[2])
        except ValueError:
            continue
    return total


def _offset_snapshot(topics: list[str]) -> dict[str, int]:
    return {topic: _topic_offset_sum(topic) for topic in topics}


@dag(
    dag_id="welding_producer_asset_dag",
    schedule=None,
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["welding", "asset", "producer"],
    default_args={"owner": "welding-team", "retries": 2, "retry_delay": timedelta(minutes=2)},
    params={
        "experimental": Param(default=False, type="boolean"),
        "target_date": Param(default="", type="string"),
        "line_count": Param(default=DEFAULT_LINE_COUNT, type="integer"),
        "producer_count": Param(default=DEFAULT_LINE_COUNT, type="integer"),
        "replay_speed": Param(default=DEFAULT_REPLAY_SPEED, type="number"),
        "line_seeds": Param(default=DEFAULT_LINE_SEEDS, type="string"),
        "target_products_total": Param(default=DEFAULT_TARGET_PRODUCTS_TOTAL, type="integer"),
        "expected_channels": Param(default=DEFAULT_EXPECTED_CHANNELS, type="integer"),
        "channel_scope": Param(default=DEFAULT_CHANNEL_SCOPE, type="string"),
    },
)
def welding_producer_asset_dag():
    @task()
    def prepare_context(dag_run=None, params=None) -> dict:
        conf = dict((dag_run.conf or {})) if dag_run else {}
        params = dict(params or {})

        experimental = _as_bool(
            conf.get("experimental", params.get("experimental", os.getenv("EXPERIMENT_MODE", "0")))
        )
        target_products_total = int(
            conf.get("target_products_total", params.get("target_products_total", DEFAULT_TARGET_PRODUCTS_TOTAL))
            or 0
        )
        line_count = int(conf.get("line_count", params.get("line_count", DEFAULT_LINE_COUNT)) or 0)
        producer_count = int(conf.get("producer_count", params.get("producer_count", line_count)) or 0)
        replay_speed = float(conf.get("replay_speed", params.get("replay_speed", DEFAULT_REPLAY_SPEED)) or 0)
        line_seeds = str(conf.get("line_seeds", params.get("line_seeds", DEFAULT_LINE_SEEDS))).strip()
        requested_date_raw = str(conf.get("target_date", params.get("target_date", ""))).strip()
        expected_channels = int(
            conf.get("expected_channels", params.get("expected_channels", DEFAULT_EXPECTED_CHANNELS))
            or DEFAULT_EXPECTED_CHANNELS
        )
        channel_scope = str(conf.get("channel_scope", params.get("channel_scope", DEFAULT_CHANNEL_SCOPE))).strip()

        if line_count < 1:
            raise ValueError(f"line_count must be >= 1 (got {line_count})")
        if producer_count < 1:
            raise ValueError(f"producer_count must be >= 1 (got {producer_count})")
        if producer_count != line_count:
            raise ValueError(
                "producer_count must equal line_count "
                f"(producer_count={producer_count}, line_count={line_count})"
            )
        if replay_speed <= 0:
            raise ValueError(f"replay_speed must be > 0 (got {replay_speed})")
        if expected_channels < 1:
            raise ValueError(f"expected_channels must be >= 1 (got {expected_channels})")
        if channel_scope not in {"single", "laser_a", "laser_b", "combined"}:
            raise ValueError(
                "channel_scope must be one of: single, laser_a, laser_b, combined "
                f"(got {channel_scope})"
            )

        discover_code = (
            "import pathlib, re\n"
            f"root = pathlib.Path('{DATA_DIR_CONTAINER}')\n"
            "pat = re.compile(r'^20\\d{6}$')\n"
            "candidates = []\n"
            "if root.exists():\n"
            "  for p in root.iterdir():\n"
            "    if not p.is_dir() or not pat.match(p.name):\n"
            "      continue\n"
            "    has_csv = any(x.suffix.lower()=='.csv' for x in p.rglob('*.csv'))\n"
            "    if has_csv:\n"
            "      candidates.append(p.name)\n"
            "print(sorted(candidates)[0] if candidates else '', end='')\n"
        )
        discover_cmd = (
            f"docker run --rm --entrypoint python --volumes-from {PRODUCER_CONTAINER} {PRODUCER_IMAGE} "
            f"-c \"{discover_code}\""
        )
        proc = subprocess.run(discover_cmd, shell=True, capture_output=True, text=True, timeout=120)
        if proc.returncode != 0:
            raise RuntimeError(f"Failed to discover data date: {proc.stderr.strip()}")

        discovered_target_raw = (proc.stdout or "").strip()
        target_raw = requested_date_raw or discovered_target_raw
        if not target_raw:
            raise ValueError("No target_date found and no data date folder discovered")
        target_iso = f"{target_raw[0:4]}-{target_raw[4:6]}-{target_raw[6:8]}"

        return {
            "run_id": str(uuid.uuid4()),
            "target_date_raw": target_raw,
            "target_date_iso": target_iso,
            "experimental": experimental,
            "target_products_total": target_products_total,
            "line_count": line_count,
            "producer_count": producer_count,
            "replay_speed": replay_speed,
            "line_seeds": line_seeds,
            "expected_channels": expected_channels,
            "channel_scope": channel_scope,
        }

    @task()
    def build_line_plan(ctx: dict) -> list[int]:
        return list(range(1, int(ctx["line_count"]) + 1))

    @task()
    def snapshot_offsets_before() -> dict:
        topics = _topics_for_delivery_check()
        return {"topics": topics, "before": _offset_snapshot(topics)}

    @task()
    def run_producer_for_line(line_number: int, ctx: dict):
        target_raw = ctx["target_date_raw"]
        shard_index = line_number - 1
        shard_total = int(ctx["line_count"])
        target_products_total = int(ctx.get("target_products_total") or 0)
        per_line_target = 0
        if target_products_total > 0:
            base = target_products_total // shard_total
            rem = target_products_total % shard_total
            per_line_target = base + (1 if shard_index < rem else 0)

        cmd_parts = [
            "docker",
            "run",
            "--rm",
            "--network",
            NETWORK_NAME,
            "--volumes-from",
            PRODUCER_CONTAINER,
            PRODUCER_IMAGE,
            "--data-dir",
            f"{DATA_DIR_CONTAINER}/{target_raw}",
            "--kafka",
            KAFKA_BOOTSTRAP,
            "--line-count",
            "1",
            "--line-number",
            str(line_number),
            "--shard-index",
            str(shard_index),
            "--shard-total",
            str(shard_total),
            "--line-seed",
            str(ctx.get("line_seeds") or DEFAULT_LINE_SEEDS),
            "--speed",
            str(ctx.get("replay_speed") or DEFAULT_REPLAY_SPEED),
            "--ingest-run-id",
            str(ctx.get("run_id") or ""),
        ]
        if TOPIC_RAW:
            cmd_parts.extend(["--topic", TOPIC_RAW])
        if TOPIC_RAW_LASER_A:
            cmd_parts.extend(["--topic-laser-a", TOPIC_RAW_LASER_A])
        if TOPIC_RAW_LASER_B:
            cmd_parts.extend(["--topic-laser-b", TOPIC_RAW_LASER_B])
        if per_line_target > 0:
            cmd_parts.extend(["--target-products", str(per_line_target)])

        log.info("producer line=%s cmd=%s", line_number, " ".join(shlex.quote(x) for x in cmd_parts))
        result = subprocess.run(cmd_parts, timeout=PRODUCER_TIMEOUT_SEC)
        if result.returncode != 0:
            raise RuntimeError(f"producer failed for line={line_number}, returncode={result.returncode}")
        return {"line_number": line_number, "status": "success"}

    @task()
    def confirm_delivery(producer_results: list[dict], offset_before_meta: dict, ctx: dict) -> dict:
        del producer_results
        topics = offset_before_meta["topics"]
        before = offset_before_meta["before"]
        after = _offset_snapshot(topics)
        delta_by_topic = {topic: int(after.get(topic, 0)) - int(before.get(topic, 0)) for topic in topics}
        total_delta = sum(delta_by_topic.values())
        if total_delta <= 0:
            raise RuntimeError(f"producer delivery check failed: no offset increase. delta={delta_by_topic}")
        return {**ctx, "delivery_topics": topics, "offset_before": before, "offset_after": after, "offset_delta": delta_by_topic, "offset_total_delta": total_delta}

    @task()
    def emit_producer_heartbeat(meta: dict) -> dict:
        run_id = str(meta.get("run_id") or "").strip()
        target_date_iso = str(meta.get("target_date_iso") or "").strip()
        expected_channels = int(meta.get("expected_channels") or DEFAULT_EXPECTED_CHANNELS)
        channel_scope = str(meta.get("channel_scope") or DEFAULT_CHANNEL_SCOPE).strip()
        target_products_total = int(meta.get("target_products_total") or 0)
        if not run_id:
            raise RuntimeError("producer heartbeat requires non-empty run_id")
        if not target_date_iso:
            raise RuntimeError("producer heartbeat requires non-empty target_date_iso")
        if expected_channels < 1:
            raise RuntimeError(f"producer heartbeat expected_channels must be >=1 (got {expected_channels})")
        if channel_scope not in {"single", "laser_a", "laser_b", "combined"}:
            raise RuntimeError(
                f"producer heartbeat channel_scope invalid: {channel_scope} "
                "(allowed: single, laser_a, laser_b, combined)"
            )
        if target_products_total < 0:
            raise RuntimeError(
                f"producer heartbeat target_products_total must be >=0 (got {target_products_total})"
            )
        details = {
            "status": "PRODUCER_DONE",
            "run_id": run_id,
            "target_date": target_date_iso,
            "line_count": int(meta["line_count"]),
            "producer_count": int(meta["producer_count"]),
            "target_products_total": target_products_total,
            "replay_speed": float(meta["replay_speed"]),
            "experimental": bool(meta["experimental"]),
            "expected_channels": expected_channels,
            "channel_scope": channel_scope,
            "offset_total_delta": int(meta["offset_total_delta"]),
            "offset_delta": meta["offset_delta"],
        }
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO welding.pipeline_heartbeat (component_name, details)
                    VALUES (%s, %s::jsonb)
                    """,
                    ("airflow.welding_producer_asset_dag", json.dumps(details)),
                )
        return meta

    @task(outlets=[PRODUCER_DONE_ASSET])
    def publish_producer_asset(meta: dict) -> dict:
        log.info(
            "Publishing producer asset: run_id=%s target_date=%s",
            meta.get("run_id"),
            meta.get("target_date_iso"),
        )
        return meta

    ctx = prepare_context()
    line_plan = build_line_plan(ctx)
    offset_before = snapshot_offsets_before()
    producers = run_producer_for_line.partial(ctx=ctx).expand(line_number=line_plan)
    delivered = confirm_delivery(producers, offset_before, ctx)
    heartbeat = emit_producer_heartbeat(delivered)
    publish_producer_asset(heartbeat)


welding_producer_asset_dag()
