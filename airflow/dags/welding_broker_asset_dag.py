"""
welding_broker_asset_dag.py
===========================
Broker domain DAG (Asset-based).

Triggered by PRODUCER_DONE_ASSET, verifies broker/topic/group health, then
publishes BROKER_READY_ASSET.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
from datetime import datetime, timedelta

import psycopg2
from airflow.decorators import dag, task
from welding_assets import BROKER_READY_ASSET, PRODUCER_DONE_ASSET

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

KAFKA_CONTAINER = os.getenv("KAFKA_CONTAINER", "welding-kafka")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_RAW = os.getenv("TOPIC_RAW", "welding.raw.v1")
TOPIC_RAW_LASER_A = os.getenv("TOPIC_RAW_LASER_A", "welding.raw.laser_a.v1")
TOPIC_RAW_LASER_B = os.getenv("TOPIC_RAW_LASER_B", "welding.raw.laser_b.v1")
CONSUMER_GROUP_LASER_A = os.getenv("CONSUMER_GROUP_LASER_A", "welding-stream-laser-a")
CONSUMER_GROUP_LASER_B = os.getenv("CONSUMER_GROUP_LASER_B", "welding-stream-laser-b")
CONSUMER_LAG_ALERT_THRESHOLD = int(os.getenv("CONSUMER_LAG_ALERT_THRESHOLD", "5000"))


def _topic_exists(topic: str) -> bool:
    cmd = f"docker exec {KAFKA_CONTAINER} kafka-topics --bootstrap-server {KAFKA_BOOTSTRAP} --topic {topic} --describe"
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
    return proc.returncode == 0 and topic in (proc.stdout or "")


def _group_lag(group_id: str) -> dict:
    cmd = (
        f"docker exec {KAFKA_CONTAINER} kafka-consumer-groups --bootstrap-server {KAFKA_BOOTSTRAP} "
        f"--group {group_id} --describe"
    )
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
    stderr_text = (proc.stderr or "").strip()
    lines = [l for l in (proc.stdout or "").splitlines() if l.strip() and not l.startswith("GROUP")]
    total_lag = 0
    max_lag = 0
    parse_errors = 0
    valid_rows = 0
    for line in lines:
        parts = line.split()
        if len(parts) < 6:
            parse_errors += 1
            continue
        token = parts[5]
        if token == "-":
            continue
        if not token.lstrip("-").isdigit():
            parse_errors += 1
            continue
        lag_val = int(token)
        if lag_val < 0:
            parse_errors += 1
            continue
        total_lag += lag_val
        max_lag = max(max_lag, lag_val)
        valid_rows += 1
    return {
        "total_lag": total_lag,
        "max_lag": max_lag,
        "parse_errors": parse_errors,
        "valid_rows": valid_rows,
        "cli_returncode": proc.returncode,
        "stderr": stderr_text,
        "healthy": (valid_rows == 0) or (max_lag <= CONSUMER_LAG_ALERT_THRESHOLD),
    }


@dag(
    dag_id="welding_broker_asset_dag",
    schedule=[PRODUCER_DONE_ASSET],
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["welding", "asset", "broker"],
    default_args={"owner": "welding-team", "retries": 1, "retry_delay": timedelta(minutes=1)},
)
def welding_broker_asset_dag():
    @task()
    def read_latest_producer_context() -> dict:
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT details
                    FROM welding.pipeline_heartbeat
                    WHERE component_name = 'airflow.welding_producer_asset_dag'
                      AND details->>'status' = 'PRODUCER_DONE'
                    ORDER BY heartbeat_at DESC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
        if not row or not isinstance(row[0], dict):
            raise RuntimeError("No producer heartbeat found for broker checks")
        details = row[0]
        run_id = str(details.get("run_id") or "").strip()
        target_date_iso = str(details.get("target_date") or "").strip()
        expected_channels = int(details.get("expected_channels", 0) or 0)
        channel_scope = str(details.get("channel_scope") or "").strip()
        target_products_total = int(details.get("target_products_total", -1) or -1)

        missing = []
        if not run_id:
            missing.append("run_id")
        if not target_date_iso:
            missing.append("target_date")
        if expected_channels < 1:
            missing.append("expected_channels")
        if channel_scope not in {"single", "laser_a", "laser_b", "combined"}:
            missing.append("channel_scope")
        if target_products_total < 0:
            missing.append("target_products_total")
        if missing:
            raise RuntimeError(
                "Invalid producer heartbeat (missing/invalid required fields): "
                f"{missing}, details={details}"
            )

        return {
            "run_id": run_id,
            "target_date_iso": target_date_iso,
            "experimental": bool(details.get("experimental", False)),
            "target_products_total": target_products_total,
            "expected_channels": expected_channels,
            "channel_scope": channel_scope,
        }

    @task()
    def validate_topics(ctx: dict) -> dict:
        missing = []
        for topic in [TOPIC_RAW, TOPIC_RAW_LASER_A, TOPIC_RAW_LASER_B]:
            if topic and not _topic_exists(topic):
                missing.append(topic)
        if missing:
            raise RuntimeError(f"Broker topic missing: {missing}")
        return ctx

    @task()
    def validate_consumer_lag(ctx: dict) -> dict:
        lag_a = _group_lag(CONSUMER_GROUP_LASER_A)
        lag_b = _group_lag(CONSUMER_GROUP_LASER_B)
        if not lag_a["healthy"] or not lag_b["healthy"]:
            raise RuntimeError(
                "Broker lag unhealthy: "
                f"{CONSUMER_GROUP_LASER_A}={lag_a}, {CONSUMER_GROUP_LASER_B}={lag_b}"
            )
        if lag_a["valid_rows"] == 0 or lag_b["valid_rows"] == 0:
            log.warning(
                "Consumer lag rows unavailable (accepted as unknown in local mode): %s=%s, %s=%s",
                CONSUMER_GROUP_LASER_A,
                lag_a,
                CONSUMER_GROUP_LASER_B,
                lag_b,
            )
        return {**ctx, "lag_a": lag_a, "lag_b": lag_b}

    @task()
    def emit_broker_heartbeat(meta: dict) -> dict:
        details = {
            "status": "BROKER_READY",
            "run_id": meta.get("run_id"),
            "target_date": meta.get("target_date_iso"),
            "experimental": bool(meta.get("experimental", False)),
            "target_products_total": int(meta.get("target_products_total", 0) or 0),
            "expected_channels": int(meta.get("expected_channels", 1) or 1),
            "channel_scope": str(meta.get("channel_scope") or "single"),
            "lag_a": meta.get("lag_a"),
            "lag_b": meta.get("lag_b"),
        }
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO welding.pipeline_heartbeat (component_name, details)
                    VALUES (%s, %s::jsonb)
                    """,
                    ("airflow.welding_broker_asset_dag", json.dumps(details)),
                )
        return meta

    @task(outlets=[BROKER_READY_ASSET])
    def publish_broker_asset(meta: dict) -> dict:
        log.info("Publishing broker asset: run_id=%s", meta.get("run_id"))
        return meta

    producer_ctx = read_latest_producer_context()
    topics_ok = validate_topics(producer_ctx)
    lag_ok = validate_consumer_lag(topics_ok)
    hb = emit_broker_heartbeat(lag_ok)
    publish_broker_asset(hb)


welding_broker_asset_dag()
