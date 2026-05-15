"""
welding_consumer_asset_dag.py
=============================
Consumer domain DAG (Asset-based).

Triggered by BROKER_READY_ASSET:
- Do NOT run spark_batch.py (streaming path only).
- Wait for stage_event completion written by spark_streaming.py.
- Validate coverage checks from pattern tables.
- Write consumer heartbeat.
- Publish CONSUMER_PROCESSED_ASSET.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta

import psycopg2
from airflow.sdk import dag, task
from welding_assets import BROKER_READY_ASSET, CONSUMER_PROCESSED_ASSET

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

STAGE_EVENT_WAIT_TIMEOUT_SEC = int(os.getenv("STAGE_EVENT_WAIT_TIMEOUT_SEC", "3600"))
STAGE_EVENT_POLL_INTERVAL_SEC = int(os.getenv("STAGE_EVENT_POLL_INTERVAL_SEC", "10"))
BROKER_FALLBACK_WINDOW_MIN = int(os.getenv("BROKER_FALLBACK_WINDOW_MIN", "30"))
STAGE_EVENT_PROGRESS_LOG_INTERVAL_SEC = int(os.getenv("STAGE_EVENT_PROGRESS_LOG_INTERVAL_SEC", "30"))


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


ALLOW_LATEST_BROKER_FALLBACK = _as_bool(os.getenv("ALLOW_LATEST_BROKER_FALLBACK", "1"), True)
BROKER_FALLBACK_REQUIRE_UNIQUE = _as_bool(os.getenv("BROKER_FALLBACK_REQUIRE_UNIQUE", "1"), True)


def _normalize_expected_channels(value) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = 1
    return parsed if parsed >= 1 else 1


def _wait_stage_event_success(run_id: str, stage_name: str) -> dict:
    deadline = time.time() + max(STAGE_EVENT_WAIT_TIMEOUT_SEC, 30)
    last_progress_log_at = 0.0
    last_seen_status = "NOT_FOUND"
    last_seen_detail = "{}"
    while time.time() < deadline:
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, COALESCE(detail_json::text, '{}')
                    FROM welding.stage_event
                    WHERE run_id = %s::uuid AND stage_name = %s
                    """,
                    (run_id, stage_name),
                )
                row = cur.fetchone()
        if row is None:
            now = time.time()
            if now - last_progress_log_at >= max(STAGE_EVENT_PROGRESS_LOG_INTERVAL_SEC, 5):
                with psycopg2.connect(DB_CONN_STR) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT stage_name, status, ended_at
                            FROM welding.stage_event
                            WHERE run_id = %s::uuid
                            ORDER BY ended_at DESC NULLS LAST, created_at DESC
                            LIMIT 4
                            """,
                            (run_id,),
                        )
                        recent = cur.fetchall() or []
                log.info(
                    "Waiting stage_event. run_id=%s stage=%s recent=%s",
                    run_id,
                    stage_name,
                    recent,
                )
                last_progress_log_at = now
            time.sleep(max(STAGE_EVENT_POLL_INTERVAL_SEC, 1))
            continue

        status, detail_text = row
        last_seen_status = str(status or "")
        last_seen_detail = detail_text or "{}"
        if status == "SUCCESS":
            try:
                return json.loads(detail_text or "{}")
            except Exception:
                return {"raw": detail_text}
        if status == "FAILED":
            detail_obj = {}
            try:
                detail_obj = json.loads(detail_text or "{}")
            except Exception:
                detail_obj = {"raw": detail_text}
            raise RuntimeError(
                f"run_id={run_id} stage_event={stage_name} FAILED "
                f"error_code={detail_obj.get('error_code') or 'unknown'} "
                f"message={detail_obj.get('error_message') or detail_obj}"
            )

        now = time.time()
        if now - last_progress_log_at >= max(STAGE_EVENT_PROGRESS_LOG_INTERVAL_SEC, 5):
            detail_obj = {}
            try:
                detail_obj = json.loads(detail_text or "{}")
            except Exception:
                detail_obj = {"raw": detail_text}
            log.info(
                "Waiting stage_event. run_id=%s stage=%s status=%s detail=%s",
                run_id,
                stage_name,
                status,
                detail_obj,
            )
            last_progress_log_at = now

        time.sleep(max(STAGE_EVENT_POLL_INTERVAL_SEC, 1))

    raise TimeoutError(
        f"run_id={run_id} stage_event={stage_name} did not reach SUCCESS "
        f"within {STAGE_EVENT_WAIT_TIMEOUT_SEC}s "
        f"(last_status={last_seen_status}, last_detail={last_seen_detail})"
    )


@dag(
    dag_id="welding_consumer_asset_dag",
    schedule=[BROKER_READY_ASSET],
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["welding", "asset", "consumer", "spark-streaming-only"],
    default_args={"owner": "welding-team", "retries": 1, "retry_delay": timedelta(minutes=2)},
)
def welding_consumer_asset_dag():
    @task()
    def prepare_run_context(dag_run=None, params=None) -> dict:
        conf = dict((dag_run.conf or {})) if dag_run else {}
        params = dict(params or {})

        run_id = str(conf.get("run_id") or params.get("run_id") or "").strip()
        target_raw = str(conf.get("target_date_raw") or params.get("target_date_raw") or "").strip()
        target_iso = str(conf.get("target_date_iso") or params.get("target_date_iso") or "").strip()
        experimental = _as_bool(conf.get("experimental", params.get("experimental", False)))
        target_products_total = int(conf.get("target_products_total", params.get("target_products_total", 0)) or 0)
        expected_channels = _normalize_expected_channels(
            conf.get("expected_channels", params.get("expected_channels", 1))
        )
        channel_scope = str(conf.get("channel_scope", params.get("channel_scope", "single"))).strip() or "single"

        if not run_id or not target_raw or not target_iso:
            if not ALLOW_LATEST_BROKER_FALLBACK:
                raise ValueError(
                    "run_id/target_date are required for consumer validation DAG "
                    "(latest BROKER_READY fallback is disabled)."
                )
            with psycopg2.connect(DB_CONN_STR) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT details, heartbeat_at
                        FROM welding.pipeline_heartbeat
                        WHERE component_name = 'airflow.welding_broker_asset_dag'
                          AND details->>'status' = 'BROKER_READY'
                          AND heartbeat_at >= NOW() - (%s * INTERVAL '1 minute')
                        ORDER BY heartbeat_at DESC
                        LIMIT 5
                        """
                        ,
                        (max(BROKER_FALLBACK_WINDOW_MIN, 1),),
                    )
                    rows = cur.fetchall() or []
            if BROKER_FALLBACK_REQUIRE_UNIQUE and rows:
                distinct_ids = {
                    str((item[0] or {}).get("run_id") or "").strip()
                    for item in rows
                    if isinstance(item[0], dict)
                }
                distinct_ids.discard("")
                if len(distinct_ids) > 1:
                    log.warning(
                        "Multiple run_ids in BROKER_READY fallback window: %s. Using most recent: %s",
                        sorted(distinct_ids),
                        (rows[0][0] or {}).get("run_id")
                    )
            if rows and isinstance(rows[0][0], dict):
                details = rows[0][0]
                run_id = run_id or str(details.get("run_id") or "").strip()
                target_iso = target_iso or str(details.get("target_date") or "").strip()
                if target_iso and not target_raw:
                    target_raw = target_iso.replace("-", "")
                experimental = _as_bool(details.get("experimental", experimental))
                target_products_total = int(details.get("target_products_total", target_products_total) or 0)
                expected_channels = _normalize_expected_channels(details.get("expected_channels", expected_channels))
                channel_scope = str(details.get("channel_scope", channel_scope)).strip() or channel_scope

        if not run_id:
            raise ValueError("run_id is required for consumer validation DAG.")
        if not target_raw and target_iso:
            target_raw = target_iso.replace("-", "")
        if not target_iso and target_raw:
            target_iso = f"{target_raw[0:4]}-{target_raw[4:6]}-{target_raw[6:8]}"

        return {
            "run_id": run_id,
            "target_date_raw": target_raw,
            "target_date_iso": target_iso,
            "experimental": experimental,
            "target_products_total": target_products_total,
            "expected_channels": expected_channels,
            "channel_scope": channel_scope,
            "source_dag_run_id": str(conf.get("source_dag_run_id") or ""),
        }

    @task(retries=1, retry_delay=timedelta(minutes=1))
    def wait_chunk_complete(context: dict) -> dict:
        detail = _wait_stage_event_success(context["run_id"], "chunk_complete")
        return {**context, "chunk_complete": detail}

    @task(retries=1, retry_delay=timedelta(minutes=1))
    def wait_segmentation_complete(context: dict) -> dict:
        detail = _wait_stage_event_success(context["run_id"], "segmentation_complete")
        return {**context, "segmentation_complete": detail}

    @task(retries=1, retry_delay=timedelta(minutes=1))
    def wait_inference_complete(context: dict) -> dict:
        detail = _wait_stage_event_success(context["run_id"], "inference_complete")
        return {**context, "inference_complete": detail}

    @task(retries=1, retry_delay=timedelta(minutes=1))
    def wait_load_complete(context: dict) -> dict:
        detail = _wait_stage_event_success(context["run_id"], "load_complete")
        return {**context, "load_complete": detail}

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def validate_segmentation_coverage(run_meta: dict) -> dict:
        run_id = run_meta["run_id"]
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH per_key AS (
                        SELECT product_id, channel, COUNT(*) AS segment_count
                        FROM welding.pattern_segment
                        WHERE run_id = %s::uuid
                        GROUP BY product_id, channel
                    )
                    SELECT
                        COUNT(*) AS key_count,
                        COUNT(*) FILTER (WHERE segment_count != 16) AS invalid_keys
                    FROM per_key
                    """,
                    (run_id,),
                )
                key_count, invalid_keys = cur.fetchone()
        if (key_count or 0) == 0:
            raise RuntimeError(f"run_id={run_id} has no pattern_segment rows")
        if (invalid_keys or 0) > 0:
            raise RuntimeError(
                f"run_id={run_id} segmentation coverage failed: invalid_keys={invalid_keys}"
            )
        return {**run_meta, "segmentation_keys": int(key_count)}

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def validate_inference_coverage(run_meta: dict) -> dict:
        run_id = run_meta["run_id"]
        expected_channels = _normalize_expected_channels(run_meta.get("expected_channels", 1))
        expected_segments = 16 * expected_channels
        experimental = bool(run_meta.get("experimental", False))
        load_decision = str((run_meta.get("load_complete") or {}).get("load_decision") or "")
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH per_product AS (
                        SELECT
                            product_id,
                            COUNT(*) AS total_segments,
                            COUNT(DISTINCT channel) AS channel_count
                        FROM welding.pattern_segment
                        WHERE run_id = %s::uuid
                        GROUP BY product_id
                    )
                    SELECT
                        COUNT(*) AS product_count,
                        COUNT(*) FILTER (
                            WHERE total_segments != %s OR channel_count != %s
                        ) AS invalid_products
                    FROM per_product
                    """,
                    (run_id, expected_segments, expected_channels),
                )
                product_count, invalid_products = cur.fetchone()
        if (product_count or 0) == 0:
            raise RuntimeError(f"run_id={run_id} has no per-product segment rows")
        if (invalid_products or 0) > 0:
            if experimental and load_decision == "partial_success_after_grace":
                log.warning(
                    "run_id=%s inference coverage partially accepted (experimental mode): "
                    "invalid_products=%s/%s expected_segments=%s expected_channels=%s",
                    run_id,
                    int(invalid_products or 0),
                    int(product_count or 0),
                    expected_segments,
                    expected_channels,
                )
            else:
                raise RuntimeError(
                    f"run_id={run_id} inference coverage failed: invalid_products={invalid_products}"
                )
        return {
            **run_meta,
            "inference_products": int(product_count),
            "inference_invalid_products": int(invalid_products or 0),
            "expected_channels": expected_channels,
            "expected_segments": expected_segments,
        }

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def validate_summary_completeness(run_meta: dict) -> dict:
        run_id = run_meta["run_id"]
        expected_channels = _normalize_expected_channels(run_meta.get("expected_channels", 1))
        experimental = bool(run_meta.get("experimental", False))
        load_decision = str((run_meta.get("load_complete") or {}).get("load_decision") or "")
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH per_product AS (
                        SELECT
                            product_id,
                            COUNT(*) AS summary_rows,
                            COUNT(DISTINCT channel) AS channel_count
                        FROM welding.pattern_summary
                        WHERE run_id = %s::uuid
                        GROUP BY product_id
                    )
                    SELECT
                        COUNT(*) AS product_count,
                        COUNT(*) FILTER (
                            WHERE summary_rows != %s OR channel_count != %s
                        ) AS invalid_products
                    FROM per_product
                    """,
                    (run_id, expected_channels, expected_channels),
                )
                product_count, invalid_products = cur.fetchone()

                cur.execute(
                    "SELECT COUNT(*), COUNT(DISTINCT product_id) FROM welding.pattern_segment WHERE run_id = %s::uuid",
                    (run_id,),
                )
                segment_rows, segment_products = cur.fetchone()

                cur.execute(
                    "SELECT COUNT(*), COUNT(DISTINCT product_id) FROM welding.pattern_summary WHERE run_id = %s::uuid",
                    (run_id,),
                )
                summary_rows, summary_products = cur.fetchone()
        if (product_count or 0) == 0:
            raise RuntimeError(f"run_id={run_id} has no pattern_summary rows")
        if (invalid_products or 0) > 0:
            if experimental and load_decision == "partial_success_after_grace":
                log.warning(
                    "run_id=%s summary completeness partially accepted (experimental mode): "
                    "invalid_products=%s/%s expected_channels=%s",
                    run_id,
                    int(invalid_products or 0),
                    int(product_count or 0),
                    expected_channels,
                )
            else:
                raise RuntimeError(
                    f"run_id={run_id} summary completeness failed: invalid_products={invalid_products}"
                )
        return {
            **run_meta,
            "summary_products": int(product_count),
            "summary_invalid_products": int(invalid_products or 0),
            "segment_rows": int(segment_rows or 0),
            "summary_rows": int(summary_rows or 0),
            "segment_products": int(segment_products or 0),
            "summary_products_distinct": int(summary_products or 0),
            "expected_channels": expected_channels,
        }

    @task()
    def emit_consumer_heartbeat(validation_result: dict) -> dict:
        details = {
            "target_date": validation_result.get("target_date_iso"),
            "status": "CONSUMER_PROCESSED",
            "spark_run_id": validation_result.get("run_id"),
            "segment_rows": validation_result.get("segment_rows"),
            "summary_rows": validation_result.get("summary_rows"),
            "experimental": bool(validation_result.get("experimental", False)),
            "target_products_total": int(validation_result.get("target_products_total") or 0),
            "expected_channels": int(validation_result.get("expected_channels") or 1),
            "channel_scope": str(validation_result.get("channel_scope") or "single"),
        }
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO welding.pipeline_heartbeat (component_name, details)
                    VALUES (%s, %s::jsonb)
                    """,
                    ("airflow.welding_consumer_asset_dag", json.dumps(details)),
                )
        return validation_result

    @task(outlets=[CONSUMER_PROCESSED_ASSET])
    def publish_consumer_asset(validation_result: dict) -> dict:
        log.info(
            "Publishing consumer asset: run_id=%s target_date=%s experimental=%s",
            validation_result.get("run_id"),
            validation_result.get("target_date_iso"),
            validation_result.get("experimental"),
        )
        return validation_result

    context = prepare_run_context()
    chunk_ok = wait_chunk_complete(context)
    seg_stage_ok = wait_segmentation_complete(chunk_ok)
    inf_stage_ok = wait_inference_complete(seg_stage_ok)
    load_stage_ok = wait_load_complete(inf_stage_ok)
    seg_cov_ok = validate_segmentation_coverage(load_stage_ok)
    inf_cov_ok = validate_inference_coverage(seg_cov_ok)
    summary_ok = validate_summary_completeness(inf_cov_ok)
    heartbeat = emit_consumer_heartbeat(summary_ok)
    publish_consumer_asset(heartbeat)


welding_consumer_asset_dag()
