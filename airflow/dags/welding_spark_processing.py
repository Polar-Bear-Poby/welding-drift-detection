"""
welding_spark_processing.py - Spark Processing (DAG 2)
=======================================================

Responsibilities:
- Wait for spark_streaming stage_event signals for a run_id.
- Validate coverage checks from pattern tables.
- Emit spark-processing heartbeat.
- Trigger post quality-control DAG (optional fallback).
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta

import psycopg2
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from welding_assets import SPARK_PROCESS_COMPLETED_ASSET

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

ENABLE_TRIGGER_FALLBACK = os.getenv("ENABLE_TRIGGER_FALLBACK", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
STAGE_EVENT_WAIT_TIMEOUT_SEC = int(os.getenv("STAGE_EVENT_WAIT_TIMEOUT_SEC", "3600"))
STAGE_EVENT_POLL_INTERVAL_SEC = int(os.getenv("STAGE_EVENT_POLL_INTERVAL_SEC", "10"))


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


def _wait_stage_event_success(run_id: str, stage_name: str) -> dict:
    deadline = time.time() + max(STAGE_EVENT_WAIT_TIMEOUT_SEC, 30)
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
            time.sleep(max(STAGE_EVENT_POLL_INTERVAL_SEC, 1))
            continue

        status, detail_text = row
        if status == "SUCCESS":
            try:
                return json.loads(detail_text or "{}")
            except Exception:
                return {"raw": detail_text}
        if status == "FAILED":
            raise RuntimeError(f"run_id={run_id} stage_event={stage_name} FAILED")

        time.sleep(max(STAGE_EVENT_POLL_INTERVAL_SEC, 1))

    raise TimeoutError(
        f"run_id={run_id} stage_event={stage_name} did not reach SUCCESS "
        f"within {STAGE_EVENT_WAIT_TIMEOUT_SEC}s"
    )


@dag(
    dag_id="welding_spark_processing",
    schedule=None,
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["welding", "airflow3", "spark", "processing"],
    default_args={"owner": "welding-team", "retries": 1, "retry_delay": timedelta(minutes=2)},
)
def welding_spark_processing_dag():
    @task()
    def prepare_run_context(dag_run=None, params=None) -> dict:
        conf = dict((dag_run.conf or {})) if dag_run else {}
        params = dict(params or {})

        target_raw = str(conf.get("target_date_raw") or params.get("target_date_raw", "")).strip()
        target_iso = str(conf.get("target_date_iso") or params.get("target_date_iso", "")).strip()
        run_id = str(conf.get("run_id") or params.get("run_id") or "").strip()
        experimental = _as_bool(conf.get("experimental", params.get("experimental", False)))

        if not run_id:
            raise ValueError(
                "run_id is required. "
                "Pass it from welding_batch_ingest TriggerDagRunOperator conf."
            )
        if not target_raw and target_iso:
            target_raw = target_iso.replace("-", "")
        if not target_iso and target_raw:
            target_iso = f"{target_raw[0:4]}-{target_raw[4:6]}-{target_raw[6:8]}"

        return {
            "run_id": run_id,
            "target_date_raw": target_raw,
            "target_date_iso": target_iso,
            "experimental": experimental,
            "source_dag_run_id": conf.get("source_dag_run_id"),
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
                            WHERE total_segments != 32 OR channel_count != 2
                        ) AS invalid_products
                    FROM per_product
                    """,
                    (run_id,),
                )
                product_count, invalid_products = cur.fetchone()
        if (product_count or 0) == 0:
            raise RuntimeError(f"run_id={run_id} has no per-product segment rows")
        if (invalid_products or 0) > 0:
            raise RuntimeError(
                f"run_id={run_id} inference coverage failed: invalid_products={invalid_products}"
            )
        return {**run_meta, "inference_products": int(product_count)}

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def validate_summary_completeness(run_meta: dict) -> dict:
        run_id = run_meta["run_id"]
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
                            WHERE summary_rows != 2 OR channel_count != 2
                        ) AS invalid_products
                    FROM per_product
                    """,
                    (run_id,),
                )
                product_count, invalid_products = cur.fetchone()
        if (product_count or 0) == 0:
            raise RuntimeError(f"run_id={run_id} has no pattern_summary rows")
        if (invalid_products or 0) > 0:
            raise RuntimeError(
                f"run_id={run_id} summary completeness failed: invalid_products={invalid_products}"
            )

        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
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

        return {
            **run_meta,
            "summary_products": int(product_count),
            "segment_rows": int(segment_rows or 0),
            "summary_rows": int(summary_rows or 0),
            "segment_products": int(segment_products or 0),
            "summary_products_distinct": int(summary_products or 0),
        }

    @task()
    def finalize_run(validation_result: dict) -> dict:
        details = {
            "target_date": validation_result.get("target_date_iso"),
            "status": "SPARK_PROCESS_COMPLETED",
            "spark_run_id": validation_result.get("run_id"),
            "segment_rows": validation_result.get("segment_rows"),
            "summary_rows": validation_result.get("summary_rows"),
            "experimental": bool(validation_result.get("experimental", False)),
        }
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO welding.pipeline_heartbeat (component_name, details)
                    VALUES (%s, %s::jsonb)
                    """,
                    ("airflow.welding_spark_processing", json.dumps(details)),
                )
        return validation_result

    @task(outlets=[SPARK_PROCESS_COMPLETED_ASSET])
    def publish_spark_asset(validation_result: dict) -> dict:
        log.info(
            "Publishing spark asset: run_id=%s target_date=%s experimental=%s",
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
    finalized = finalize_run(summary_ok)
    published = publish_spark_asset(finalized)

    if ENABLE_TRIGGER_FALLBACK:
        trigger_post_quality_control = TriggerDagRunOperator(
            task_id="trigger_post_quality_control",
            trigger_dag_id="welding_post_quality_control",
            conf={
                "run_id": "{{ ti.xcom_pull(task_ids='publish_spark_asset')['run_id'] }}",
                "target_date_iso": "{{ ti.xcom_pull(task_ids='publish_spark_asset')['target_date_iso'] }}",
                "experimental": "{{ ti.xcom_pull(task_ids='publish_spark_asset')['experimental'] }}",
                "source_dag_run_id": "{{ run_id }}",
            },
            wait_for_completion=False,
            reset_dag_run=False,
        )
        published >> trigger_post_quality_control

    finalized >> published


welding_spark_processing_dag()
