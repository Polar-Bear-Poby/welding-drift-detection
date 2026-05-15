"""
welding_db_asset_dag.py
=======================
DB/QC domain DAG (Asset-based).

Triggered by CONSUMER_PROCESSED_ASSET:
- Verify final load success.
- Verify business-rule consistency.
- Write final heartbeat.
- Cleanup run-scoped intermediates after QC success.
- Publish DB_QC_COMPLETED_ASSET.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from welding_assets import CONSUMER_PROCESSED_ASSET, DB_QC_COMPLETED_ASSET

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

SPARK_INTERMEDIATE_ROOTS = [
    p.strip()
    for p in os.getenv(
        "SPARK_INTERMEDIATE_ROOTS",
        "/spark-out/intermediate,/spark-out/spark_batch/intermediate",
    ).split(",")
    if p.strip()
]
DB_QC_MIN_PARTIAL_RATIO = float(os.getenv("DB_QC_MIN_PARTIAL_RATIO", "0.60"))


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


def _to_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _is_under(root: Path, candidate: Path) -> bool:
    try:
        root_resolved = root.resolve(strict=False)
        cand_resolved = candidate.resolve(strict=False)
        return root_resolved == cand_resolved or root_resolved in cand_resolved.parents
    except Exception:
        return False


@dag(
    dag_id="welding_db_asset_dag",
    schedule=[CONSUMER_PROCESSED_ASSET],
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["welding", "asset", "db", "qc"],
    default_args={"owner": "welding-team", "retries": 1, "retry_delay": timedelta(minutes=2)},
)
def welding_db_asset_dag():
    @task()
    def prepare_qc_context(dag_run=None, params=None) -> dict:
        conf = dict((dag_run.conf or {})) if dag_run else {}
        params = dict(params or {})
        run_id = str(conf.get("run_id") or params.get("run_id", "")).strip()
        target_date_iso = str(conf.get("target_date_iso") or params.get("target_date_iso", "")).strip()
        experimental = _as_bool(conf.get("experimental", params.get("experimental", False)))
        expected_channels = _to_int(conf.get("expected_channels", params.get("expected_channels", 1)), 1)
        channel_scope = str(conf.get("channel_scope") or params.get("channel_scope") or "single").strip() or "single"
        target_products_total = _to_int(conf.get("target_products_total", params.get("target_products_total", 0)), 0)

        if not run_id:
            with psycopg2.connect(DB_CONN_STR) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT details
                        FROM welding.pipeline_heartbeat
                        WHERE component_name = 'airflow.welding_consumer_asset_dag'
                          AND details->>'status' = 'CONSUMER_PROCESSED'
                        ORDER BY heartbeat_at DESC
                        LIMIT 1
                        """
                    )
                    row = cur.fetchone()
            if row and isinstance(row[0], dict):
                details = row[0]
                run_id = str(details.get("spark_run_id") or "").strip()
                target_date_iso = str(details.get("target_date") or target_date_iso).strip()
                experimental = _as_bool(details.get("experimental", experimental))
                expected_channels = _to_int(details.get("expected_channels", expected_channels), expected_channels)
                channel_scope = str(details.get("channel_scope") or channel_scope).strip() or channel_scope
                target_products_total = _to_int(
                    details.get("target_products_total", target_products_total),
                    target_products_total,
                )
        if not run_id:
            raise ValueError("run_id is required for DB QC")
        if expected_channels < 1:
            expected_channels = 1
        return {
            "run_id": run_id,
            "target_date_iso": target_date_iso,
            "experimental": experimental,
            "expected_channels": expected_channels,
            "channel_scope": channel_scope,
            "target_products_total": target_products_total,
        }

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def verify_load_success(context: dict) -> dict:
        run_id = context["run_id"]
        experimental = bool(context.get("experimental", False))
        expected_channels_from_ctx = max(_to_int(context.get("expected_channels", 1), 1), 1)
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, detail_json, error_code, error_message
                    FROM welding.stage_event
                    WHERE run_id = %s::uuid
                      AND stage_name = 'load_complete'
                    """,
                    (run_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError(f"No load_complete stage_event found for run_id={run_id}")
                status = str(row[0] or "")
                detail_json = row[1] if isinstance(row[1], dict) else {}
                load_error_code = str(row[2] or "")
                load_error_message = str(row[3] or "")
                load_decision = str(detail_json.get("load_decision") or "")
                stage_progress_ratio = _to_float(detail_json.get("progress_ratio"), 0.0)
                stage_summary_channels = _to_int(detail_json.get("summary_channels"), 0)
                stage_expected_channels = max(_to_int(detail_json.get("expected_channels"), 0), 0)
                stage_expected_products = _to_int(detail_json.get("expected_products"), 0)
                stage_summary_products = _to_int(detail_json.get("summary_products"), 0)
                stage_summary_rows = _to_int(detail_json.get("summary_rows"), 0)

                if status == "FAILED":
                    raise RuntimeError(
                        f"run_id={run_id} load_complete FAILED "
                        f"code={load_error_code or 'unknown'} "
                        f"decision={load_decision or 'n/a'} "
                        f"message={load_error_message or detail_json}"
                    )
                if status != "SUCCESS":
                    raise RuntimeError(f"run_id={run_id} load_complete status={status} (expected SUCCESS)")

                cur.execute(
                    """
                    SELECT
                        COUNT(*),
                        COUNT(DISTINCT product_id),
                        COUNT(DISTINCT channel)
                    FROM welding.pattern_segment
                    WHERE run_id = %s::uuid
                    """,
                    (run_id,),
                )
                seg_rows, seg_products, seg_channels = cur.fetchone()

                cur.execute(
                    """
                    SELECT
                        COUNT(*),
                        COUNT(DISTINCT product_id),
                        COUNT(DISTINCT channel)
                    FROM welding.pattern_summary
                    WHERE run_id = %s::uuid
                    """,
                    (run_id,),
                )
                sum_rows, sum_products, sum_channels = cur.fetchone()

                cur.execute(
                    """
                    SELECT
                        COALESCE((details->>'target_products_total')::int, 0),
                        GREATEST(COALESCE((details->>'expected_channels')::int, 1), 1)
                    FROM welding.pipeline_heartbeat
                    WHERE component_name='airflow.welding_producer_asset_dag'
                      AND details->>'status'='PRODUCER_DONE'
                      AND details->>'run_id'=%s
                    ORDER BY heartbeat_at DESC
                    LIMIT 1
                    """,
                    (run_id,),
                )
                hb_row = cur.fetchone()
                expected_products = int(hb_row[0] or 0) if hb_row else 0
                expected_channels = int(hb_row[1] or expected_channels_from_ctx) if hb_row else expected_channels_from_ctx
                expected_channels = max(expected_channels, 1)

                if (seg_rows or 0) <= 0 or (sum_rows or 0) <= 0:
                    raise RuntimeError(
                        f"run_id={run_id} invalid row counts: segment={seg_rows}, summary={sum_rows}"
                    )
                if int(sum_channels or 0) < expected_channels:
                    raise RuntimeError(
                        f"run_id={run_id} invalid summary channel coverage: "
                        f"summary_channels={sum_channels}, expected_channels={expected_channels}"
                    )

                # Policy alignment:
                # - exact_match: strict full completion
                # - partial_success_after_grace + experimental: conditional pass
                if load_decision == "exact_match":
                    if expected_products > 0 and int(sum_products or 0) < expected_products:
                        raise RuntimeError(
                            f"run_id={run_id} not fully processed yet: "
                            f"summary_products={sum_products}, expected={expected_products}"
                        )
                elif load_decision == "partial_success_after_grace":
                    if not experimental:
                        raise RuntimeError(
                            f"run_id={run_id} partial success is not allowed in non-experimental mode"
                        )
                    effective_expected_products = expected_products if expected_products > 0 else stage_expected_products
                    effective_progress_ratio = (
                        stage_progress_ratio
                        if stage_progress_ratio > 0
                        else (
                            float(sum_products) / float(effective_expected_products)
                            if effective_expected_products > 0
                            else 0.0
                        )
                    )
                    if effective_progress_ratio < DB_QC_MIN_PARTIAL_RATIO:
                        raise RuntimeError(
                            f"run_id={run_id} partial success ratio too low: "
                            f"ratio={effective_progress_ratio:.4f}, min={DB_QC_MIN_PARTIAL_RATIO:.4f}, "
                            f"summary_products={sum_products}, expected_products={effective_expected_products}"
                        )
                else:
                    # Unknown decision is treated as strict policy for safety.
                    if expected_products > 0 and int(sum_products or 0) < expected_products:
                        raise RuntimeError(
                            f"run_id={run_id} load_decision={load_decision or 'unknown'} "
                            f"requires strict completeness: summary_products={sum_products}, expected={expected_products}"
                        )
        return {
            **context,
            "segment_rows": int(seg_rows or 0),
            "summary_rows": int(sum_rows or 0),
            "segment_products": int(seg_products or 0),
            "summary_products": int(sum_products or 0),
            "expected_products": int(expected_products),
            "segment_channels": int(seg_channels or 0),
            "summary_channels": int(sum_channels or 0),
            "expected_channels": int(expected_channels or expected_channels_from_ctx),
            "load_decision": load_decision,
            "load_error_code": load_error_code,
            "load_error_message": load_error_message,
            "stage_progress_ratio": stage_progress_ratio,
            "stage_summary_products": stage_summary_products,
            "stage_summary_rows": stage_summary_rows,
            "stage_summary_channels": stage_summary_channels,
            "stage_expected_channels": stage_expected_channels,
        }

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def verify_business_rule_applied(context: dict) -> dict:
        run_id = context["run_id"]
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH seg AS (
                        SELECT
                            run_id, source_file, channel,
                            BOOL_OR(segment_drift_flag) AS expected_is_drift
                        FROM welding.pattern_segment
                        WHERE run_id = %s::uuid
                        GROUP BY run_id, source_file, channel
                    )
                    SELECT
                        COUNT(*) FILTER (WHERE s.quality_decision NOT IN ('normal', 'drift')) AS invalid_rows,
                        COUNT(*) FILTER (
                            WHERE (
                                seg.expected_is_drift AND s.quality_decision <> 'drift'
                            ) OR (
                                NOT seg.expected_is_drift AND s.quality_decision <> 'normal'
                            )
                        ) AS inconsistent_rows,
                        COUNT(*) FILTER (WHERE s.quality_decision = 'drift') AS drift_rows,
                        COUNT(*) AS total_rows
                    FROM welding.pattern_summary s
                    JOIN seg
                      ON seg.run_id = s.run_id
                     AND seg.source_file = s.source_file
                     AND seg.channel = s.channel
                    WHERE s.run_id = %s::uuid
                    """,
                    (run_id, run_id),
                )
                invalid_rows, inconsistent_rows, drift_rows, total_rows = cur.fetchone()
        if (invalid_rows or 0) > 0:
            raise RuntimeError(
                f"run_id={run_id} business rule invalid quality_decision rows={invalid_rows}"
            )
        if (inconsistent_rows or 0) > 0:
            raise RuntimeError(
                f"run_id={run_id} business rule inconsistent_rows={inconsistent_rows}"
            )
        return {
            **context,
            "drift_rows": int(drift_rows or 0),
            "summary_rows_checked": int(total_rows or 0),
        }

    @task.branch()
    def route_alert_or_success(context: dict) -> str:
        return "write_alert_heartbeat" if int(context.get("drift_rows", 0)) > 0 else "cleanup_intermediate_artifacts"

    @task()
    def write_alert_heartbeat(context: dict) -> dict:
        details = {
            "target_date": context.get("target_date_iso"),
            "status": "QC_COMPLETED",
            "qc_status": "drift_detected",
            "spark_run_id": context.get("run_id"),
            "drift_rows": context.get("drift_rows"),
            "summary_rows": context.get("summary_rows"),
            "experimental": bool(context.get("experimental", False)),
            "load_decision": context.get("load_decision"),
            "load_error_code": context.get("load_error_code"),
            "load_error_message": context.get("load_error_message"),
        }
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO welding.pipeline_heartbeat (component_name, details)
                    VALUES (%s, %s::jsonb)
                    """,
                    ("airflow.welding_db_asset_dag", json.dumps(details)),
                )
        return {**context, "qc_status": "drift_detected"}

    @task()
    def cleanup_intermediate_artifacts(context: dict) -> dict:
        run_id = str(context.get("run_id", "")).strip()
        target_date_iso = str(context.get("target_date_iso", "")).strip()
        target_date_raw = target_date_iso.replace("-", "") if target_date_iso else ""
        deleted: list[str] = []

        for root_str in SPARK_INTERMEDIATE_ROOTS:
            root = Path(root_str).resolve(strict=False)
            if not root.exists() or not root.is_dir():
                continue
            candidates: list[Path] = [root / run_id]
            if target_date_raw:
                candidates.append(root / target_date_raw / run_id)
            for candidate in candidates:
                if not candidate.exists() or not _is_under(root, candidate):
                    continue
                if candidate.is_dir():
                    shutil.rmtree(candidate)
                    deleted.append(str(candidate))
                elif candidate.is_file():
                    candidate.unlink(missing_ok=True)
                    deleted.append(str(candidate))

        log.info("QC cleanup done. run_id=%s deleted=%s", run_id, deleted)
        return {**context, "cleanup_deleted": deleted, "qc_status": "normal"}

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def merge_qc_context(alert_ctx: dict | None, cleaned_ctx: dict | None) -> dict:
        if alert_ctx:
            return alert_ctx
        if cleaned_ctx:
            return cleaned_ctx
        raise RuntimeError("No QC context to finalize")

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def write_success_heartbeat(context: dict) -> dict:
        details = {
            "target_date": context.get("target_date_iso"),
            "status": "QC_COMPLETED",
            "qc_status": context.get("qc_status", "normal"),
            "spark_run_id": context.get("run_id"),
            "drift_rows": context.get("drift_rows"),
            "summary_rows": context.get("summary_rows"),
            "experimental": bool(context.get("experimental", False)),
            "load_decision": context.get("load_decision"),
            "load_error_code": context.get("load_error_code"),
            "load_error_message": context.get("load_error_message"),
        }
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO welding.pipeline_heartbeat (component_name, details)
                    VALUES (%s, %s::jsonb)
                    """,
                    ("airflow.welding_db_asset_dag", json.dumps(details)),
                )
        return context

    @task(outlets=[DB_QC_COMPLETED_ASSET], trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def publish_db_asset(context: dict) -> dict:
        log.info("Publishing db asset: run_id=%s qc_status=%s", context.get("run_id"), context.get("qc_status"))
        return context

    context = prepare_qc_context()
    load_ok = verify_load_success(context)
    rule_ok = verify_business_rule_applied(load_ok)
    branch = route_alert_or_success(rule_ok)
    alert = write_alert_heartbeat(rule_ok)
    cleaned = cleanup_intermediate_artifacts(rule_ok)
    merged = merge_qc_context(alert, cleaned)
    success = write_success_heartbeat(merged)
    published = publish_db_asset(success)

    branch >> [alert, cleaned]
    cleaned >> success >> published


welding_db_asset_dag()
