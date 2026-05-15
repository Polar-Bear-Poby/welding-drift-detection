"""
welding_post_quality_control.py - Post Quality Control (DAG 3)
===============================================================

Responsibilities:
- Verify final load success.
- Verify business-rule consistency between segment and summary.
- Write final heartbeat/alert records.
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
from welding_assets import DB_QC_COMPLETED_ASSET

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
        "/storage/intermediate,/storage/spark_batch/intermediate",
    ).split(",")
    if p.strip()
]

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


def _is_under(root: Path, candidate: Path) -> bool:
    try:
        root_resolved = root.resolve(strict=False)
        cand_resolved = candidate.resolve(strict=False)
        return root_resolved == cand_resolved or root_resolved in cand_resolved.parents
    except Exception:
        return False


@dag(
    dag_id="welding_post_quality_control",
    schedule=None,
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["welding", "airflow3", "quality", "gate"],
    default_args={"owner": "welding-team", "retries": 1, "retry_delay": timedelta(minutes=2)},
)
def welding_post_quality_control_dag():
    @task()
    def prepare_qc_context(dag_run=None, params=None) -> dict:
        conf = dict((dag_run.conf or {})) if dag_run else {}
        run_id = str(conf.get("run_id") or params.get("run_id", "")).strip()
        target_date_iso = str(conf.get("target_date_iso") or params.get("target_date_iso", "")).strip()
        experimental = _as_bool(conf.get("experimental", params.get("experimental", False)))
        if not run_id:
            raise ValueError(
                "run_id is required for post quality control. "
                "Pass it from welding_spark_processing TriggerDagRunOperator conf."
            )
        return {
            "run_id": run_id,
            "target_date_iso": target_date_iso,
            "experimental": experimental,
            "source_dag_run_id": conf.get("source_dag_run_id"),
        }

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def verify_load_success(context: dict) -> dict:
        run_id = context["run_id"]
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, COALESCE(detail_json::text, '{}')
                    FROM welding.stage_event
                    WHERE run_id = %s::uuid
                      AND stage_name = 'load_complete'
                    """,
                    (run_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError(f"No load_complete stage_event found for run_id={run_id}")
                status, _detail_json = row
                if status != "SUCCESS":
                    raise RuntimeError(
                        f"run_id={run_id} load_complete status={status} (expected SUCCESS)"
                    )
                cur.execute(
                    "SELECT COUNT(*) FROM welding.pattern_segment WHERE run_id = %s::uuid",
                    (run_id,),
                )
                seg_rows = int((cur.fetchone() or [0])[0] or 0)
                cur.execute(
                    "SELECT COUNT(*) FROM welding.pattern_summary WHERE run_id = %s::uuid",
                    (run_id,),
                )
                sum_rows = int((cur.fetchone() or [0])[0] or 0)
        if seg_rows <= 0 or sum_rows <= 0:
            raise RuntimeError(
                f"run_id={run_id} invalid row counts after load_complete: segment={seg_rows}, summary={sum_rows}"
            )
        return {**context, "segment_rows": seg_rows, "summary_rows": sum_rows}

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
        drift_rows = int(context.get("drift_rows", 0))
        return "write_alert_heartbeat" if drift_rows > 0 else "cleanup_intermediate_artifacts"

    @task()
    def write_alert_heartbeat(context: dict):
        details = {
            "target_date": context.get("target_date_iso"),
            "status": "REPLAY_COMPLETED",
            "qc_status": "drift_detected",
            "spark_run_id": context.get("run_id"),
            "drift_rows": context.get("drift_rows"),
            "summary_rows": context.get("summary_rows"),
            "experimental": bool(context.get("experimental", False)),
        }
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO welding.pipeline_heartbeat (component_name, details)
                    VALUES (%s, %s::jsonb)
                    """,
                    ("airflow.welding_post_quality_control", json.dumps(details)),
                )

    @task(outlets=[DB_QC_COMPLETED_ASSET])
    def write_success_heartbeat(context: dict):
        details = {
            "target_date": context.get("target_date_iso"),
            "status": "REPLAY_COMPLETED",
            "qc_status": "normal",
            "spark_run_id": context.get("run_id"),
            "drift_rows": context.get("drift_rows"),
            "summary_rows": context.get("summary_rows"),
            "experimental": bool(context.get("experimental", False)),
        }
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO welding.pipeline_heartbeat (component_name, details)
                    VALUES (%s, %s::jsonb)
                    """,
                    ("airflow.welding_post_quality_control", json.dumps(details)),
                )

    @task()
    def cleanup_intermediate_artifacts(context: dict) -> dict:
        """Delete only run-scoped intermediate artifacts after QC success.

        Final outputs (DB rows, /storage/spark_batch/segments, /storage/spark_batch/summary)
        are intentionally excluded.
        """
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
                if not candidate.exists():
                    continue
                if not _is_under(root, candidate):
                    continue
                if candidate.is_dir():
                    shutil.rmtree(candidate)
                    deleted.append(str(candidate))
                elif candidate.is_file():
                    candidate.unlink(missing_ok=True)
                    deleted.append(str(candidate))

        log.info("QC cleanup done. run_id=%s deleted=%s", run_id, deleted)
        return {**context, "cleanup_deleted": deleted}

    context = prepare_qc_context()
    load_ok = verify_load_success(context)
    rule_ok = verify_business_rule_applied(load_ok)
    branch = route_alert_or_success(rule_ok)
    alert = write_alert_heartbeat(rule_ok)
    cleaned = cleanup_intermediate_artifacts(rule_ok)
    success = write_success_heartbeat(cleaned)
    branch >> [alert, cleaned]
    cleaned >> success


welding_post_quality_control_dag()
