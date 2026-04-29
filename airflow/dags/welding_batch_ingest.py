"""
welding_batch_ingest.py - Airflow 3 Version
==========================================
Modern TaskFlow API approach for Airflow 3.
Simulates multi-line welding data replay.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timedelta

import psycopg2
from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator

log = logging.getLogger(__name__)

# Config
_PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
_PG_PORT = os.getenv("POSTGRES_PORT", "5432")
_PG_DB = os.getenv("POSTGRES_DB", "welding_drift")
_PG_USER = os.getenv("POSTGRES_USER", "welding")
_PG_PASS = os.getenv("POSTGRES_PASSWORD", "welding_pass")
DB_CONN_STR = (
    f"host={_PG_HOST} port={_PG_PORT} dbname={_PG_DB} "
    f"user={_PG_USER} password={_PG_PASS}"
)
REPLAY_LINE_COUNT = int(os.getenv("REPLAY_LINE_COUNT", "3"))
REPLAY_LINE_SEEDS = os.getenv("REPLAY_LINE_SEEDS", "42,73,128")
REPLAY_SPEED = float(os.getenv("REPLAY_SPEED", "100"))
SPARK_BATCH_OUTPUT_DIR = os.getenv("SPARK_BATCH_OUTPUT_DIR", "/storage/spark_batch")
PRODUCER_CONTAINER = os.getenv("PRODUCER_CONTAINER", "welding-producer")

@dag(
    dag_id="welding_batch_ingest",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["welding", "airflow3", "modern"],
    default_args={"owner": "welding-team", "retries": 2}
)
def welding_batch_ingest_dag():

    @task()
    def prepare_run_context(ds=None) -> dict:
        """Create deterministic context for this DAG run."""
        return {
            "run_id": str(uuid.uuid4()),
            "event_date": ds,
        }

    @task.short_circuit()
    def check_already_replayed(ds=None):
        """Skip if already replayed for this date."""
        try:
            with psycopg2.connect(DB_CONN_STR) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM welding.pipeline_heartbeat "
                        "WHERE component_name = 'airflow.welding_batch_ingest' "
                        "AND details->>'event_date' = %s AND details->>'status' = 'REPLAY_COMPLETED'",
                        (ds,)
                    )
                    if cur.fetchone()[0] > 0:
                        log.info(f"Date {ds} already replayed. Skipping.")
                        return False
            return True
        except Exception as e:
            log.warning(f"DB check failed: {e}")
            return True

    run_producer = BashOperator(
        task_id="run_producer",
        bash_command=(
            f"docker start {PRODUCER_CONTAINER} >/dev/null 2>&1 || true && "
            f"docker exec {PRODUCER_CONTAINER} python /app/producer.py "
            "--data-dir /data --kafka kafka:9092 "
            f"--line-count {REPLAY_LINE_COUNT} --line-seed \"{REPLAY_LINE_SEEDS}\" "
            f"--speed {REPLAY_SPEED} --no-schedule-wait --oldest-date-only"
        )
    )

    @task()
    def run_spark_batch(run_context: dict):
        """Run batch job and bind the result to this DAG run_id."""
        import subprocess

        run_id = run_context["run_id"]
        cmd = (
            "docker exec welding-spark-master /opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/spark/apps/spark_batch.py "
            "--input-dir /data "
            f"--output-dir {SPARK_BATCH_OUTPUT_DIR} "
            "--write-postgres "
            f"--postgres-host {_PG_HOST} "
            f"--postgres-port {_PG_PORT} "
            f"--postgres-db {_PG_DB} "
            f"--postgres-user {_PG_USER} "
            f"--postgres-password {_PG_PASS} "
            "--oldest-date-only "
            f"--run-id {run_id}"
        )
        result = subprocess.run(cmd, shell=True, timeout=1800)
        if result.returncode != 0:
            raise RuntimeError(f"spark_batch.py failed: returncode={result.returncode}")

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def validate_results(run_context: dict, ds=None):
        """Verify this DAG run's Spark batch result and quality."""
        run_id = run_context["run_id"]
        try:
            with psycopg2.connect(DB_CONN_STR) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT status, total_segment_rows, total_summary_rows
                        FROM welding.spark_batch_run
                        WHERE run_id = %s
                        """
                        ,
                        (run_id,),
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise RuntimeError(f"No spark_batch_run found for run_id={run_id}.")

                    status, seg_rows, sum_rows = row
                    if status != "SUCCESS":
                        raise RuntimeError(f"Spark batch run {run_id} status={status} (not SUCCESS).")
                    if sum_rows == 0:
                        raise RuntimeError(f"Spark batch run {run_id} produced 0 summary rows.")

                    cur.execute(
                        """
                        SELECT COUNT(*), COUNT(*) FILTER (WHERE quality_decision = 'ERROR')
                        FROM welding.pattern_summary
                        WHERE run_id = %s
                        """,
                        (run_id,),
                    )
                    total, errors = cur.fetchone()

            log.info(
                "Validation - run_id=%s segment_rows=%s summary_rows=%s total=%s errors=%s",
                run_id,
                seg_rows,
                sum_rows,
                total,
                errors,
            )
            if total == 0:
                raise RuntimeError(f"No pattern_summary rows found for run_id={run_id}.")

            if (errors / total) >= 0.5:
                raise ValueError(
                    f"Data quality failure for run_id={run_id}: Error rate {(errors/total):.1%} >= 50%"
                )

            return {
                "run_id": str(run_id),
                "segment_rows": int(seg_rows),
                "summary_rows": int(sum_rows),
                "error_rows": int(errors),
            }
        except psycopg2.Error as e:
            raise RuntimeError(f"DB error: {e}")

    @task()
    def report_heartbeat(validation_result: dict, ds=None):
        """Log completion."""
        details = {
            "event_date": ds,
            "status": "REPLAY_COMPLETED",
            "spark_run_id": validation_result.get("run_id"),
            "segment_rows": validation_result.get("segment_rows"),
            "summary_rows": validation_result.get("summary_rows"),
            "error_rows": validation_result.get("error_rows"),
        }
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) VALUES (%s, %s::jsonb)",
                    ("airflow.welding_batch_ingest", json.dumps(details)),
                )

    # Dependency Flow
    gate = check_already_replayed()
    run_context = prepare_run_context()
    run_spark = run_spark_batch(run_context)
    validation = validate_results(run_context)
    heartbeat = report_heartbeat(validation)
    gate >> run_context >> run_producer >> run_spark >> validation >> heartbeat

# Instantiate
welding_batch_ingest_dag()
