"""
welding_streaming_monitor.py - Airflow 3 Version
===============================================
Modern TaskFlow API with Branching for Airflow 3.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import datetime, timedelta

import psycopg2
from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator

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
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "").strip()
HEALTH_WINDOW_MIN = 15
TOPIC_RAW_LASER_A = os.getenv("TOPIC_RAW_LASER_A", "welding.raw.laser_a.v1")
TOPIC_RAW_LASER_B = os.getenv("TOPIC_RAW_LASER_B", "welding.raw.laser_b.v1")
SOURCE_LASER_A = f"kafka://{TOPIC_RAW_LASER_A}"
SOURCE_LASER_B = f"kafka://{TOPIC_RAW_LASER_B}"
SPARK_SUBMIT_CMD = (
    "docker exec welding-spark-master bash -lc '"
    "LINE_COUNT=${LINE_COUNT:-3}; "
    "CONSUMER_COUNT=${CONSUMER_COUNT:-$((LINE_COUNT * 2))}; "
    "SPARK_STREAMING_CORES_MAX=${SPARK_STREAMING_CORES_MAX:-1}; "
    "SPARK_STREAMING_EXECUTOR_CORES=${SPARK_STREAMING_EXECUTOR_CORES:-1}; "
    "SPARK_STREAMING_EXECUTOR_MEMORY=${SPARK_STREAMING_EXECUTOR_MEMORY:-1g}; "
    "if [ $((CONSUMER_COUNT % 2)) -ne 0 ] || [ \"${CONSUMER_COUNT}\" -lt 2 ]; then "
    "  echo \"CONSUMER_COUNT must be even and >=2\"; exit 1; "
    "fi; "
    "pids=$(pgrep -f \"spark_streaming.py\" 2>/dev/null | grep -vw \"$$\" || true); "
    "if [ -n \"$pids\" ]; then kill -TERM $pids || true; sleep 5; fi; "
    "pids2=$(pgrep -f \"spark_streaming.py\" 2>/dev/null | grep -vw \"$$\" || true); "
    "if [ -n \"$pids2\" ]; then kill -KILL $pids2 || true; fi; "
    "mkdir -p /tmp/.ivy2; "
    "for consumer_id in $(seq 1 ${CONSUMER_COUNT}); do "
    "  if [ $((consumer_id % 2)) -eq 1 ]; then "
    "    topic=\"welding.raw.laser_a.v1\"; channel=\"1\"; group_id=\"welding-stream-laser-a\"; "
    "  else "
    "    topic=\"welding.raw.laser_b.v1\"; channel=\"0\"; group_id=\"welding-stream-laser-b\"; "
    "  fi; "
    "  rm -rf /tmp/spark-checkpoints-consumer-${consumer_id}; "
    "  nohup env TOPIC_RAW=\"${topic}\" CHANNEL_FILTER=\"${channel}\" KAFKA_GROUP_ID=\"${group_id}\" SPARK_CHECKPOINT_DIR=\"/tmp/spark-checkpoints-consumer-${consumer_id}\" "
    "    /opt/spark/bin/spark-submit --master spark://spark-master:7077 "
    "    --conf spark.cores.max=${SPARK_STREAMING_CORES_MAX} "
    "    --conf spark.executor.cores=${SPARK_STREAMING_EXECUTOR_CORES} "
    "    --conf spark.executor.memory=${SPARK_STREAMING_EXECUTOR_MEMORY} "
    "    --conf spark.jars.ivy=/tmp/.ivy2 "
    "    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 "
    "    /opt/spark/apps/spark_streaming.py >/tmp/spark_streaming_consumer_${consumer_id}.log 2>&1 & "
    "done;'"
)


def _recent_channel_counts(window_minutes: int) -> tuple[int, int]:
    with psycopg2.connect(DB_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT source_file, COUNT(*)
                FROM welding.pattern_summary
                WHERE processed_at >= NOW() - (%s || ' minutes')::interval
                  AND source_file IN (%s, %s)
                GROUP BY source_file
                """,
                (window_minutes, SOURCE_LASER_A, SOURCE_LASER_B),
            )
            rows = cur.fetchall()

    counts = {SOURCE_LASER_A: 0, SOURCE_LASER_B: 0}
    for source_file, count in rows:
        counts[source_file] = int(count)
    return counts[SOURCE_LASER_A], counts[SOURCE_LASER_B]


def _send_external_alert(title: str, details: dict) -> None:
    """Send optional external alert to webhook endpoint."""
    if not ALERT_WEBHOOK_URL:
        return
    payload = {
        "text": title,
        "details": details,
    }
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        ALERT_WEBHOOK_URL,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5):
            pass
    except Exception as exc:
        log.warning("External alert webhook failed: %s", exc)

@dag(
    dag_id="welding_streaming_monitor",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["welding", "airflow3", "streaming"],
    default_args={"owner": "welding-team", "retries": 1}
)
def welding_streaming_monitor_dag():

    @task.branch()
    def check_streaming_health():
        try:
            a_count, b_count = _recent_channel_counts(HEALTH_WINDOW_MIN)
            log.info(
                "Recent channel records (%s min): laser_a=%s laser_b=%s",
                HEALTH_WINDOW_MIN,
                a_count,
                b_count,
            )
            return "log_healthy_heartbeat" if (a_count > 0 and b_count > 0) else "restart_spark_streaming"
        except Exception as e:
            log.error(f"Health check failed: {e}")
            return "restart_spark_streaming"

    @task()
    def log_healthy_heartbeat():
        a_count, b_count = _recent_channel_counts(HEALTH_WINDOW_MIN)
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    (
                        "airflow.streaming_monitor",
                        (
                            "{\"status\":\"healthy\","
                            f"\"window_min\":{HEALTH_WINDOW_MIN},"
                            f"\"laser_a_count\":{a_count},"
                            f"\"laser_b_count\":{b_count}"
                            "}"
                        ),
                    ),
                )

    restart_spark = BashOperator(
        task_id="restart_spark_streaming",
        bash_command=SPARK_SUBMIT_CMD
    )

    @task()
    def verify_restart():
        import time

        time.sleep(60)
        a_count, b_count = _recent_channel_counts(2)
        log.info("Post-restart channel records (2 min): laser_a=%s laser_b=%s", a_count, b_count)
        if not (a_count > 0 and b_count > 0):
            raise RuntimeError(
                f"restart verify failed: laser_a={a_count}, laser_b={b_count}"
            )

    @task(trigger_rule="one_failed")
    def notify_failure():
        details = {"status": "restart_failed"}
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    ("airflow.streaming_monitor", "{\"status\":\"restart_failed\"}"),
                )
        _send_external_alert("welding_streaming_monitor restart failed", details)

    # Dependency Flow
    branch = check_streaming_health()
    branch >> log_healthy_heartbeat()
    verify = verify_restart()
    branch >> restart_spark >> verify >> notify_failure()

welding_streaming_monitor_dag()


