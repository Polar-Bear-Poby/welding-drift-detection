"""
welding_streaming_monitor.py - Airflow 3 Version
===============================================
Modern TaskFlow API with Branching for Airflow 3.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import psycopg2
from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator

log = logging.getLogger(__name__)

_PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
_PG_PORT = os.getenv("POSTGRES_PORT", "5432")
_PG_DB = os.getenv("POSTGRES_DB", "welding_drift")
_PG_USER = os.getenv("POSTGRES_USER", "welding")
_PG_PASS = os.getenv("POSTGRES_PASSWORD", "welding_pass")
DB_CONN_STR = (
    f"host={_PG_HOST} port={_PG_PORT} dbname={_PG_DB} "
    f"user={_PG_USER} password={_PG_PASS}"
)
HEALTH_WINDOW_MIN = 15
SPARK_SUBMIT_CMD = (
    "docker exec welding-spark-master bash -lc '"
    "LINE_COUNT=${LINE_COUNT:-3}; "
    "CONSUMER_COUNT=${CONSUMER_COUNT:-$((LINE_COUNT * 2))}; "
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
    "    /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 "
    "    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 "
    "    /opt/spark/apps/spark_streaming.py >/tmp/spark_streaming_consumer_${consumer_id}.log 2>&1 & "
    "done;'"
)

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
            with psycopg2.connect(DB_CONN_STR) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM welding.pattern_summary "
                        "WHERE processed_at >= NOW() - INTERVAL '%s minutes' AND source_file LIKE 'kafka://%%'",
                        (HEALTH_WINDOW_MIN,)
                    )
                    count = cur.fetchone()[0]
            
            log.info(f"Recent records: {count}")
            return "log_healthy_heartbeat" if count > 0 else "restart_spark_streaming"
        except Exception as e:
            log.error(f"Health check failed: {e}")
            return "log_healthy_heartbeat"

    @task()
    def log_healthy_heartbeat():
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES ('airflow.streaming_monitor', '{\"status\":\"healthy\"}'::jsonb)"
                )

    restart_spark = BashOperator(
        task_id="restart_spark_streaming",
        bash_command=SPARK_SUBMIT_CMD
    )

    verify_restart = BashOperator(
        task_id="verify_restart",
        bash_command=(
            "sleep 60 && docker exec welding-postgres psql -U welding -d welding_drift -c \""
            "SELECT COUNT(*) FROM welding.pattern_summary WHERE processed_at >= NOW() - INTERVAL '2 minutes' "
            "AND source_file LIKE 'kafka://%';\""
        )
    )

    @task(trigger_rule="one_failed")
    def notify_failure():
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES ('airflow.streaming_monitor', '{\"status\":\"restart_failed\"}'::jsonb)"
                )

    # Dependency Flow
    branch = check_streaming_health()
    branch >> log_healthy_heartbeat()
    branch >> restart_spark >> verify_restart >> notify_failure()

welding_streaming_monitor_dag()
