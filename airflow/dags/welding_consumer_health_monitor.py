"""
welding_consumer_health_monitor.py - Airflow 3 Version (수정)
=============================================================
5분 주기로 Kafka consumer-group 기반으로 채널별 컨슈머 수를 검증한다.

수정 내역 (2026-04-28):
  - [fix #2] RESTART_CMD: /opt/airflow/scripts 의존 제거.
            Airflow 컨테이너에서 docker exec로 spark 소비자만 직접 재기동.
  - [fix #5] 총 프로세스 수 wc -l → kafka-consumer-groups --describe 기반
            laser_a 그룹 3개 / laser_b 그룹 3개를 개별 검증

채널 규칙:
  - 홀수 consumer_id (1,3,5) → welding-stream-laser-a 그룹
  - 짝수 consumer_id (2,4,6) → welding-stream-laser-b 그룹
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
from datetime import datetime, timedelta

import psycopg2
from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator

log = logging.getLogger(__name__)

# ── DB 연결: 환경변수 우선, 없으면 로컬 기본값 ─────────────────────────
_PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
_PG_PORT = os.getenv("POSTGRES_PORT", "5432")
_PG_DB   = os.getenv("POSTGRES_DB",   "welding_drift")
_PG_USER = os.getenv("POSTGRES_USER", "welding")
_PG_PASS = os.getenv("POSTGRES_PASSWORD", "welding_pass")
DB_CONN_STR = f"host={_PG_HOST} port={_PG_PORT} dbname={_PG_DB} user={_PG_USER} password={_PG_PASS}"

KAFKA_CONTAINER  = "welding-kafka"
KAFKA_BOOTSTRAP  = "kafka:9092"

# 채널별 기대 컨슈머 그룹 멤버 수
EXPECTED_LASER_A = 3  # 홀수 consumer_id: 1, 3, 5
EXPECTED_LASER_B = 3  # 짝수 consumer_id: 2, 4, 6
CONSUMER_GROUPS  = {
    "welding-stream-laser-a": EXPECTED_LASER_A,
    "welding-stream-laser-b": EXPECTED_LASER_B,
}

# [fix #2] 재기동: Airflow 컨테이너에서 spark 컨테이너 내부 프로세스를 직접 재기동
RESTART_CMD = (
    "docker exec welding-spark-master bash -lc '"
    "CONSUMER_COUNT=6; "
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
    "  nohup env TOPIC_RAW=\"${topic}\" CHANNEL_FILTER=\"${channel}\" "
    "    KAFKA_GROUP_ID=\"${group_id}\" SPARK_CHECKPOINT_DIR=\"/tmp/spark-checkpoints-consumer-${consumer_id}\" "
    "    /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 "
    "    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 "
    "    /opt/spark/apps/spark_streaming.py >/tmp/spark_streaming_consumer_${consumer_id}.log 2>&1 & "
    "done;'"
)


def _get_group_member_count(group_id: str) -> int:
    """
    kafka-consumer-groups --describe로 특정 그룹의 활성 멤버(consumer) 수를 반환한다.
    MEMBERS 컬럼이 없는 구버전 출력 형식은 CLIENT-ID 고유 개수로 대체.
    """
    result = subprocess.run(
        f"docker exec {KAFKA_CONTAINER} "
        f"kafka-consumer-groups --bootstrap-server {KAFKA_BOOTSTRAP} "
        f"--group {group_id} --describe",
        shell=True, capture_output=True, text=True, timeout=30,
    )
    lines = [l for l in result.stdout.strip().split("\n") if l.strip() and not l.startswith("GROUP")]
    # 출력 컬럼: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
    # 활성 컨슈머는 CONSUMER-ID가 "-"가 아닌 행으로 식별
    active_ids: set[str] = set()
    for line in lines:
        parts = line.split()
        if len(parts) >= 7 and parts[6] != "-":
            active_ids.add(parts[6])
    return len(active_ids)


@dag(
    dag_id="welding_consumer_health_monitor",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["welding", "airflow3", "monitoring", "consumer"],
    default_args={
        "owner": "welding-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    doc_md="""
## welding_consumer_health_monitor

**목적**: 5분 주기로 Kafka consumer-group 기반으로 채널별 컨슈머 수를 검증하고,
부족 시 `start_always_on_pipeline.sh`를 통해 자동 재기동한다.

**검증 방식 (v2)**
- `kafka-consumer-groups --describe`로 그룹별 활성 CONSUMER-ID 개수를 산출
- `welding-stream-laser-a` → 3개, `welding-stream-laser-b` → 3개 각각 검증

**재기동 경로 (fix #2)**
- Airflow 컨테이너에서 `docker exec welding-spark-master ...`로 소비자 프로세스만 직접 재기동
    """,
)
def welding_consumer_health_monitor_dag():

    @task.branch()
    def check_consumer_count():
        """
        Kafka consumer-group별로 활성 컨슈머 수를 확인한다.
        laser_a 또는 laser_b 그룹 중 하나라도 기대치 미달이면 재기동 브랜치.
        """
        group_status = {}
        all_healthy = True

        for group_id, expected in CONSUMER_GROUPS.items():
            try:
                count = _get_group_member_count(group_id)
            except Exception as exc:
                log.error("그룹 %s 확인 실패: %s", group_id, exc)
                count = -1

            healthy = count >= expected
            group_status[group_id] = {"count": count, "expected": expected, "healthy": healthy}
            if not healthy:
                all_healthy = False
                log.warning(
                    "컨슈머 부족 — 그룹: %s, 현재: %d, 기대: %d",
                    group_id, count, expected,
                )
            else:
                log.info("컨슈머 정상 — 그룹: %s, 현재: %d", group_id, count)

        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    (
                        "airflow.consumer_health_monitor.check",
                        json.dumps({"groups": group_status, "all_healthy": all_healthy}),
                    ),
                )

        return "log_healthy_heartbeat" if all_healthy else "restart_consumers"

    @task()
    def log_healthy_heartbeat():
        """정상 상태 heartbeat 기록"""
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    (
                        "airflow.consumer_health_monitor",
                        json.dumps({
                            "status": "healthy",
                            "groups": {g: e for g, e in CONSUMER_GROUPS.items()},
                        }),
                    ),
                )
        log.info("컨슈머 헬스 정상 — heartbeat 기록 완료")

    # [fix #2] BashOperator: Airflow 컨테이너에서 spark 소비자 직접 재기동
    restart_consumers = BashOperator(
        task_id="restart_consumers",
        bash_command=RESTART_CMD,
        execution_timeout=timedelta(minutes=3),
    )

    @task()
    def verify_after_restart():
        """재기동 20초 후 그룹별 컨슈머 수 재검증."""
        import time
        time.sleep(20)

        group_status = {}
        all_ok = True
        for group_id, expected in CONSUMER_GROUPS.items():
            try:
                count = _get_group_member_count(group_id)
            except Exception as exc:
                log.error("재검증 실패 — 그룹: %s: %s", group_id, exc)
                count = -1
            ok = count >= expected
            group_status[group_id] = {"count": count, "expected": expected, "ok": ok}
            if not ok:
                all_ok = False

        log.info("재기동 후 그룹별 상태: %s", json.dumps(group_status))
        if not all_ok:
            raise RuntimeError(f"재기동 후에도 컨슈머 수 부족: {group_status}")

    @task(trigger_rule="all_done")
    def notify_restart_result():
        """재기동 후 최종 상태를 DB에 기록. 성공/실패 무관하게 항상 실행."""
        group_status = {}
        all_ok = True
        for group_id, expected in CONSUMER_GROUPS.items():
            try:
                count = _get_group_member_count(group_id)
            except Exception:
                count = -1
            ok = count >= expected
            group_status[group_id] = {"count": count, "expected": expected, "ok": ok}
            if not ok:
                all_ok = False

        status = "restart_ok" if all_ok else "restart_failed"
        log.warning("재기동 최종 상태: %s → %s", status, json.dumps(group_status))

        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    (
                        "airflow.consumer_health_monitor",
                        json.dumps({"status": status, "groups": group_status}),
                    ),
                )

    # ── 의존성 연결 ──────────────────────────────────────────────
    branch = check_consumer_count()
    branch >> log_healthy_heartbeat()
    branch >> restart_consumers >> verify_after_restart() >> notify_restart_result()


welding_consumer_health_monitor_dag()
