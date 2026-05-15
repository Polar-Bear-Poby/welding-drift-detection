"""
welding_batch_ingest.py - Ingest Orchestrator (DAG 1)
======================================================

Responsibilities:
- Discover oldest date folder with arrived CSV files.
- Execute producer per line (mapped task).
- Verify Kafka offsets increased (delivery confirmation).
- Emit ingest heartbeat.
- Trigger spark processing DAG.
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
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from welding_assets import INGEST_COMPLETED_ASSET

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

REPLAY_LINE_COUNT = int(os.getenv("REPLAY_LINE_COUNT", "3"))
REPLAY_LINE_SEEDS = os.getenv("REPLAY_LINE_SEEDS", "42,73,128")
REPLAY_SPEED = float(os.getenv("REPLAY_SPEED", "100"))
PRODUCER_TIMEOUT_SEC = int(os.getenv("REPLAY_PRODUCER_TIMEOUT_SEC", "7200"))
REPLAY_TARGET_PRODUCTS_TOTAL = int(os.getenv("REPLAY_TARGET_PRODUCTS_TOTAL", "0"))

PRODUCER_CONTAINER = os.getenv("PRODUCER_CONTAINER", "welding-producer")
PRODUCER_IMAGE = os.getenv("PRODUCER_IMAGE", "welding-kafka-submission-producer:latest")
NETWORK_NAME = os.getenv("NETWORK_NAME", "welding-kafka-submission_welding-net")
KAFKA_CONTAINER = os.getenv("KAFKA_CONTAINER", "welding-kafka")
DATA_DIR_CONTAINER = os.getenv("DATA_DIR_CONTAINER", "/data")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

TOPIC_RAW = os.getenv("TOPIC_RAW", "welding.raw.v1")
TOPIC_RAW_LASER_A = os.getenv("TOPIC_RAW_LASER_A", "").strip()
TOPIC_RAW_LASER_B = os.getenv("TOPIC_RAW_LASER_B", "").strip()


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
        # Topic not created yet can happen in first run. Treat as 0.
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


ENABLE_TRIGGER_FALLBACK = _as_bool(os.getenv("ENABLE_TRIGGER_FALLBACK", "0"), default=False)


@dag(
    dag_id="welding_batch_ingest",
    schedule=None,
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["welding", "airflow3", "modern", "data-arrival", "ingest"],
    default_args={"owner": "welding-team", "retries": 2, "retry_delay": timedelta(minutes=2)},
    params={
        "experimental": Param(
            default=False,
            type="boolean",
            description="True면 이미 처리된 날짜도 재실행 허용",
        ),
        "line_count": Param(
            default=REPLAY_LINE_COUNT,
            type="integer",
            description="생산라인(=프로듀서) 개수",
        ),
        "producer_count": Param(
            default=REPLAY_LINE_COUNT,
            type="integer",
            description="라인과 동일해야 함 (producer_count == line_count)",
        ),
        "replay_speed": Param(
            default=REPLAY_SPEED,
            type="number",
            description="producer replay speed",
        ),
        "line_seeds": Param(
            default=REPLAY_LINE_SEEDS,
            type="string",
            description="라인 시드 문자열 (예: 42,73,128)",
        ),
        "target_date": Param(
            default="",
            type="string",
            description="수동 실행 시 YYYYMMDD 지정 (비우면 자동 탐색)",
        ),
        "target_products_total": Param(
            default=0,
            type="integer",
            description="0이면 전체, 양수면 라인별 분할 전송",
        ),
    },
)
def welding_batch_ingest_dag():
    @task()
    def discover_target_date(dag_run=None, params=None) -> dict:
        conf = dict((dag_run.conf or {})) if dag_run else {}
        params = dict(params or {})

        experimental = _as_bool(
            conf.get("experimental", params.get("experimental", os.getenv("EXPERIMENT_MODE", "0"))),
            default=False,
        )
        target_products_total = int(
            conf.get(
                "target_products_total",
                params.get("target_products_total", REPLAY_TARGET_PRODUCTS_TOTAL),
            )
            or 0
        )
        line_count = int(conf.get("line_count", params.get("line_count", REPLAY_LINE_COUNT)) or 0)
        producer_count = int(
            conf.get("producer_count", params.get("producer_count", line_count)) or 0
        )
        replay_speed = float(conf.get("replay_speed", params.get("replay_speed", REPLAY_SPEED)) or 0)
        line_seeds = str(conf.get("line_seeds", params.get("line_seeds", REPLAY_LINE_SEEDS))).strip()

        if line_count < 1:
            raise ValueError(f"line_count must be >= 1 (got {line_count})")
        if producer_count < 1:
            raise ValueError(f"producer_count must be >= 1 (got {producer_count})")
        if producer_count != line_count:
            raise ValueError(
                f"producer_count must equal line_count in orchestrator mode "
                f"(got producer_count={producer_count}, line_count={line_count})"
            )
        if replay_speed <= 0:
            raise ValueError(f"replay_speed must be > 0 (got {replay_speed})")

        requested_date_raw = str(
            conf.get("target_date", params.get("target_date", ""))
        ).strip()

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
            f"docker start {PRODUCER_CONTAINER} >/dev/null 2>&1 || true && "
            f"docker exec {PRODUCER_CONTAINER} python -c \"{discover_code}\""
        )
        proc = subprocess.run(discover_cmd, shell=True, capture_output=True, text=True, timeout=120)
        if proc.returncode != 0:
            raise RuntimeError(
                f"Failed to discover data date in {PRODUCER_CONTAINER}: {proc.stderr.strip()}"
            )

        discovered_target_raw = (proc.stdout or "").strip()
        target_raw = requested_date_raw or discovered_target_raw
        if requested_date_raw and target_raw != discovered_target_raw:
            log.info(
                "Manual target_date requested=%s (discovered oldest=%s)",
                requested_date_raw,
                discovered_target_raw,
            )

        if not target_raw:
            return {
                "should_run": False,
                "reason": "no_data_arrived",
                "target_date_raw": None,
                "target_date_iso": None,
                "run_id": None,
                "experimental": experimental,
                "target_products_total": target_products_total,
                "line_count": line_count,
                "producer_count": producer_count,
                "replay_speed": replay_speed,
                "line_seeds": line_seeds,
            }

        target_iso = f"{target_raw[0:4]}-{target_raw[4:6]}-{target_raw[6:8]}"
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM welding.pipeline_heartbeat
                    WHERE details->>'target_date' = %s
                      AND details->>'status' = 'REPLAY_COMPLETED'
                      AND component_name IN (
                          'airflow.welding_post_quality_control',
                          'airflow.welding_batch_ingest'
                      )
                    """,
                    (target_iso,),
                )
                already_done = (cur.fetchone()[0] or 0) > 0

        if already_done and not experimental:
            return {
                "should_run": False,
                "reason": "already_replayed",
                "target_date_raw": target_raw,
                "target_date_iso": target_iso,
                "run_id": None,
                "experimental": experimental,
                "target_products_total": target_products_total,
                "line_count": line_count,
                "producer_count": producer_count,
                "replay_speed": replay_speed,
                "line_seeds": line_seeds,
            }

        if already_done and experimental:
            log.info(
                "experimental=true -> allow replay for already_replayed date=%s",
                target_iso,
            )

        return {
            "should_run": True,
            "reason": "ready",
            "target_date_raw": target_raw,
            "target_date_iso": target_iso,
            "run_id": str(uuid.uuid4()),
            "experimental": experimental,
            "target_products_total": target_products_total,
            "line_count": line_count,
            "producer_count": producer_count,
            "replay_speed": replay_speed,
            "line_seeds": line_seeds,
        }

    @task.short_circuit()
    def gate_on_target(target_context: dict) -> bool:
        should_run = bool(target_context.get("should_run"))
        log.info("Ingest gate=%s context=%s", should_run, target_context)
        return should_run

    @task()
    def build_line_plan(target_context: dict) -> list[int]:
        if not target_context.get("should_run"):
            return []
        line_count = int(target_context.get("line_count") or REPLAY_LINE_COUNT)
        return list(range(1, line_count + 1))

    @task()
    def snapshot_offsets_before(target_context: dict) -> dict:
        topics = _topics_for_delivery_check()
        offsets = _offset_snapshot(topics)
        return {"topics": topics, "before": offsets}

    @task()
    def run_producer_for_line(line_number: int, target_context: dict):
        target_raw = target_context["target_date_raw"]
        shard_index = line_number - 1
        shard_total = int(target_context.get("line_count") or REPLAY_LINE_COUNT)
        per_line_target = 0
        target_products_total = int(target_context.get("target_products_total") or 0)
        if target_products_total > 0:
            base = target_products_total // shard_total
            rem = target_products_total % shard_total
            per_line_target = base + (1 if shard_index < rem else 0)

        # 1 producer container = 1 line task (isolated docker run), not shared docker exec.
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
            str(target_context.get("line_seeds") or REPLAY_LINE_SEEDS),
            "--speed",
            str(target_context.get("replay_speed") or REPLAY_SPEED),
            "--ingest-run-id",
            str(target_context.get("run_id") or ""),
            "--no-schedule-wait",
        ]
        if TOPIC_RAW:
            cmd_parts.extend(["--topic", TOPIC_RAW])
        if TOPIC_RAW_LASER_A:
            cmd_parts.extend(["--topic-laser-a", TOPIC_RAW_LASER_A])
        if TOPIC_RAW_LASER_B:
            cmd_parts.extend(["--topic-laser-b", TOPIC_RAW_LASER_B])
        if per_line_target > 0:
            cmd_parts.extend(["--target-products", str(per_line_target)])

        cmd_for_log = " ".join(shlex.quote(p) for p in cmd_parts)
        log.info("producer line_number=%s start: %s", line_number, cmd_for_log)
        result = subprocess.run(cmd_parts, timeout=PRODUCER_TIMEOUT_SEC)
        if result.returncode != 0:
            raise RuntimeError(
                f"producer failed: line_number={line_number}, returncode={result.returncode}"
            )
        return {"line_number": line_number, "status": "success"}

    @task()
    def confirm_producer_delivery(
        producer_results: list[dict], offset_before_meta: dict, target_context: dict
    ) -> dict:
        del producer_results  # explicit: dependency anchor
        topics = offset_before_meta["topics"]
        before = offset_before_meta["before"]
        after = _offset_snapshot(topics)
        delta_by_topic = {
            topic: int(after.get(topic, 0)) - int(before.get(topic, 0)) for topic in topics
        }
        total_delta = sum(delta_by_topic.values())
        if total_delta <= 0:
            raise RuntimeError(
                f"producer delivery check failed: no offset increase. delta={delta_by_topic}"
            )
        return {
            **target_context,
            "delivery_topics": topics,
            "offset_before": before,
            "offset_after": after,
            "offset_delta": delta_by_topic,
            "offset_total_delta": total_delta,
        }

    @task()
    def emit_ingest_heartbeat(delivery_meta: dict) -> dict:
        details = {
            "target_date": delivery_meta.get("target_date_iso"),
            "status": "INGEST_COMPLETED",
            "run_id": delivery_meta.get("run_id"),
            "offset_total_delta": delivery_meta.get("offset_total_delta"),
            "offset_delta": delivery_meta.get("offset_delta"),
            "line_count": int(delivery_meta.get("line_count") or REPLAY_LINE_COUNT),
            "producer_count": int(delivery_meta.get("producer_count") or REPLAY_LINE_COUNT),
            "target_products_total": int(delivery_meta.get("target_products_total") or 0),
            "replay_speed": float(delivery_meta.get("replay_speed") or REPLAY_SPEED),
            "experimental": bool(delivery_meta.get("experimental", False)),
        }
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO welding.pipeline_heartbeat (component_name, details)
                    VALUES (%s, %s::jsonb)
                    """,
                    ("airflow.welding_batch_ingest", json.dumps(details)),
                )
        return delivery_meta

    @task(outlets=[INGEST_COMPLETED_ASSET])
    def publish_ingest_asset(delivery_meta: dict) -> dict:
        log.info(
            "Publishing ingest asset: run_id=%s target_date=%s experimental=%s",
            delivery_meta.get("run_id"),
            delivery_meta.get("target_date_iso"),
            delivery_meta.get("experimental"),
        )
        return delivery_meta

    target_context = discover_target_date()
    gate = gate_on_target(target_context)
    line_plan = build_line_plan(target_context)
    offset_before = snapshot_offsets_before(target_context)

    producers = run_producer_for_line.partial(target_context=target_context).expand(
        line_number=line_plan
    )
    delivery_ok = confirm_producer_delivery(producers, offset_before, target_context)
    heartbeat = emit_ingest_heartbeat(delivery_ok)
    published = publish_ingest_asset(heartbeat)

    if ENABLE_TRIGGER_FALLBACK:
        trigger_spark_processing = TriggerDagRunOperator(
            task_id="trigger_spark_processing",
            trigger_dag_id="welding_spark_processing",
            conf={
                "run_id": "{{ ti.xcom_pull(task_ids='publish_ingest_asset')['run_id'] }}",
                "target_date_raw": "{{ ti.xcom_pull(task_ids='publish_ingest_asset')['target_date_raw'] }}",
                "target_date_iso": "{{ ti.xcom_pull(task_ids='publish_ingest_asset')['target_date_iso'] }}",
                "experimental": "{{ ti.xcom_pull(task_ids='publish_ingest_asset')['experimental'] }}",
                "source_dag_run_id": "{{ run_id }}",
            },
            wait_for_completion=False,
            reset_dag_run=False,
        )
        published >> trigger_spark_processing

    gate >> [line_plan, offset_before]
    [line_plan, offset_before] >> producers
    producers >> delivery_ok >> heartbeat >> published


welding_batch_ingest_dag()
