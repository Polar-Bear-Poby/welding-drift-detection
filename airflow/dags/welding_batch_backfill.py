"""
welding_batch_backfill.py - Airflow 3 Version (수정)
====================================================
특정 날짜의 용접 데이터를 수동으로 소급 재처리하는 DAG.
`schedule=None`으로 설정되어 있어 반드시 수동 트리거해야 한다.

수정 내역 (2026-04-28):
  - [fix #1] run_producer: --date-folder → --data-dir /data (실제 producer.py 인자와 일치)
             run_spark_batch: --input-dir /data --run-id {run_id} 사용
             (producer.py: --data-dir, --oldest-date-only 지원 / --date-folder 미지원)
             (spark_batch.py: --input-dir, --run-id 지원 / --date-folder 미지원)
  - [fix #6] DAG에서 run_id를 UUID로 생성 → XCom으로 전달 → spark_batch에 --run-id 주입
             validate_results에서 "최근 30분 최신 run" 대신 해당 run_id만 조회
  - [fix #7] DB 연결을 환경변수 기반으로 변경, 하드코딩 경로를 env로 분리

사용법 (Airflow UI → Trigger DAG w/ config):
    {
      "target_date": "20220417",
      "line_count": 3,
      "line_seeds": "42,73,128",
      "replay_speed": 100,
      "force_overwrite": false
    }
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timedelta

import psycopg2
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.standard.operators.bash import BashOperator

log = logging.getLogger(__name__)

# ── 환경변수 기반 설정 (fix #7) ─────────────────────────────────────────
_PG_HOST    = os.getenv("POSTGRES_HOST",     "postgres")
_PG_PORT    = os.getenv("POSTGRES_PORT",     "5432")
_PG_DB      = os.getenv("POSTGRES_DB",       "welding_drift")
_PG_USER    = os.getenv("POSTGRES_USER",     "welding")
_PG_PASS    = os.getenv("POSTGRES_PASSWORD", "")
DB_CONN_STR = f"host={_PG_HOST} port={_PG_PORT} dbname={_PG_DB} user={_PG_USER} password={_PG_PASS}"

# [fix #7] 컨테이너 내부 경로: docker-compose.yml 기준 마운트 경로
_DATA_DIR_CONTAINER    = os.getenv("BACKFILL_DATA_DIR",    "/data")
_STORAGE_DIR_CONTAINER = os.getenv("BACKFILL_STORAGE_DIR", "/spark-out/spark_batch")
_PRODUCER_CONTAINER = os.getenv("PRODUCER_CONTAINER", "welding-producer")
_PRODUCER_IMAGE = os.getenv("PRODUCER_IMAGE", "welding-kafka-submission-producer:latest")
_NETWORK_NAME = os.getenv("NETWORK_NAME", "welding-kafka-submission_welding-net")
_KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")


@dag(
    dag_id="welding_batch_backfill",
    schedule=None,
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["welding", "airflow3", "backfill", "manual"],
    params={
        "target_date": Param(
            default="20220417",
            type="string",
            description="재처리할 날짜 (YYYYMMDD). 예: 20220417",
        ),
        "line_count": Param(default=3, type="integer", description="생산 라인 수"),
        "consumer_count": Param(default=6, type="integer", description="컨슈머 수 (짝수)"),
        "line_seeds": Param(
            default="42,73,128",
            type="string",
            description="라인별 랜덤 시드 (쉼표 구분)",
        ),
        "replay_speed": Param(default=100, type="number", description="재생 속도 배율"),
        "force_overwrite": Param(
            default=False,
            type="boolean",
            description="True이면 기존 데이터 삭제 후 재처리",
        ),
    },
    default_args={
        "owner": "welding-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    doc_md="""
## welding_batch_backfill

**목적**: 날짜 지정 소급 재처리. 수동 트리거 전용.

**트리거 예시 (Airflow UI → Trigger DAG w/ config)**
```json
{
  "target_date": "20220417",
  "line_count": 3,
  "line_seeds": "42,73,128",
  "replay_speed": 100,
  "force_overwrite": false
}
```

**수정 사항 (2026-04-28)**
- `--date-folder` 제거 → `--data-dir /data` (producer.py 실제 인자)
- `--input-dir /data` + `--run-id {uuid}` (spark_batch.py 실제 인자)
- `run_id`를 DAG가 생성해 validate에서 해당 run만 검증
- DB/경로 설정을 환경변수 기반으로 분리
    """,
)
def welding_batch_backfill_dag():
    def _get_stage_event_detail(run_id: str, stage_name: str) -> dict:
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
            raise RuntimeError(f"run_id={run_id} stage_event missing: {stage_name}")
        status, detail_text = row
        if status != "SUCCESS":
            raise RuntimeError(
                f"run_id={run_id} stage_event={stage_name} status={status} (expected SUCCESS)"
            )
        try:
            return json.loads(detail_text or "{}")
        except Exception:
            return {"raw": detail_text}

    @task()
    def validate_params(**context) -> dict:
        """파라미터 검증 + run_id 생성."""
        params = context["params"]
        raw_date    = str(params["target_date"]).strip()
        line_count  = int(params["line_count"])
        consumer_count = int(params["consumer_count"])
        line_seeds  = str(params["line_seeds"]).strip()
        replay_speed = float(params["replay_speed"])
        force_overwrite = bool(params["force_overwrite"])

        # 날짜 포맷 검증
        try:
            dt = datetime.strptime(raw_date, "%Y%m%d")
            target_date_iso = dt.strftime("%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(f"target_date 형식 오류 (YYYYMMDD 필요): {raw_date}") from exc

        if consumer_count % 2 != 0 or consumer_count < 2:
            raise ValueError(f"consumer_count는 짝수 >= 2: {consumer_count}")

        seeds = [s.strip() for s in line_seeds.split(",")]
        if len(seeds) != line_count:
            raise ValueError(
                f"line_seeds 개수({len(seeds)}) != line_count({line_count})"
            )

        # [fix #6] run_id를 여기서 생성 → spark_batch에 전달 → validate에서 동일 run_id 조회
        run_id = str(uuid.uuid4())

        validated = {
            "target_date_raw": raw_date,
            "target_date_iso": target_date_iso,
            "line_count": line_count,
            "consumer_count": consumer_count,
            "line_seeds": line_seeds,
            "replay_speed": replay_speed,
            "force_overwrite": force_overwrite,
            "run_id": run_id,          # ← 핵심
        }
        log.info("파라미터 검증 완료: %s", json.dumps(validated, ensure_ascii=False))
        return validated

    @task.branch()
    def check_existing_data(validated: dict) -> str:
        target_date = validated["target_date_iso"]
        force = validated["force_overwrite"]

        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM welding.pattern_summary WHERE event_date = %s",
                    (target_date,),
                )
                existing_count = cur.fetchone()[0]

        log.info("%s 기존 데이터: %d 행, force_overwrite: %s", target_date, existing_count, force)

        if force and existing_count > 0:
            return "clear_existing_data"
        elif existing_count > 0:
            return "skip_already_done"
        else:
            return "run_producer"

    @task()
    def clear_existing_data(validated: dict) -> dict:
        """force_overwrite=True: 해당 날짜 데이터 전체 삭제."""
        target_date = validated["target_date_iso"]
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM welding.pattern_segment WHERE event_date = %s", (target_date,))
                seg_del = cur.rowcount
                cur.execute("DELETE FROM welding.pattern_summary WHERE event_date = %s", (target_date,))
                sum_del = cur.rowcount
                cur.execute("DELETE FROM welding.daily_report WHERE report_date = %s", (target_date,))
                rep_del = cur.rowcount
        log.info("삭제 완료 — segment: %d, summary: %d, daily_report: %d", seg_del, sum_del, rep_del)
        return validated

    @task()
    def skip_already_done(validated: dict):
        target_date = validated["target_date_iso"]
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    (
                        "airflow.welding_batch_backfill",
                        json.dumps({
                            "status": "skipped_existing",
                            "target_date": target_date,
                            "hint": "Set force_overwrite=true to reprocess",
                        }),
                    ),
                )
        log.info("%s: 데이터 이미 존재 → 스킵 (force_overwrite=False)", target_date)

    # ── [fix #1] run_producer: --data-dir /data/{target_date} ─────────────
    # 6차 정책: producer 실행은 docker exec 대신 docker run --rm로 통일한다.
    run_producer = BashOperator(
        task_id="run_producer",
        bash_command=(
            "docker run --rm "
            f"--network {_NETWORK_NAME} "
            f"--volumes-from {_PRODUCER_CONTAINER} "
            f"{_PRODUCER_IMAGE} "
            "--data-dir /data/{{ params.target_date }} "
            f"--kafka {_KAFKA_BOOTSTRAP} "
            "--line-count {{ params.line_count }} "
            "--line-seed \"{{ params.line_seeds }}\" "
            "--speed {{ params.replay_speed }} "
            "--no-schedule-wait"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    # ── [fix #1 + #6] run_spark_batch: --input-dir + --run-id 전달 ──────
    # run_id는 validate_params XCom에서 Jinja로 참조할 수 없으므로
    # BashOperator를 @task로 교체하여 Python에서 직접 명령 구성
    @task()
    def run_spark_batch(validated: dict):
        """
        [fix #1] --date-folder 제거 → --input-dir /data
        [fix #6] --run-id로 DAG 생성 run_id 전달
        """
        import subprocess
        run_id      = validated["run_id"]
        target_date = validated["target_date_raw"]  # YYYYMMDD

        cmd = (
            "docker exec welding-spark-master /opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "/opt/spark/apps/spark_batch.py "
            f"--input-dir {_DATA_DIR_CONTAINER}/{target_date} "
            f"--output-dir {_STORAGE_DIR_CONTAINER} "
            "--write-postgres "
            f"--postgres-host {_PG_HOST} "
            f"--postgres-port {_PG_PORT} "
            f"--postgres-db {_PG_DB} "
            f"--postgres-user {_PG_USER} "
            f"--postgres-password {_PG_PASS} "
            f"--run-id {run_id}"
        )
        log.info("Spark Batch 실행: run_id=%s, date=%s", run_id, target_date)
        result = subprocess.run(cmd, shell=True, timeout=1800)
        if result.returncode != 0:
            raise RuntimeError(f"spark_batch.py 실패: returncode={result.returncode}")

    @task(retries=3, retry_delay=timedelta(minutes=2))
    def validate_run_status(validated: dict) -> dict:
        """Validate spark_batch_run status for this run_id."""
        run_id      = validated["run_id"]
        target_date = validated["target_date_iso"]

        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                # 해당 run_id의 spark_batch_run 상태 확인
                cur.execute(
                    "SELECT status, total_segment_rows, total_summary_rows "
                    "FROM welding.spark_batch_run "
                    "WHERE run_id = %s",
                    (run_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError(
                        f"run_id={run_id}에 해당하는 spark_batch_run 레코드가 없습니다. "
                        "spark_batch.py가 --write-postgres 없이 실행됐을 가능성 있음."
                    )

                status, seg_rows, sum_rows = row
                if status != "SUCCESS":
                    raise RuntimeError(f"run_id={run_id}: status={status} (SUCCESS 아님)")
                if sum_rows == 0:
                    raise RuntimeError(f"run_id={run_id}: summary 행이 0개")
        return {
            "run_id": run_id,
            "target_date": target_date,
            "segment_rows": int(seg_rows),
            "summary_rows": int(sum_rows),
        }

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def validate_chunk_complete(run_meta: dict) -> dict:
        run_id = run_meta["run_id"]
        detail = _get_stage_event_detail(run_id, "chunk_complete")
        return {**run_meta, "chunk_complete": detail}

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def validate_segmentation_stage(run_meta: dict) -> dict:
        run_id = run_meta["run_id"]
        detail = _get_stage_event_detail(run_id, "segmentation_complete")
        return {**run_meta, "segmentation_stage": detail}

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def validate_inference_stage(run_meta: dict) -> dict:
        run_id = run_meta["run_id"]
        detail = _get_stage_event_detail(run_id, "inference_complete")
        return {**run_meta, "inference_stage": detail}

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def validate_load_stage(run_meta: dict) -> dict:
        run_id = run_meta["run_id"]
        detail = _get_stage_event_detail(run_id, "load_complete")
        return {**run_meta, "load_stage": detail}

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def validate_reassembly_integrity(run_meta: dict) -> dict:
        """Validate reassembly integrity only when run-scoped audit rows exist."""
        run_id = run_meta["run_id"]
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = 'welding'
                          AND table_name = 'reassembly_audit'
                    )
                    """
                )
                has_table = bool(cur.fetchone()[0])
                if not has_table:
                    return {**run_meta, "reassembly_validation": "skipped_no_table"}

                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM welding.reassembly_audit
                    WHERE batch_id IN (
                        SELECT id
                        FROM welding.spark_batch_run
                        WHERE run_id = %s::uuid
                    )
                    """,
                    (run_id,),
                )
                scoped_rows = int(cur.fetchone()[0] or 0)
                if scoped_rows == 0:
                    return {**run_meta, "reassembly_validation": "skipped_no_rows"}

                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM welding.reassembly_audit
                    WHERE batch_id IN (
                        SELECT id
                        FROM welding.spark_batch_run
                        WHERE run_id = %s::uuid
                    )
                    AND reassembly_status IN ('incomplete_chunks', 'mixed_chunk_metadata', 'sample_count_mismatch')
                    """,
                    (run_id,),
                )
                bad_rows = int(cur.fetchone()[0] or 0)
        if bad_rows > 0:
            raise RuntimeError(f"run_id={run_id} reassembly integrity failed: bad_rows={bad_rows}")
        return {**run_meta, "reassembly_validation": "passed"}

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def validate_segmentation_coverage(run_meta: dict) -> dict:
        """Validate per-product/channel segmentation count == 16."""
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
        """Validate per-product total 32 segment rows (2 channels x 16 segments)."""
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
        """Validate per-product summary has both channels (2 rows)."""
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
        return {**run_meta, "summary_products": int(product_count)}

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def validate_business_rule(run_meta: dict) -> dict:
        """Validate summary decision is consistent with segment drift aggregation."""
        run_id = run_meta["run_id"]
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
                f"run_id={run_id} business rule failed: invalid quality_decision rows={invalid_rows}"
            )
        if (inconsistent_rows or 0) > 0:
            raise RuntimeError(
                f"run_id={run_id} business rule failed: inconsistent_rows={inconsistent_rows}"
            )
        return {**run_meta, "drift_rows": int(drift_rows or 0), "summary_rows": int(total_rows or 0)}


    @task()
    def report_backfill_complete(validation_result: dict, **context):
        """백필 완료 heartbeat 기록."""
        params = context["params"]
        details = {
            "status": "BACKFILL_COMPLETED",
            **validation_result,
            "force_overwrite": params.get("force_overwrite", False),
        }
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    ("airflow.welding_batch_backfill", json.dumps(details)),
                )
        log.info("백필 완료: %s", json.dumps(details, ensure_ascii=False))

    # ── 의존성 연결 ──────────────────────────────────────────────
    validated = validate_params()
    branch = check_existing_data(validated)

    # force_overwrite 경로
    cleared = clear_existing_data(validated)
    branch >> cleared >> run_producer

    # 데이터 없음 → 바로 producer
    branch >> run_producer

    # 이미 있고 force=False → 스킵
    branch >> skip_already_done(validated)

    # producer 이후 공통 흐름: spark_batch에 validated(run_id 포함) 전달
    batch = run_spark_batch(validated)
    run_status = validate_run_status(validated)
    chunk_ok = validate_chunk_complete(run_status)
    stage_seg_ok = validate_segmentation_stage(chunk_ok)
    stage_inf_ok = validate_inference_stage(stage_seg_ok)
    stage_load_ok = validate_load_stage(stage_inf_ok)
    reassembly_ok = validate_reassembly_integrity(stage_load_ok)
    segmentation_ok = validate_segmentation_coverage(reassembly_ok)
    inference_ok = validate_inference_coverage(segmentation_ok)
    summary_ok = validate_summary_completeness(inference_ok)
    validation = validate_business_rule(summary_ok)
    run_producer >> batch >> run_status >> chunk_ok >> stage_seg_ok >> stage_inf_ok >> stage_load_ok >> reassembly_ok >> segmentation_ok >> inference_ok >> summary_ok >> validation >> report_backfill_complete(validation)


welding_batch_backfill_dag()


