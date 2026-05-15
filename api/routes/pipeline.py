from __future__ import annotations

import subprocess
from typing import Any

import psycopg2
from fastapi import APIRouter, Depends, Query

from api.deps import get_db
from api.services.query_service import fetch_pattern_segments

router = APIRouter(prefix="/api/v1/pipeline", tags=["pipeline"])

STAGE_ORDER = [
    "chunk_complete",
    "segmentation_complete",
    "inference_complete",
    "load_complete",
]

STAGE_LABELS = {
    "chunk_complete":        "재결합 완료",
    "segmentation_complete": "16균등 분할 완료",
    "inference_complete":    "드리프트 추론 완료",
    "load_complete":         "DB 적재 완료",
}

SPARK_MASTER_CONTAINER = "welding-spark-master"
KAFKA_CONTAINER        = "welding-kafka"
KAFKA_BOOTSTRAP        = "kafka:9092"


# ── 내부 헬퍼 ───────────────────────────────────────────────────────────────

def _docker_exec(*args: str, timeout: int = 5) -> str:
    """컨테이너에서 명령을 실행하고 stdout을 반환합니다. 실패 시 빈 문자열."""
    try:
        result = subprocess.run(
            ["docker", "exec", *args],
            capture_output=True, text=True, timeout=timeout,
        )
        return result.stdout.strip()
    except Exception:
        return ""


def _kafka_lag(group_id: str) -> int | None:
    """Consumer Group의 총 LAG 합계를 반환합니다. 조회 실패 시 None."""
    out = _docker_exec(
        KAFKA_CONTAINER,
        "kafka-consumer-groups.sh",
        "--bootstrap-server", KAFKA_BOOTSTRAP,
        "--group", group_id,
        "--describe",
        timeout=8,
    )
    if not out:
        return None
    total_lag = 0
    for line in out.splitlines():
        parts = line.split()
        # 헤더·빈 줄 제외: 형식 = GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
        if len(parts) >= 6 and parts[5].lstrip("-").isdigit():
            lag_val = int(parts[5])
            if lag_val >= 0:
                total_lag += lag_val
    return total_lag


# ── 엔드포인트 1: 파이프라인 단계별 상태 ────────────────────────────────────

@router.get("/stages")
def pipeline_stages(
    conn: psycopg2.extensions.connection = Depends(get_db),
) -> dict[str, Any]:
    """
    가장 최근 run_id 기준으로 stage_event 4단계 상태를 반환합니다.

    단계 순서: chunk_complete → segmentation_complete → inference_complete → load_complete
    """
    with conn.cursor() as cur:
        # 가장 최근 run_id 조회
        cur.execute("""
            SELECT run_id::text
            FROM welding.stage_event
            ORDER BY started_at DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        if not row:
            return {"run_id": None, "stages": []}

        run_id = row["run_id"]

        cur.execute("""
            SELECT stage_name, status,
                   started_at::text AS started_at,
                   ended_at::text   AS ended_at,
                   error_code, error_message
            FROM welding.stage_event
            WHERE run_id = %s::uuid
        """, (run_id,))
        rows_by_name = {r["stage_name"]: dict(r) for r in cur.fetchall()}

    stages = []
    for name in STAGE_ORDER:
        if name in rows_by_name:
            r = rows_by_name[name]
            stages.append({
                "name":          name,
                "label":         STAGE_LABELS.get(name, name),
                "status":        r["status"],
                "started_at":    r["started_at"],
                "ended_at":      r["ended_at"],
                "error_code":    r.get("error_code"),
                "error_message": r.get("error_message"),
            })
        else:
            stages.append({
                "name":          name,
                "label":         STAGE_LABELS.get(name, name),
                "status":        "PENDING",
                "started_at":    None,
                "ended_at":      None,
                "error_code":    None,
                "error_message": None,
            })

    return {"run_id": run_id, "stages": stages}


# ── 엔드포인트 2: 실험 진행 현황 ─────────────────────────────────────────────

@router.get("/progress")
def pipeline_progress(
    total_batteries: int = Query(default=20, ge=1, description="실험 목표 배터리 수"),
    conn: psycopg2.extensions.connection = Depends(get_db),
) -> dict[str, Any]:
    """
    현재 DB에 적재된 배터리 수와 목표 수를 비교해 진행률을 반환합니다.
    Kafka Consumer Group LAG도 함께 반환합니다.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(DISTINCT product_id)          AS processed_batteries,
                COUNT(*)                            AS total_segments,
                MAX(processed_at)::text             AS latest_processed_at,
                (SELECT product_id
                 FROM welding.pattern_segment
                 ORDER BY processed_at DESC LIMIT 1) AS latest_product_id
            FROM welding.pattern_segment
        """)
        row = dict(cur.fetchone())

    processed = row["processed_batteries"] or 0
    pct = round(processed / total_batteries * 100, 1) if total_batteries else 0

    lag_a = _kafka_lag("welding-stream-laser-a")
    lag_b = _kafka_lag("welding-stream-laser-b")

    return {
        "processed_batteries":  processed,
        "total_batteries":      total_batteries,
        "progress_pct":         pct,
        "total_segments":       row["total_segments"] or 0,
        "latest_product_id":    row["latest_product_id"],
        "latest_processed_at":  row["latest_processed_at"],
        "kafka_lag_laser_a":    lag_a,
        "kafka_lag_laser_b":    lag_b,
    }


# ── 엔드포인트 3: Spark 로그 조회 ────────────────────────────────────────────

@router.get("/logs")
def spark_logs(
    consumer_id: int = Query(default=1, ge=1, le=10, description="컨슈머 번호 (1=laser_a, 2=laser_b)"),
    lines: int = Query(default=40, ge=5, le=200, description="마지막 N줄"),
) -> dict[str, Any]:
    """
    마운트된 볼륨(/storage/logs)에서 Spark 컨슈머 로그 마지막 N줄을 반환합니다.
    """
    log_path = f"/storage/logs/spark_consumer_{consumer_id}.log"
    content = ""
    
    try:
        import os
        if os.path.exists(log_path):
            with open(log_path, "r", encoding="utf-8") as f:
                # 마지막 N줄만 읽기 (간단한 구현)
                all_lines = f.readlines()
                content = "".join(all_lines[-lines:])
    except Exception as exc:
        content = f"(로그 읽기 오류: {exc})"

    if not content:
        content = f"(로그 없음 — {log_path} 파일이 존재하지 않거나 비어 있습니다.)"

    return {
        "consumer_id": consumer_id,
        "channel":     "laser_a" if consumer_id % 2 == 1 else "laser_b",
        "log_path":    log_path,
        "lines":       lines,
        "content":     content,
    }


@router.get("/segments/{product_id}")
def pipeline_segments(
    product_id: str,
    conn: psycopg2.extensions.connection = Depends(get_db),
) -> list[dict[str, Any]]:
    """특정 배터리의 16구간 분할 및 드리프트 상세 결과를 반환합니다."""
    return fetch_pattern_segments(conn, product_id)
