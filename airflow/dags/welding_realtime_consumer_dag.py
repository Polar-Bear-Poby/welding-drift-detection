"""
welding_realtime_consumer_dag.py
==================================
new_src/ 아키텍처 전용 Consumer DAG (Asset-based).
기존 welding_consumer_asset_dag.py 대응.

기존 vs new_src 대응:
  stage_event 테이블 폴링             → result CSV 행 수 폴링
  pattern_segment 16개 검증           → result CSV 행 존재 검증
  pattern_summary coverage 검증       → batteries × 2 행 커버리지 검증
  pipeline_heartbeat (PostgreSQL)     → airflow_heartbeat.jsonl
  BROKER_READY_ASSET 트리거           → REALTIME_BROKER_READY_ASSET 트리거
  CONSUMER_PROCESSED_ASSET emit       → REALTIME_CONSUMER_PROCESSED_ASSET emit

역할:
  - Broker DAG 완료(REALTIME_BROKER_READY_ASSET) 후 자동 트리거
  - Broker 컨텍스트 읽기 (heartbeat.jsonl)
  - result CSV 완료 대기 (stage_event SUCCESS 폴링 대응)
  - 세그먼트 커버리지 검증 (pattern_segment 16개 검증 대응)
    → result CSV drift_segments 컬럼으로 검증
  - 채널 완성도 검증 (pattern_summary 2채널 검증 대응)
    → laser_a + laser_b 각각 존재 여부
  - Consumer heartbeat 기록
  - REALTIME_CONSUMER_PROCESSED_ASSET emit

흐름:
  prepare_run_context                  (dag_run conf + BROKER_READY fallback 대응)
    → wait_result_complete             (wait_chunk/segmentation/load_complete 대응)
    → validate_segment_coverage        (validate_segmentation_coverage 대응)
    → validate_channel_completeness    (validate_inference_coverage 대응)
    → validate_summary_completeness    (validate_summary_completeness 대응)
    → emit_consumer_heartbeat          (pipeline_heartbeat INSERT 대응)
    → publish_consumer_asset           (REALTIME_CONSUMER_PROCESSED_ASSET emit)

schedule=[REALTIME_BROKER_READY_ASSET]:
  Broker DAG가 Asset을 emit하면 자동 트리거.
  (기존 Consumer DAG와 동일한 Asset-based 스케줄링)
"""

from __future__ import annotations

import csv
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from welding_realtime_assets import REALTIME_BROKER_READY_ASSET, REALTIME_CONSUMER_PROCESSED_ASSET

log = logging.getLogger(__name__)

# ── 경로 설정 ─────────────────────────────────────────────────────────────────
ROOT_DIR = Path(os.getenv(
    "WELDING_ROOT_DIR",
    "D:/metacode_battery_drfit/welding-kafka-submission",
))
METRICS_DIR = ROOT_DIR / "storage" / "metrics" / "realtime_experiment"
HEARTBEAT_LOG = ROOT_DIR / "storage" / "logs" / "airflow_heartbeat.jsonl"

# ── 대기 파라미터 (기존 stage_event 폴링 파라미터 대응) ──────────────────────
RESULT_WAIT_TIMEOUT_SEC = int(os.getenv("REALTIME_RESULT_WAIT_TIMEOUT_SEC", "3600"))
RESULT_POLL_INTERVAL_SEC = int(os.getenv("REALTIME_RESULT_POLL_INTERVAL_SEC", "10"))
RESULT_PROGRESS_LOG_INTERVAL_SEC = int(os.getenv("REALTIME_RESULT_PROGRESS_LOG_INTERVAL_SEC", "30"))

# ── 커버리지 허용 비율 ────────────────────────────────────────────────────────
MIN_COVERAGE_RATIO = float(os.getenv("REALTIME_MIN_COVERAGE_RATIO", "0.8"))

# ── Broker fallback 허용 여부 (기존 ALLOW_LATEST_BROKER_FALLBACK 대응) ────────
ALLOW_BROKER_FALLBACK = os.getenv("REALTIME_ALLOW_BROKER_FALLBACK", "1").strip() in ("1", "true", "yes")
BROKER_FALLBACK_WINDOW_MIN = int(os.getenv("REALTIME_BROKER_FALLBACK_WINDOW_MIN", "30"))


# ── 공통 유틸 ─────────────────────────────────────────────────────────────────

def _write_heartbeat(record: dict) -> None:
    HEARTBEAT_LOG.parent.mkdir(parents=True, exist_ok=True)
    with HEARTBEAT_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _read_latest_broker_heartbeat(window_min: int = 30) -> dict:
    """
    heartbeat.jsonl에서 최근 window_min 분 내 BROKER_READY 레코드를 읽는다.
    기존: pipeline_heartbeat에서 BROKER_READY fallback SELECT 대응.
    """
    if not HEARTBEAT_LOG.exists():
        return {}
    cutoff_iso = datetime.fromtimestamp(
        time.time() - window_min * 60, tz=timezone.utc
    ).isoformat()

    lines = HEARTBEAT_LOG.read_text(encoding="utf-8").splitlines()
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            if (rec.get("component") == "airflow.realtime_broker_dag"
                    and rec.get("status") == "BROKER_READY"):
                emitted_at = rec.get("emitted_at", "")
                if emitted_at >= cutoff_iso:
                    return rec
        except json.JSONDecodeError:
            continue
    return {}


def _load_result_rows(run_since_ts: float | None = None) -> list[dict]:
    """
    METRICS_DIR 내 result CSV를 읽어 행 목록을 반환한다.
    run_since_ts 지정 시 해당 타임스탬프 이후 생성된 파일만 읽는다.
    기존: pattern_summary / pattern_segment DB 쿼리 대응.
    """
    rows: list[dict] = []
    if not METRICS_DIR.exists():
        return rows
    for csv_path in METRICS_DIR.glob("*.csv"):
        try:
            if run_since_ts and csv_path.stat().st_mtime < run_since_ts:
                continue
            with open(csv_path, encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(row)
        except (OSError, csv.Error) as exc:
            log.warning("CSV 읽기 실패 %s: %s", csv_path.name, exc)
    return rows


def _wait_result_complete(
    expected_rows: int,
    run_since_ts: float,
    timeout_sec: int,
    poll_sec: int,
    progress_log_sec: int,
) -> list[dict]:
    """
    expected_rows 행의 MIN_COVERAGE_RATIO 이상이 result CSV에 쌓일 때까지 폴링 대기.
    기존: _wait_stage_event_success (stage_event 테이블 폴링) 대응.
    """
    deadline = time.time() + max(timeout_sec, 30)
    last_progress = 0.0
    target = int(expected_rows * MIN_COVERAGE_RATIO)

    while time.time() < deadline:
        rows = _load_result_rows(run_since_ts)
        current = len(rows)

        now = time.time()
        if now - last_progress >= max(progress_log_sec, 5):
            log.info(
                "result CSV 대기 중 — current=%d / target=%d (%.0f%%)",
                current, target, (current / target * 100) if target else 0,
            )
            last_progress = now

        if current >= target:
            log.info("result 완료 확인 — rows=%d >= target=%d", current, target)
            return rows

        time.sleep(max(poll_sec, 1))

    # 타임아웃 — 현재까지 쌓인 것으로 진행 (기존 TimeoutError와 달리 경고로 처리)
    rows = _load_result_rows(run_since_ts)
    if not rows:
        raise TimeoutError(
            f"result CSV 없음 — {timeout_sec}초 대기 후 타임아웃. "
            f"Consumer가 실행됐는지 확인하세요."
        )
    log.warning(
        "대기 타임아웃 — 현재 rows=%d / expected=%d (%.0f%%)로 진행",
        len(rows), expected_rows, len(rows) / expected_rows * 100 if expected_rows else 0,
    )
    return rows


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="welding_realtime_consumer_dag",
    schedule=[REALTIME_BROKER_READY_ASSET],
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=["welding", "realtime", "asset", "consumer"],
    default_args={
        "owner": "welding-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    doc_md="""
## welding_realtime_consumer_dag

**아키텍처**: new_src/ FileWatcher 기반 / 기존 `welding_consumer_asset_dag` 대응

**트리거**: `REALTIME_BROKER_READY_ASSET` emit 시 자동 실행

**대응 관계**
| 기존 (Spark/PostgreSQL) | new_src (CSV) |
|---|---|
| `stage_event` 테이블 폴링 | result CSV 행 수 폴링 |
| `pattern_segment` 16세그먼트 검증 | result CSV `drift_segments` 컬럼 검증 |
| `pattern_summary` 2채널 완성도 | laser_a + laser_b 각각 존재 여부 |
| `pipeline_heartbeat` (PostgreSQL) | `airflow_heartbeat.jsonl` |
| `CONSUMER_PROCESSED_ASSET` | `REALTIME_CONSUMER_PROCESSED_ASSET` |

**주의**: 데이터 처리 자체는 Consumer 스레드가 담당. 이 DAG는 처리 결과를 "검증"만 한다.
    """,
)
def welding_realtime_consumer_dag():

    @task()
    def prepare_run_context(dag_run=None) -> dict:
        """
        DAG run conf 또는 heartbeat.jsonl fallback으로 실행 컨텍스트를 구성한다.
        기존: prepare_run_context (BROKER_READY fallback 포함) 대응.
        """
        conf = dict((dag_run.conf or {})) if dag_run else {}

        run_id = str(conf.get("run_id") or "").strip()
        batteries = int(conf.get("batteries") or 0)
        experimental = bool(conf.get("experimental", False))
        run_since_ts = float(conf.get("run_since_ts") or 0)

        # conf가 비어있으면 최신 Broker heartbeat로 fallback
        if not run_id or batteries < 1:
            if not ALLOW_BROKER_FALLBACK:
                raise ValueError(
                    "run_id/batteries 가 conf에 없고 BROKER fallback이 비활성화됨."
                )
            broker_ctx = _read_latest_broker_heartbeat(BROKER_FALLBACK_WINDOW_MIN)
            if not broker_ctx:
                raise RuntimeError(
                    f"최근 {BROKER_FALLBACK_WINDOW_MIN}분 내 BROKER_READY heartbeat 없음. "
                    "welding_realtime_broker_dag 먼저 실행 후 재시도하세요."
                )
            run_id = run_id or str(broker_ctx.get("run_id") or "").strip()
            batteries = batteries or int(broker_ctx.get("batteries") or 0)
            experimental = bool(broker_ctx.get("experimental", experimental))
            log.info(
                "BROKER fallback 적용 — run_id=%s, batteries=%d",
                run_id[:8] if run_id else "N/A", batteries,
            )

        if not run_id:
            raise ValueError("run_id 를 확인할 수 없습니다.")
        if batteries < 1:
            raise ValueError("batteries 를 확인할 수 없습니다.")

        expected_rows = batteries * 2  # laser_a + laser_b

        log.info(
            "Consumer 컨텍스트 준비 — run_id=%s, batteries=%d, expected_rows=%d",
            run_id[:8], batteries, expected_rows,
        )
        return {
            "run_id": run_id,
            "batteries": batteries,
            "experimental": experimental,
            "expected_rows": expected_rows,
            "run_since_ts": run_since_ts,
        }

    @task(retries=1, retry_delay=timedelta(minutes=1))
    def wait_result_complete(context: dict) -> dict:
        """
        result CSV에 예상 행 수(batteries × 2)의 MIN_COVERAGE_RATIO 이상이 쌓일 때까지 대기.
        기존: wait_chunk_complete + wait_load_complete (stage_event 폴링) 대응.

        핵심 개념:
          기존 stage_event는 Spark가 처리 완료 시 INSERT하는 신호 테이블.
          new_src에서는 result CSV 행 수가 그 신호 역할을 한다.
          Consumer가 배터리 하나를 처리할 때마다 CSV에 행이 추가된다.
        """
        expected_rows = context["expected_rows"]
        run_since_ts = float(context.get("run_since_ts") or 0)

        rows = _wait_result_complete(
            expected_rows=expected_rows,
            run_since_ts=run_since_ts,
            timeout_sec=RESULT_WAIT_TIMEOUT_SEC,
            poll_sec=RESULT_POLL_INTERVAL_SEC,
            progress_log_sec=RESULT_PROGRESS_LOG_INTERVAL_SEC,
        )
        return {**context, "result_rows": len(rows), "rows": rows}

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def validate_segment_coverage(run_meta: dict) -> dict:
        """
        각 배터리의 drift_segments 컬럼을 검증한다.
        기존: validate_segmentation_coverage (pattern_segment 16개 검증) 대응.

        기존: 배터리 × 채널당 정확히 16개 segment 존재 여부
        new_src: drift_segments 컬럼이 "N/M" 형식이고 M=16 이어야 함
                 (16 segment 중 N개가 drift)
        """
        rows: list[dict] = run_meta.get("rows", [])
        batteries = int(run_meta.get("batteries", 0))
        experimental = bool(run_meta.get("experimental", False))

        invalid_rows = []
        for row in rows:
            ds_val = row.get("drift_segments", "")
            if "/" in str(ds_val):
                try:
                    total_seg = int(str(ds_val).split("/")[1])
                    if total_seg != 16:
                        invalid_rows.append({
                            "battery": row.get("battery_id"),
                            "channel": row.get("channel_name"),
                            "drift_segments": ds_val,
                        })
                except (ValueError, IndexError):
                    pass  # 형식 없으면 무시

        total_keys = len(rows)
        invalid_count = len(invalid_rows)

        if total_keys == 0:
            raise RuntimeError("result CSV에 데이터 없음 (0행)")

        if invalid_count > 0:
            if experimental:
                log.warning(
                    "세그먼트 수 이상 (experimental 허용) — invalid=%d/%d: %s",
                    invalid_count, total_keys, invalid_rows[:3],
                )
            else:
                log.warning(
                    "세그먼트 수 이상 감지 — invalid=%d/%d (경고만, 실험 후 정리 가능)",
                    invalid_count, total_keys,
                )

        log.info(
            "세그먼트 커버리지 확인 완료 — total_keys=%d, invalid=%d",
            total_keys, invalid_count,
        )
        return {**run_meta, "segment_total_keys": total_keys, "segment_invalid": invalid_count}

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def validate_channel_completeness(run_meta: dict) -> dict:
        """
        배터리별 laser_a + laser_b 두 채널이 모두 존재하는지 검증한다.
        기존: validate_inference_coverage (channel_count 검증) 대응.

        기존: 배터리당 2개 채널(laser_a + laser_b) 정확히 존재
        new_src: result CSV에서 배터리별 channel_name 집합 검증
        """
        rows: list[dict] = run_meta.get("rows", [])
        batteries = int(run_meta.get("batteries", 0))
        experimental = bool(run_meta.get("experimental", False))

        # 배터리별 채널 집계
        by_battery: dict[str, set] = {}
        for row in rows:
            bid = str(row.get("battery_id") or row.get("battery") or "unknown")
            ch = str(row.get("channel_name") or "unknown")
            if bid not in by_battery:
                by_battery[bid] = set()
            by_battery[bid].add(ch)

        # 두 채널 모두 있는지 확인
        incomplete = {
            bid: sorted(channels)
            for bid, channels in by_battery.items()
            if not ({"laser_a", "laser_b"} <= channels)
        }
        complete_count = len(by_battery) - len(incomplete)

        coverage = complete_count / batteries if batteries > 0 else 0

        if incomplete:
            if experimental or coverage >= MIN_COVERAGE_RATIO:
                log.warning(
                    "채널 불완전 배터리 %d개 (커버리지 %.0f%%) — 샘플: %s",
                    len(incomplete), coverage * 100,
                    dict(list(incomplete.items())[:3]),
                )
            else:
                raise RuntimeError(
                    f"채널 완성도 미달 — 불완전 배터리: {len(incomplete)}개, "
                    f"커버리지: {coverage:.0%} < {MIN_COVERAGE_RATIO:.0%}"
                )
        else:
            log.info(
                "채널 완성도 검증 통과 — 완전한 배터리: %d/%d (%.0f%%)",
                complete_count, batteries, coverage * 100,
            )

        return {
            **run_meta,
            "channel_complete_batteries": complete_count,
            "channel_incomplete_batteries": len(incomplete),
            "channel_coverage": round(coverage, 4),
        }

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def validate_summary_completeness(run_meta: dict) -> dict:
        """
        result CSV 전체 통계를 집계하고 품질 분포를 확인한다.
        기존: validate_summary_completeness (pattern_summary 행 수 검증) 대응.

        기존: pattern_summary에 batteries × channels 행 존재
        new_src: result CSV에 같은 수의 행이 있어야 하며
                 quality_decision이 유효한 값(drift/normal)이어야 함
        """
        rows: list[dict] = run_meta.get("rows", [])
        batteries = int(run_meta.get("batteries", 0))
        experimental = bool(run_meta.get("experimental", False))

        total = len(rows)
        expected = batteries * 2
        drift_count = sum(1 for r in rows if r.get("quality_decision") == "drift")
        normal_count = sum(1 for r in rows if r.get("quality_decision") == "normal")
        unknown_count = total - drift_count - normal_count

        coverage = total / expected if expected > 0 else 0

        if total == 0:
            raise RuntimeError(f"result CSV 행 없음 (expected={expected})")

        if coverage < MIN_COVERAGE_RATIO:
            if experimental:
                log.warning(
                    "요약 완성도 미달 (experimental 허용) — rows=%d/%d (%.0f%%)",
                    total, expected, coverage * 100,
                )
            else:
                raise RuntimeError(
                    f"요약 완성도 미달 — actual={total}/{expected} "
                    f"(coverage={coverage:.0%} < {MIN_COVERAGE_RATIO:.0%})"
                )

        log.info(
            "요약 완성도 검증 완료 — rows=%d/%d (%.0f%%), drift=%d, normal=%d",
            total, expected, coverage * 100, drift_count, normal_count,
        )

        # cpd_score 통계 (기존 avg_cpd, max_cpd 대응)
        cpd_scores = []
        for r in rows:
            try:
                v = r.get("cpd_score")
                if v not in (None, "", "None"):
                    cpd_scores.append(float(v))
            except (ValueError, TypeError):
                pass

        return {
            **run_meta,
            "summary_rows": total,
            "summary_expected": expected,
            "summary_coverage": round(coverage, 4),
            "drift_count": drift_count,
            "normal_count": normal_count,
            "unknown_count": unknown_count,
            "avg_cpd": round(sum(cpd_scores) / len(cpd_scores), 6) if cpd_scores else None,
            "max_cpd": round(max(cpd_scores), 6) if cpd_scores else None,
        }

    @task()
    def emit_consumer_heartbeat(validation_result: dict) -> dict:
        """
        Consumer 처리 결과를 heartbeat.jsonl에 기록한다.
        기존: emit_consumer_heartbeat (PostgreSQL INSERT) 대응.
        """
        details = {
            "component": "airflow.realtime_consumer_dag",
            "status": "CONSUMER_PROCESSED",
            "run_id": validation_result.get("run_id"),
            "batteries": validation_result.get("batteries"),
            "summary_rows": validation_result.get("summary_rows"),
            "summary_expected": validation_result.get("summary_expected"),
            "summary_coverage": validation_result.get("summary_coverage"),
            "drift_count": validation_result.get("drift_count"),
            "normal_count": validation_result.get("normal_count"),
            "avg_cpd": validation_result.get("avg_cpd"),
            "max_cpd": validation_result.get("max_cpd"),
            "channel_coverage": validation_result.get("channel_coverage"),
            "experimental": bool(validation_result.get("experimental", False)),
            "emitted_at": datetime.now(timezone.utc).isoformat(),
        }
        _write_heartbeat(details)
        log.info(
            "Consumer heartbeat 기록 — run_id=%s, coverage=%.0f%%, drift=%d, normal=%d",
            str(validation_result.get("run_id") or "")[:8],
            (validation_result.get("summary_coverage") or 0) * 100,
            validation_result.get("drift_count", 0),
            validation_result.get("normal_count", 0),
        )
        return validation_result

    @task(outlets=[REALTIME_CONSUMER_PROCESSED_ASSET])
    def publish_consumer_asset(validation_result: dict) -> dict:
        """
        REALTIME_CONSUMER_PROCESSED_ASSET을 emit한다.
        → welding_realtime_daily_report 등 다운스트림 DAG 자동 트리거 가능.
        기존: publish_consumer_asset 과 동일.
        """
        log.info(
            "REALTIME_CONSUMER_PROCESSED_ASSET 발행 — run_id=%s, coverage=%.0f%%",
            str(validation_result.get("run_id") or "")[:8],
            (validation_result.get("summary_coverage") or 0) * 100,
        )
        return validation_result

    # ── 의존성 연결 (기존 consumer DAG와 동일한 구조) ─────────────────────────
    context = prepare_run_context()
    result_ready = wait_result_complete(context)
    seg_ok = validate_segment_coverage(result_ready)
    ch_ok = validate_channel_completeness(seg_ok)
    summary_ok = validate_summary_completeness(ch_ok)
    heartbeat = emit_consumer_heartbeat(summary_ok)
    publish_consumer_asset(heartbeat)


welding_realtime_consumer_dag()
