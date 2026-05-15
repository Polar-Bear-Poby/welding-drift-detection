"""
welding_realtime_manual_backfill.py
======================================
new_src/ 아키텍처 전용 — 수동 트리거 전용 소급 처리 DAG.
schedule=None: 자동 실행 없음, 운영자가 명시적으로 트리거해야 실행된다.

용도:
  - Consumer 장애로 처리 누락된 배터리 소급 분석
  - 파라미터를 바꿔 실험 반복 (interval, lines 조정)
  - welding_realtime_daily_report 가 "데이터 부족" 스킵 후 소급 처리

파라미터 (Airflow UI Trigger DAG w/ config):
  batteries (int, 기본 20): 재처리할 배터리 수
  lines     (int, 기본 2) : 생산라인 수
  interval  (float, 기본 3.0): 용접 주기 초 (backfill이므로 빠른 속도 권장)
  dry_run   (bool, 기본 False): True면 실행 계획만 출력하고 실제 실험 없음

흐름:
  validate_backfill_params
    → plan_backfill              (dry_run=True면 여기서 종료)
    → run_backfill_experiment    (run_realtime_experiment.py 실행)
    → verify_backfill_results    (result CSV 존재 여부 검증)
    → log_backfill_complete      (→ REALTIME_BACKFILL_DONE_ASSET emit)

실행 방법:
  Airflow UI → welding_realtime_manual_backfill → Trigger DAG w/ config:
  {
    "batteries": 20,
    "lines": 2,
    "interval": 3.0,
    "dry_run": false
  }

Airflow 역할 (핵심):
  평소에는 FileWatcher → Consumer 가 상시 자동 처리.
  이 DAG는 그 처리가 실패했을 때 사람이 수동으로 "재처리 명령"을 내리는 인터페이스.
  운영자가 Airflow UI에서 파라미터를 조작하고 실행 결과를 추적할 수 있다.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param
from welding_realtime_assets import REALTIME_BACKFILL_DONE_ASSET

log = logging.getLogger(__name__)

# ── 경로 설정 ─────────────────────────────────────────────────────────────────
ROOT_DIR = Path(os.getenv(
    "WELDING_ROOT_DIR",
    "D:/metacode_battery_drfit/welding-kafka-submission",
))
METRICS_DIR = ROOT_DIR / "storage" / "metrics" / "realtime_experiment"
HEARTBEAT_LOG = ROOT_DIR / "storage" / "logs" / "airflow_heartbeat.jsonl"

# Python 실행기 탐색 우선순위:
# 1. uv 가상환경 (Windows)
# 2. uv 가상환경 (Linux/Mac)
# 3. uv 명령어
# 4. 시스템 python
_PYTHON_CANDIDATES = [
    ROOT_DIR / ".venv" / "Scripts" / "python.exe",
    ROOT_DIR / ".venv" / "bin" / "python",
]
_EXPERIMENT_SCRIPT = ROOT_DIR / "new_src" / "run_realtime_experiment.py"
BACKFILL_TIMEOUT_SEC = int(os.getenv("REALTIME_BACKFILL_TIMEOUT_SEC", "3600"))


# ── 공통 유틸 ─────────────────────────────────────────────────────────────────

def _write_heartbeat(record: dict) -> None:
    HEARTBEAT_LOG.parent.mkdir(parents=True, exist_ok=True)
    with HEARTBEAT_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _find_python() -> str:
    """프로젝트 가상환경 Python 실행기를 탐색한다."""
    for candidate in _PYTHON_CANDIDATES:
        if candidate.exists():
            return str(candidate)
    # uv 사용 가능하면 uv run
    try:
        subprocess.run(["uv", "--version"], capture_output=True, timeout=5, check=True)
        return "uv run python"
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        pass
    # 폴백: 현재 Python
    return sys.executable


def _count_result_rows_after(start_ts: float) -> int:
    """start_ts 이후 생성/수정된 result CSV의 총 행 수를 반환한다."""
    if not METRICS_DIR.exists():
        return 0
    total = 0
    for csv_path in METRICS_DIR.glob("*.csv"):
        try:
            if csv_path.stat().st_mtime >= start_ts:
                lines = csv_path.read_text(encoding="utf-8").splitlines()
                total += max(len(lines) - 1, 0)
        except OSError:
            continue
    return total


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="welding_realtime_manual_backfill",
    schedule=None,
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "batteries": Param(
            default=20,
            type="integer",
            minimum=1,
            description="재처리할 배터리 수 (기본: 20)",
        ),
        "lines": Param(
            default=2,
            type="integer",
            minimum=1,
            description="생산라인 수 (기본: 2)",
        ),
        "interval": Param(
            default=3.0,
            type="number",
            minimum=0.1,
            description="용접 주기 초. Backfill이므로 빠른 속도 권장 (기본: 3.0)",
        ),
        "dry_run": Param(
            default=False,
            type="boolean",
            description="True면 실행 계획만 출력하고 실제 실험 없음",
        ),
    },
    tags=["welding", "realtime", "backfill", "manual"],
    default_args={
        "owner": "welding-team",
        "retries": 0,
    },
    doc_md="""
## welding_realtime_manual_backfill

**아키텍처**: new_src/ FileWatcher 기반 실시간 파이프라인

**목적**: 파이프라인 장애로 누락된 데이터를 수동으로 소급 처리한다.

**트리거 방법**
```
Airflow UI → Trigger DAG w/ config:
{
  "batteries": 20,
  "lines": 2,
  "interval": 3.0,
  "dry_run": false
}
```

**파라미터**
| 파라미터 | 기본값 | 설명 |
|---|---|---|
| batteries | 20 | 재처리할 배터리 수 |
| lines | 2 | 생산라인 수 |
| interval | 3.0 | 용접 주기 초 (빠른 속도 권장) |
| dry_run | false | True면 실행 계획만 출력 |

**언제 사용하는가**
1. Consumer 장애로 처리 누락된 날의 데이터 소급
2. `welding_realtime_daily_report` 가 "데이터 부족 스킵" 후 재처리
3. 특정 파라미터(라인 수, 배터리 수)로 실험 반복

**Airflow 역할 (핵심)**
평소에는 파이프라인이 자동으로 처리한다.
이 DAG는 운영자가 Airflow UI에서 재처리를 명시적으로 실행하는 인터페이스.
실행 기록이 Airflow DAG Run 이력으로 남아 감사(audit) 추적이 가능하다.
    """,
)
def welding_realtime_manual_backfill_dag():

    @task()
    def validate_backfill_params(params=None) -> dict:
        """
        백필 파라미터를 검증하고 실행 컨텍스트를 구성한다.

        검증 항목:
          - batteries >= 1
          - lines >= 1
          - batteries >= lines (라인당 최소 1개 배터리)
          - interval > 0
          - run_realtime_experiment.py 스크립트 존재 여부
          - 가용 배터리 CSV 개수 확인
        """
        params = dict(params or {})
        batteries = int(params.get("batteries", 20))
        lines = int(params.get("lines", 2))
        interval = float(params.get("interval", 3.0))
        dry_run = bool(params.get("dry_run", False))

        errors = []
        if batteries < 1:
            errors.append(f"batteries({batteries}) >= 1 이어야 합니다")
        if lines < 1:
            errors.append(f"lines({lines}) >= 1 이어야 합니다")
        if batteries < lines:
            errors.append(f"batteries({batteries}) >= lines({lines}) 이어야 합니다")
        if interval <= 0:
            errors.append(f"interval({interval}) > 0 이어야 합니다")
        if not _EXPERIMENT_SCRIPT.exists():
            errors.append(f"실험 스크립트 없음: {_EXPERIMENT_SCRIPT}")

        if errors:
            raise ValueError("파라미터 검증 실패:\n  " + "\n  ".join(errors))

        # 가용 배터리 수 확인
        source_base = None
        for c in [
            Path("D:/metacode_battery_drfit/data_runtime_by_channel/20220417"),
            Path("/d/metacode_battery_drfit/data_runtime_by_channel/20220417"),
            Path("/mnt/d/metacode_battery_drfit/data_runtime_by_channel/20220417"),
        ]:
            if (c / "laser_a").is_dir():
                source_base = c
                break

        available_count = 0
        if source_base:
            available_count = len(list((source_base / "laser_a").glob("20220417_battery_*_laser_a.csv")))

        if available_count > 0 and batteries > available_count:
            raise ValueError(
                f"요청 batteries({batteries}) > 가용 데이터({available_count}개). "
                f"batteries를 {available_count} 이하로 줄여주세요."
            )

        python_exe = _find_python()

        ctx = {
            "batteries": batteries,
            "lines": lines,
            "interval": interval,
            "dry_run": dry_run,
            "python_exe": python_exe,
            "script_path": str(_EXPERIMENT_SCRIPT),
            "available_batteries": available_count,
            "estimated_duration_sec": round((batteries / lines) * interval * 1.2, 1),
            "validated_at": datetime.now(timezone.utc).isoformat(),
        }
        log.info(
            "백필 파라미터 검증 완료 — batteries=%d, lines=%d, interval=%.1fs, "
            "dry_run=%s, python=%s, 예상소요=%.0fs",
            batteries, lines, interval, dry_run, python_exe,
            ctx["estimated_duration_sec"],
        )
        return ctx

    @task()
    def plan_backfill(ctx: dict) -> dict:
        """
        실행 계획을 출력하고 dry_run 모드에서는 여기서 종료한다.
        실제 실행(run_backfill_experiment)은 dry_run=False 일 때만 진행.
        """
        batteries = ctx["batteries"]
        lines = ctx["lines"]
        interval = ctx["interval"]
        dry_run = ctx["dry_run"]

        per_line = batteries // lines
        rem = batteries % lines

        log.info("=" * 60)
        log.info("백필 실행 계획")
        log.info("=" * 60)
        log.info("  배터리      : %d개", batteries)
        log.info("  생산라인    : %d대", lines)
        log.info("  용접 주기   : %.1f초", interval)
        log.info("  컨슈머      : 2대 고정 (laser_a + laser_b 전담)")
        log.info("  예상 소요   : %.0f초", ctx["estimated_duration_sec"])
        log.info("  스크립트    : %s", ctx["script_path"])
        log.info("  Python      : %s", ctx["python_exe"])
        log.info("")
        log.info("  라인별 분배:")
        for i in range(1, lines + 1):
            n = per_line + (1 if i <= rem else 0)
            log.info("    LINE_%02d: %d개 배터리", i, n)

        if dry_run:
            log.info("")
            log.info("dry_run=True → 실제 실험 없이 계획만 출력")
            log.info("=" * 60)
            _write_heartbeat({
                "component": "airflow.realtime_manual_backfill",
                "status": "dry_run",
                "plan": ctx,
                "planned_at": datetime.now(timezone.utc).isoformat(),
            })

        return ctx

    @task()
    def run_backfill_experiment(ctx: dict) -> dict:
        """
        run_realtime_experiment.py 를 서브프로세스로 실행한다.
        dry_run=True 이면 실행을 건너뛴다.

        실행 명령:
          python -u new_src/run_realtime_experiment.py
            --batteries N --lines N --consumers 2 --interval F

        Consumer 수는 항상 2 (laser_a 전담 1 + laser_b 전담 1).
        """
        dry_run = ctx["dry_run"]
        if dry_run:
            log.info("dry_run=True → 실험 실행 건너뜀")
            return {**ctx, "experiment_status": "skipped_dry_run", "output_rows": 0}

        python_exe = ctx["python_exe"]
        batteries = ctx["batteries"]
        lines = ctx["lines"]
        interval = ctx["interval"]

        # uv run python 처럼 공백이 있는 경우 처리
        if " " in python_exe:
            cmd = python_exe.split() + [
                "-u", str(_EXPERIMENT_SCRIPT),
                "--batteries", str(batteries),
                "--lines", str(lines),
                "--consumers", "2",
                "--interval", str(interval),
            ]
            shell = False
        else:
            cmd = [
                python_exe, "-u", str(_EXPERIMENT_SCRIPT),
                "--batteries", str(batteries),
                "--lines", str(lines),
                "--consumers", "2",
                "--interval", str(interval),
            ]
            shell = False

        log.info("백필 실험 시작: %s", " ".join(str(c) for c in cmd))

        start_ts = time.monotonic()
        wall_start = time.time()

        _write_heartbeat({
            "component": "airflow.realtime_manual_backfill",
            "status": "running",
            "batteries": batteries,
            "lines": lines,
            "interval": interval,
            "started_at": datetime.now(timezone.utc).isoformat(),
        })

        try:
            proc = subprocess.run(
                cmd,
                cwd=str(ROOT_DIR),
                capture_output=False,  # 출력을 Airflow 태스크 로그로 전달
                timeout=BACKFILL_TIMEOUT_SEC,
                env={**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"},
                shell=shell,
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"백필 실험 타임아웃 ({BACKFILL_TIMEOUT_SEC}초). "
                "interval을 줄이거나 batteries 수를 줄여주세요."
            )

        elapsed = time.monotonic() - start_ts

        if proc.returncode != 0:
            raise RuntimeError(
                f"백필 실험 실패 (returncode={proc.returncode}). "
                "Airflow 태스크 로그에서 Python 오류를 확인하세요."
            )

        # 실험 완료 후 생성된 result CSV 행 수 산출
        output_rows = _count_result_rows_after(wall_start)

        log.info(
            "백필 실험 완료 — elapsed=%.1fs, output_rows=%d",
            elapsed, output_rows,
        )
        return {
            **ctx,
            "experiment_status": "completed",
            "elapsed_sec": round(elapsed, 1),
            "output_rows": output_rows,
            "wall_start_ts": wall_start,
        }

    @task()
    def verify_backfill_results(ctx: dict) -> dict:
        """
        백필 실험 결과를 검증한다.

        검증 항목:
          - dry_run이면 스킵
          - 예상 result CSV 행 수: batteries × 2 (laser_a + laser_b)
          - 실제 output_rows가 예상치의 80% 이상이면 PASS
          - 미달이면 WARNING (실패 처리하지 않음 — 일부 파일 없을 수 있음)
        """
        dry_run = ctx["dry_run"]
        if dry_run:
            log.info("dry_run=True → 결과 검증 건너뜀")
            return {**ctx, "verification": "skipped"}

        batteries = ctx["batteries"]
        output_rows = ctx.get("output_rows", 0)
        expected_rows = batteries * 2

        ratio = output_rows / expected_rows if expected_rows > 0 else 0
        passed = ratio >= 0.8

        log.info(
            "결과 검증 — expected=%d, actual=%d (%.0f%%) → %s",
            expected_rows, output_rows, ratio * 100,
            "PASS" if passed else "WARNING",
        )

        if not passed:
            log.warning(
                "처리 건수 미달 (%d/%d = %.0f%%). "
                "일부 배터리 CSV가 없거나 Consumer 처리 중 오류가 발생했을 수 있습니다. "
                "storage/metrics/realtime_experiment/ 를 직접 확인하세요.",
                output_rows, expected_rows, ratio * 100,
            )

        return {
            **ctx,
            "verification": "pass" if passed else "warning",
            "expected_rows": expected_rows,
            "actual_rows": output_rows,
            "coverage_ratio": round(ratio, 4),
        }

    @task(outlets=[REALTIME_BACKFILL_DONE_ASSET])
    def log_backfill_complete(ctx: dict):
        """
        백필 완료를 heartbeat 파일에 기록하고 REALTIME_BACKFILL_DONE_ASSET을 emit한다.

        Airflow 감사 추적(audit trail):
          Airflow UI의 DAG Run 이력에 실행 시간, 파라미터, 결과가 남는다.
          추가로 heartbeat JSONL에도 기록되어 운영 로그로 활용 가능.
        """
        dry_run = ctx.get("dry_run", False)
        status = ctx.get("experiment_status", "unknown")
        verification = ctx.get("verification", "unknown")

        record = {
            "component": "airflow.realtime_manual_backfill",
            "status": status,
            "dry_run": dry_run,
            "batteries": ctx.get("batteries"),
            "lines": ctx.get("lines"),
            "interval": ctx.get("interval"),
            "elapsed_sec": ctx.get("elapsed_sec"),
            "output_rows": ctx.get("output_rows", 0),
            "expected_rows": ctx.get("expected_rows"),
            "coverage_ratio": ctx.get("coverage_ratio"),
            "verification": verification,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        _write_heartbeat(record)

        log.info(
            "백필 완료 기록 — status=%s, verification=%s, rows=%d/%s",
            status, verification,
            ctx.get("output_rows", 0),
            ctx.get("expected_rows", "?"),
        )
        return record

    # ── 의존성 연결 ──────────────────────────────────────────────────────────
    validated = validate_backfill_params()
    planned = plan_backfill(validated)
    ran = run_backfill_experiment(planned)
    verified = verify_backfill_results(ran)
    log_backfill_complete(verified)


welding_realtime_manual_backfill_dag()
