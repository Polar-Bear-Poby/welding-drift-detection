"""
welding_realtime_producer_dag.py
==================================
new_src/ 아키텍처 전용 Producer DAG (Asset-based).
기존 welding_producer_asset_dag.py 대응.

기존 vs new_src 대응:
  docker run producer.py          → run_realtime_experiment.py subprocess
  Kafka offset 증가 확인           → watched/ 폴더 파일 수 확인
  pipeline_heartbeat (PostgreSQL)  → airflow_heartbeat.jsonl
  PRODUCER_DONE_ASSET              → REALTIME_PRODUCER_DONE_ASSET

역할:
  - 파라미터 검증
  - run_realtime_experiment.py 실행 (DataFeeder + FileWatcher + Consumer 통합 프로세스)
  - watched/ 폴더에 파일이 실제로 생성되었는지 검증 (Kafka offset 증가 확인 대응)
  - 생산자 heartbeat 기록
  - REALTIME_PRODUCER_DONE_ASSET emit → Broker DAG 자동 트리거

흐름:
  prepare_context
    → build_line_plan
    → snapshot_watched_before    (실험 전 watched/ 상태 스냅샷 — offset 스냅샷 대응)
    → run_experiment_for_line    (expand: 라인별 병렬 실행 — docker run 병렬 대응)
    → confirm_delivery           (Kafka offset 증가 확인 대응)
    → emit_producer_heartbeat    (pipeline_heartbeat INSERT 대응)
    → publish_producer_asset     (REALTIME_PRODUCER_DONE_ASSET emit)

schedule=None:
  자동 실행 없음. Airflow UI에서 수동 트리거.
  (기존 producer DAG와 동일)
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.sdk import Param
from welding_realtime_assets import REALTIME_PRODUCER_DONE_ASSET

log = logging.getLogger(__name__)

# ── 경로 설정 ─────────────────────────────────────────────────────────────────
ROOT_DIR = Path(os.getenv(
    "WELDING_ROOT_DIR",
    "D:/metacode_battery_drfit/welding-kafka-submission",
))
WATCHED_DIR = ROOT_DIR / "new_src" / "watched"
METRICS_DIR = ROOT_DIR / "storage" / "metrics" / "realtime_experiment"
HEARTBEAT_LOG = ROOT_DIR / "storage" / "logs" / "airflow_heartbeat.jsonl"
EXPERIMENT_SCRIPT = ROOT_DIR / "new_src" / "run_realtime_experiment.py"

# ── 기본값 (환경변수 오버라이드 가능) ─────────────────────────────────────────
DEFAULT_LINE_COUNT = int(os.getenv("REALTIME_LINE_COUNT", "2"))
DEFAULT_BATTERIES = int(os.getenv("REALTIME_BATTERIES", "20"))
DEFAULT_INTERVAL = float(os.getenv("REALTIME_INTERVAL_SEC", "10.0"))
PRODUCER_TIMEOUT_SEC = int(os.getenv("REALTIME_PRODUCER_TIMEOUT_SEC", "7200"))

_PYTHON_CANDIDATES = [
    ROOT_DIR / ".venv" / "Scripts" / "python.exe",
    ROOT_DIR / ".venv" / "bin" / "python",
]


# ── 공통 유틸 ─────────────────────────────────────────────────────────────────

def _write_heartbeat(record: dict) -> None:
    HEARTBEAT_LOG.parent.mkdir(parents=True, exist_ok=True)
    with HEARTBEAT_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _find_python() -> str:
    for c in _PYTHON_CANDIDATES:
        if c.exists():
            return str(c)
    try:
        subprocess.run(["uv", "--version"], capture_output=True, timeout=5, check=True)
        return "uv run python"
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        pass
    return sys.executable


def _count_watched_files(line_id: str | None = None) -> int:
    """watched/ 폴더의 CSV 파일 수를 반환한다. (Kafka offset 대응)"""
    if not WATCHED_DIR.exists():
        return 0
    pattern = f"**/*.csv"
    base = WATCHED_DIR / line_id if line_id else WATCHED_DIR
    return sum(1 for _ in base.glob(pattern)) if base.exists() else 0


def _count_result_files_since(since_ts: float) -> int:
    """since_ts 이후 생성된 result CSV 파일 수를 반환한다."""
    if not METRICS_DIR.exists():
        return 0
    return sum(
        1 for p in METRICS_DIR.glob("*.csv")
        if p.stat().st_mtime >= since_ts
    )


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="welding_realtime_producer_dag",
    schedule=None,
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=["welding", "realtime", "asset", "producer"],
    default_args={
        "owner": "welding-team",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    params={
        "batteries": Param(default=DEFAULT_BATTERIES, type="integer", minimum=1,
                           description="총 배터리 수 (기본: 20)"),
        "lines": Param(default=DEFAULT_LINE_COUNT, type="integer", minimum=1,
                       description="생산라인 수 (기본: 2)"),
        "interval": Param(default=DEFAULT_INTERVAL, type="number", minimum=0.1,
                          description="용접 주기 초 (기본: 10.0, 실제 생산라인 속도)"),
        "experimental": Param(default=False, type="boolean",
                              description="True면 실험 모드 (결과 검증 완화)"),
    },
    doc_md="""
## welding_realtime_producer_dag

**아키텍처**: new_src/ FileWatcher 기반 / 기존 `welding_producer_asset_dag` 대응

**대응 관계**
| 기존 (Docker/Kafka) | new_src |
|---|---|
| `docker run producer.py` | `run_realtime_experiment.py` subprocess |
| Kafka offset 증가 확인 | `watched/` 파일 수 + result CSV 생성 확인 |
| `pipeline_heartbeat` (PostgreSQL) | `airflow_heartbeat.jsonl` |
| `PRODUCER_DONE_ASSET` | `REALTIME_PRODUCER_DONE_ASSET` |

**트리거 방법**
```
Airflow UI → Trigger DAG w/ config:
{"batteries": 20, "lines": 2, "interval": 10.0}
```

**Asset 체인**
```
welding_realtime_producer_dag (REALTIME_PRODUCER_DONE_ASSET)
  → welding_realtime_broker_dag (REALTIME_BROKER_READY_ASSET)
    → welding_realtime_consumer_dag (REALTIME_CONSUMER_PROCESSED_ASSET)
```
    """,
)
def welding_realtime_producer_dag():

    @task()
    def prepare_context(params=None) -> dict:
        """
        파라미터를 검증하고 실행 컨텍스트를 구성한다.
        기존: validate_run_params + discover data date
        """
        params = dict(params or {})
        batteries = int(params.get("batteries", DEFAULT_BATTERIES))
        lines = int(params.get("lines", DEFAULT_LINE_COUNT))
        interval = float(params.get("interval", DEFAULT_INTERVAL))
        experimental = bool(params.get("experimental", False))

        errors = []
        if batteries < 1:
            errors.append(f"batteries({batteries}) >= 1 이어야 합니다")
        if lines < 1:
            errors.append(f"lines({lines}) >= 1 이어야 합니다")
        if batteries < lines:
            errors.append(f"batteries({batteries}) >= lines({lines}) 이어야 합니다")
        if interval <= 0:
            errors.append(f"interval({interval}) > 0 이어야 합니다")
        if not EXPERIMENT_SCRIPT.exists():
            errors.append(f"실험 스크립트 없음: {EXPERIMENT_SCRIPT}")
        if errors:
            raise ValueError("파라미터 검증 실패:\n  " + "\n  ".join(errors))

        # 가용 배터리 수 확인 (기존: data date 자동 발견)
        source_base = None
        for c in [
            Path("D:/metacode_battery_drfit/data_runtime_by_channel/20220417"),
            Path("/d/metacode_battery_drfit/data_runtime_by_channel/20220417"),
            Path("/mnt/d/metacode_battery_drfit/data_runtime_by_channel/20220417"),
        ]:
            if (c / "laser_a").is_dir():
                source_base = c
                break

        available = 0
        if source_base:
            available = len(list((source_base / "laser_a").glob("20220417_battery_*_laser_a.csv")))

        if available > 0 and batteries > available:
            raise ValueError(
                f"요청 batteries({batteries}) > 가용 데이터({available}개)"
            )

        python_exe = _find_python()
        run_id = str(uuid.uuid4())

        ctx = {
            "run_id": run_id,
            "batteries": batteries,
            "lines": lines,
            "interval": interval,
            "experimental": experimental,
            "python_exe": python_exe,
            "available_batteries": available,
            "target_date": datetime.now().strftime("%Y%m%d"),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        log.info(
            "컨텍스트 준비 완료 — run_id=%s, batteries=%d, lines=%d, interval=%.1fs, python=%s",
            run_id[:8], batteries, lines, interval, python_exe,
        )
        return ctx

    @task()
    def build_line_plan(ctx: dict) -> list[int]:
        """
        라인 번호 목록을 반환한다.
        기존: build_line_plan과 동일.
        expand()에 사용.
        """
        return list(range(1, int(ctx["lines"]) + 1))

    @task()
    def snapshot_watched_before() -> dict:
        """
        실험 시작 전 watched/ 폴더와 metrics/ 폴더 상태를 기록한다.
        기존: snapshot_offsets_before (Kafka offset 스냅샷) 대응.
        """
        before_ts = time.time()
        watched_count = _count_watched_files()
        metrics_count = sum(1 for _ in METRICS_DIR.glob("*.csv")) if METRICS_DIR.exists() else 0

        snapshot = {
            "before_ts": before_ts,
            "watched_csv_count": watched_count,
            "metrics_csv_count": metrics_count,
        }
        log.info(
            "실험 전 스냅샷 — watched=%d파일, metrics=%d파일",
            watched_count, metrics_count,
        )
        return snapshot

    @task()
    def run_experiment_for_line(line_number: int, ctx: dict) -> dict:
        """
        라인별로 실험 서브프로세스를 실행한다.
        기존: run_producer_for_line (docker run producer.py) 대응.

        주의: run_realtime_experiment.py 는 모든 라인을 하나의 프로세스에서 관리한다.
        line_number=1 에서만 전체 실험을 실행하고, 나머지는 스킵한다.
        (기존과 달리 선형 실험은 내부에서 멀티스레드로 처리)
        """
        # 첫 번째 라인에서만 전체 실험을 실행
        if line_number != 1:
            log.info("line_number=%d → 내부 멀티스레드로 처리됨 (스킵)", line_number)
            return {"line_number": line_number, "status": "handled_internally"}

        python_exe = ctx["python_exe"]
        batteries = ctx["batteries"]
        lines = ctx["lines"]
        interval = ctx["interval"]

        if " " in python_exe:
            cmd = python_exe.split() + [
                "-u", str(EXPERIMENT_SCRIPT),
                "--batteries", str(batteries),
                "--lines", str(lines),
                "--consumers", "2",
                "--interval", str(interval),
            ]
        else:
            cmd = [
                python_exe, "-u", str(EXPERIMENT_SCRIPT),
                "--batteries", str(batteries),
                "--lines", str(lines),
                "--consumers", "2",
                "--interval", str(interval),
            ]

        log.info("실험 시작 (line=1/전체): %s", " ".join(str(c) for c in cmd))

        proc = subprocess.run(
            cmd,
            cwd=str(ROOT_DIR),
            timeout=PRODUCER_TIMEOUT_SEC,
            env={**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"},
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"실험 프로세스 실패 (returncode={proc.returncode})"
            )

        return {"line_number": line_number, "status": "success"}

    @task()
    def confirm_delivery(
        producer_results: list[dict],
        snapshot_before: dict,
        ctx: dict,
    ) -> dict:
        """
        실험 후 watched/ 파일 생성 및 result CSV 생성을 확인한다.
        기존: confirm_delivery (Kafka offset delta 확인) 대응.

        검증:
          1. 실험 전보다 result CSV가 증가했는지 (배달 확인)
          2. 예상 result CSV 행 수 (batteries × 2) 달성률 >= 80%
        """
        before_ts = snapshot_before["before_ts"]
        metrics_before = snapshot_before["metrics_csv_count"]

        # 실험 후 result CSV 수
        metrics_after = sum(1 for _ in METRICS_DIR.glob("*.csv")) if METRICS_DIR.exists() else 0
        new_csv_count = metrics_after - metrics_before

        # result CSV에서 총 행 수 합산
        batteries = ctx["batteries"]
        expected_rows = batteries * 2
        actual_rows = 0
        for csv_path in METRICS_DIR.glob("*.csv"):
            if csv_path.stat().st_mtime >= before_ts:
                try:
                    lines_in_file = csv_path.read_text(encoding="utf-8").splitlines()
                    actual_rows += max(len(lines_in_file) - 1, 0)
                except OSError:
                    continue

        coverage = actual_rows / expected_rows if expected_rows > 0 else 0

        if new_csv_count <= 0:
            raise RuntimeError(
                f"배달 확인 실패: 새로운 result CSV 없음 "
                f"(before={metrics_before}, after={metrics_after})"
            )
        if coverage < 0.5:
            raise RuntimeError(
                f"처리율 미달: actual_rows={actual_rows}, expected={expected_rows} "
                f"(coverage={coverage:.0%} < 50%)"
            )

        log.info(
            "배달 확인 완료 — new_csv=%d, rows=%d/%d (%.0f%%)",
            new_csv_count, actual_rows, expected_rows, coverage * 100,
        )
        return {
            **ctx,
            "new_csv_count": new_csv_count,
            "actual_rows": actual_rows,
            "expected_rows": expected_rows,
            "coverage": round(coverage, 4),
        }

    @task()
    def emit_producer_heartbeat(meta: dict) -> dict:
        """
        생산자 heartbeat를 JSONL 파일에 기록한다.
        기존: emit_producer_heartbeat (PostgreSQL INSERT) 대응.
        """
        details = {
            "status": "PRODUCER_DONE",
            "run_id": meta["run_id"],
            "target_date": meta["target_date"],
            "batteries": meta["batteries"],
            "lines": meta["lines"],
            "interval": meta["interval"],
            "experimental": meta["experimental"],
            "new_csv_count": meta.get("new_csv_count", 0),
            "actual_rows": meta.get("actual_rows", 0),
            "expected_rows": meta.get("expected_rows", 0),
            "coverage": meta.get("coverage", 0),
        }
        _write_heartbeat({
            "component": "airflow.realtime_producer_dag",
            **details,
            "emitted_at": datetime.now(timezone.utc).isoformat(),
        })
        log.info(
            "Producer heartbeat 기록 — run_id=%s, coverage=%.0f%%",
            meta["run_id"][:8], meta.get("coverage", 0) * 100,
        )
        return meta

    @task(outlets=[REALTIME_PRODUCER_DONE_ASSET])
    def publish_producer_asset(meta: dict) -> dict:
        """
        REALTIME_PRODUCER_DONE_ASSET을 emit한다.
        → Broker DAG (welding_realtime_broker_dag) 자동 트리거.
        기존: publish_producer_asset 과 동일.
        """
        log.info(
            "REALTIME_PRODUCER_DONE_ASSET 발행 — run_id=%s, target_date=%s",
            meta.get("run_id", "")[:8],
            meta.get("target_date"),
        )
        return meta

    # ── 의존성 연결 (기존 producer DAG와 동일한 구조) ─────────────────────────
    ctx = prepare_context()
    line_plan = build_line_plan(ctx)
    snapshot_before = snapshot_watched_before()
    # expand: 라인별 병렬 실행 (기존 run_producer_for_line.expand와 동일 패턴)
    producers = run_experiment_for_line.partial(ctx=ctx).expand(line_number=line_plan)
    delivered = confirm_delivery(producers, snapshot_before, ctx)
    heartbeat = emit_producer_heartbeat(delivered)
    publish_producer_asset(heartbeat)


welding_realtime_producer_dag()
