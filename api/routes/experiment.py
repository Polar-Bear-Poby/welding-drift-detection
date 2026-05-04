from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Literal

from fastapi import APIRouter
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/v1", tags=["experiment"])

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"


def find_bash() -> str:
    """Git Bash 실행 파일 경로를 반환합니다. PATH → 일반 설치 경로 순으로 탐색."""
    bash = shutil.which("bash")
    if bash:
        return bash
    for candidate in [
        r"C:\Program Files\Git\bin\bash.exe",
        r"C:\Program Files\Git\usr\bin\bash.exe",
        r"C:\Program Files (x86)\Git\bin\bash.exe",
    ]:
        if Path(candidate).exists():
            return candidate
    return "bash"


def to_bash_path(win_path: Path) -> str:
    """Windows 경로를 Git Bash(MSYS2/Cygwin) 호환 Unix 경로로 변환합니다.
    예: D:\\scripts\\foo.sh  →  /d/scripts/foo.sh
    """
    s = str(win_path).replace("\\", "/")
    if len(s) >= 2 and s[1] == ":":
        drive = s[0].lower()
        rest = s[2:].lstrip("/")
        s = f"/{drive}/{rest}"
    return s


BASH = find_bash()
START_PIPELINE_SH = to_bash_path(SCRIPTS_DIR / "start_always_on_pipeline.sh")
MEASURE_SH = to_bash_path(SCRIPTS_DIR / "measure_p1_c1_stream_timing.sh")



class ExperimentRequest(BaseModel):
    # 실험 조건
    line_count: int = Field(default=2, ge=1, le=10, description="생산라인 수")
    consumer_count: int | None = Field(
        default=2,
        description="컨슈머 수 (기본 2대, 직접 늘릴 때만 변경. 반드시 짝수)",
    )

    @property
    def resolved_consumer_count(self) -> int:
        """None 또는 0 이하이면 최솟값 2, 홀수면 +1 보정."""
        base = self.consumer_count if (self.consumer_count and self.consumer_count > 0) else 2
        return base if base % 2 == 0 else base + 1
    max_products: int = Field(default=10, ge=1, le=500, description="원본 배터리 수")
    target_products: int = Field(default=0, ge=0, description="총 발행 수 (0=max_products×line_count)")
    speed: float = Field(default=220.0, ge=1.0, description="재생 속도 배율")
    line_seeds: str = Field(default="", description="라인별 셔플 시드 (예: '1,2,3')")
    date_folder: str = Field(default="20220417", description="데이터 날짜 폴더")

    # 파이프라인 제어
    stop_runtime_after_run: bool = Field(default=True, description="실험 후 런타임 컨테이너 종료 (producer, kafka, zookeeper, spark-master, spark-worker) — Airflow·PostgreSQL은 유지")
    down_after_run: bool = Field(default=False, description="실험 후 전체 컨테이너 종료 (Airflow 포함 모두 종료, 기본 비활성)")


class StepResult(BaseModel):
    step: str
    returncode: int
    stdout: str
    stderr: str


class ExperimentResponse(BaseModel):
    status: Literal["ok", "error"]
    steps: list[StepResult]
    summary: str  # measure_p1_c1_stream_timing.sh 결과 txt 내용


def _run_sh(cmd: list[str], env: dict[str, str] | None = None, timeout: int = 120) -> StepResult:
    """bash 스크립트를 실행하고 StepResult로 반환합니다."""
    import os
    run_env = os.environ.copy()
    if env:
        run_env.update(env)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(PROJECT_ROOT),
            env=run_env,
        )
        return StepResult(
            step=cmd[-1].split("/")[-1],
            returncode=result.returncode,
            stdout=result.stdout[-6000:] if result.stdout else "",
            stderr=result.stderr[-3000:] if result.stderr else "",
        )
    except subprocess.TimeoutExpired:
        return StepResult(
            step=cmd[-1].split("/")[-1],
            returncode=-1,
            stdout="",
            stderr=f"TimeoutExpired: {timeout}초 초과",
        )
    except Exception as exc:
        return StepResult(
            step=cmd[-1].split("/")[-1],
            returncode=-1,
            stdout="",
            stderr=str(exc),
        )


@router.post("/experiment/run", response_model=ExperimentResponse)
def run_experiment(payload: ExperimentRequest) -> ExperimentResponse:
    """
    ## 실험 실행 순서

    1. **start_always_on_pipeline.sh** — 필수 컨테이너(Kafka, Spark, Airflow, PostgreSQL) 점검 및 자동 기동
    2. **measure_p1_c1_stream_timing.sh** — 환경변수로 파라미터를 전달해 실험 실행

    Kafka·Spark·PostgreSQL Docker 컨테이너가 없으면 Step 1에서 자동으로 올립니다.
    """
    steps: list[StepResult] = []

    # ── Step 1: 컨테이너 점검 & 자동 기동 ──────────────────────────────────
    step1 = _run_sh(
        [BASH, START_PIPELINE_SH],
        env={
            "LINE_COUNT": str(payload.line_count),
            "CONSUMER_COUNT": str(payload.resolved_consumer_count),
            "BUILD_PRODUCER_IMAGE": "0",   # 이미 빌드돼 있다고 가정
            "UNPAUSE_BATCH_DAG": "0",
        },
        timeout=180,
    )
    steps.append(step1)

    if step1.returncode != 0:
        return ExperimentResponse(
            status="error",
            steps=steps,
            summary="Step 1 실패 — 컨테이너 기동 오류. stderr를 확인하세요.",
        )

    # ── Step 2: 실험 실행 ──────────────────────────────────────────────────
    consumer_count = payload.resolved_consumer_count

    measure_env: dict[str, str] = {
        "LINE_COUNT": str(payload.line_count),
        "CONSUMER_COUNT": str(consumer_count),
        "MAX_PRODUCTS": str(payload.max_products),
        "TARGET_PRODUCTS": str(payload.target_products),
        "REPLAY_SPEED": str(payload.speed),
        "DATE_FOLDER": payload.date_folder,
        "DOWN_AFTER_RUN": "1" if payload.down_after_run else "0",
        "STOP_RUNTIME_AFTER_RUN": "1" if payload.stop_runtime_after_run else "0",
    }
    if payload.line_seeds.strip():
        measure_env["LINE_SEEDS"] = payload.line_seeds.strip()

    measure_args = [BASH, MEASURE_SH]
    if payload.down_after_run:
        measure_args.append("--down-after-run")
    if payload.stop_runtime_after_run:
        measure_args.append("--stop-runtime-after-run")

    step2 = _run_sh(measure_args, env=measure_env, timeout=900)
    steps.append(step2)

    # ── 결과 요약 파일 읽기 ────────────────────────────────────────────────
    summary = ""
    metrics_dir = PROJECT_ROOT / "storage" / "metrics" / "p1c1"
    if metrics_dir.exists():
        txt_files = sorted(metrics_dir.glob("p1c1_timing_*.txt"), key=lambda f: f.stat().st_mtime, reverse=True)
        if txt_files:
            try:
                summary = txt_files[0].read_text(encoding="utf-8")
            except Exception:
                summary = "(결과 파일 읽기 실패)"

    overall_status = "ok" if step2.returncode == 0 else "error"
    return ExperimentResponse(
        status=overall_status,
        steps=steps,
        summary=summary,
    )
