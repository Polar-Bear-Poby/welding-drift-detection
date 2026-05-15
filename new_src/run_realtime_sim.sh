#!/usr/bin/env bash
# =============================================================================
# run_realtime_sim.sh
# 실무 유사 레이저 용접 드리프트 탐지 시뮬레이션
#
# 구조:
#   생산라인 N대 → DataFeeder N개 (10초마다 파일 생성, 센서 API 대체)
#               → FileWatcher N개 (폴더 감시 → Kafka queue 전송)
#               → Consumer  C개 (짝수, laser_a/laser_b 각 C/2개)
#
# 사용:
#   bash new_src/run_realtime_sim.sh
#   bash new_src/run_realtime_sim.sh --lines 3 --batteries 30 --consumers 4
#   bash new_src/run_realtime_sim.sh --lines 2 --batteries 20 --interval 5
#
# 옵션:
#   --lines N       생산라인 수 (DataFeeder + FileWatcher 각 N개)  기본: 2
#   --batteries N   총 배터리 수 (라인별로 균등 분배)              기본: 20
#   --consumers N   컨슈머 수 (짝수 필수, laser_a/b 각 N/2개)     기본: 2
#   --interval N    용접 주기 초 (실제 생산: 10초)                 기본: 10
#   --no-ui         FastAPI / Streamlit / 브라우저 열지 않음
#   --no-cleanup    실험 전 중간 결과물 삭제하지 않음
#   --help          도움말
# =============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT_DIR="${ROOT_DIR}/new_src"

# ── 기본값 ────────────────────────────────────────────────────────────────────
LINE_COUNT=2
TOTAL_BATTERIES=20
CONSUMER_COUNT=2
INTERVAL_SEC=10
OPEN_UI=1          # 1: FastAPI + Streamlit 시작 및 브라우저 열기
DO_CLEANUP=1       # 1: 실험 전 중간 결과물 삭제

FASTAPI_PORT=8000
STREAMLIT_PORT=8501
AIRFLOW_PORT=8080

# ── 도움말 ────────────────────────────────────────────────────────────────────
usage() {
  cat <<EOF
사용법:
  bash new_src/run_realtime_sim.sh [옵션]

옵션:
  --lines N       생산라인 수 (기본: ${LINE_COUNT})
  --batteries N   총 배터리 수 (기본: ${TOTAL_BATTERIES})
  --interval N    용접 주기 초 (기본: ${INTERVAL_SEC}, 실제=10, 테스트=3)
  --no-ui         FastAPI / Streamlit 시작 및 브라우저 오픈 건너뜀
  --no-cleanup    실험 전 중간 결과물 삭제 건너뜀
  --help          도움말

  * 컨슈머는 항상 2대로 고정 (laser_a 전담 1대 + laser_b 전담 1대)

예시:
  bash new_src/run_realtime_sim.sh --lines 2 --batteries 20
  bash new_src/run_realtime_sim.sh --lines 2 --batteries 10 --interval 3
  bash new_src/run_realtime_sim.sh --lines 4 --batteries 40 --no-ui
EOF
}

# ── 인수 파싱 ─────────────────────────────────────────────────────────────────
parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --lines)      LINE_COUNT="${2:?--lines 값 필요}";     shift 2 ;;
      --batteries)  TOTAL_BATTERIES="${2:?--batteries 값 필요}"; shift 2 ;;
      --interval)   INTERVAL_SEC="${2:?--interval 값 필요}"; shift 2 ;;
      --no-ui)      OPEN_UI=0; shift ;;
      --no-cleanup) DO_CLEANUP=0; shift ;;
      --consumers)
        echo "WARN: --consumers 는 무시됩니다. 컨슈머는 2대로 고정입니다." >&2
        shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) echo "ERROR: 알 수 없는 옵션: $1" >&2; usage; exit 1 ;;
    esac
  done
}

# ── 유효성 검사 ───────────────────────────────────────────────────────────────
validate() {
  local errors=0
  if (( LINE_COUNT < 1 )); then
    echo "ERROR: --lines 는 1 이상이어야 합니다 (현재: ${LINE_COUNT})" >&2
    errors=1
  fi
  if (( TOTAL_BATTERIES < LINE_COUNT )); then
    echo "ERROR: --batteries(${TOTAL_BATTERIES}) 는 --lines(${LINE_COUNT}) 이상이어야 합니다" >&2
    errors=1
  fi
  if (( errors > 0 )); then exit 1; fi
}

# ── Python 실행기 확인 ────────────────────────────────────────────────────────
find_python() {
  if [[ -x "${ROOT_DIR}/.venv/Scripts/python.exe" ]]; then
    echo "${ROOT_DIR}/.venv/Scripts/python.exe"
  elif [[ -x "${ROOT_DIR}/.venv/bin/python" ]]; then
    echo "${ROOT_DIR}/.venv/bin/python"
  elif command -v uv >/dev/null 2>&1; then
    echo "uv run python"
  elif command -v python >/dev/null 2>&1; then
    echo "python"
  else
    echo "ERROR: Python 실행기를 찾을 수 없습니다." >&2
    exit 1
  fi
}

# ── ① 실험 전 중간 결과물 삭제 ───────────────────────────────────────────────
cleanup_before_run() {
  echo ""
  echo "┌─────────────────────────────────────────────────────────────┐"
  echo "│  [사전 정리] 이전 실험 중간 결과물 삭제 중...              │"
  echo "└─────────────────────────────────────────────────────────────┘"

  # watched/ 폴더 (DataFeeder가 생성한 임시 CSV)
  if [[ -d "${ROOT_DIR}/new_src/watched" ]]; then
    rm -rf "${ROOT_DIR}/new_src/watched"
    echo "  ✓ new_src/watched/ 삭제"
  fi

  # 이전 실험 결과 CSV (DB 적재 역할)
  local metrics_dir="${ROOT_DIR}/storage/metrics/realtime_experiment"
  mkdir -p "${metrics_dir}"
  local csv_count
  csv_count="$(find "${metrics_dir}" -name '*.csv' 2>/dev/null | wc -l || echo 0)"
  if (( csv_count > 0 )); then
    rm -f "${metrics_dir}/"*.csv
    echo "  ✓ storage/metrics/realtime_experiment/*.csv 삭제 (${csv_count}개)"
  fi

  # Airflow 하트비트 로그
  local hb_log="${ROOT_DIR}/storage/logs/airflow_heartbeat.jsonl"
  if [[ -f "${hb_log}" ]]; then
    rm -f "${hb_log}"
    echo "  ✓ storage/logs/airflow_heartbeat.jsonl 삭제"
  fi

  # Consumer 진행 로그
  local prog_log="${ROOT_DIR}/storage/logs/consumer_progress.jsonl"
  if [[ -f "${prog_log}" ]]; then
    rm -f "${prog_log}"
    echo "  ✓ storage/logs/consumer_progress.jsonl 삭제"
  fi

  # 실험 PID 파일
  local pid_file="${ROOT_DIR}/storage/logs/experiment.pid"
  if [[ -f "${pid_file}" ]]; then
    rm -f "${pid_file}"
    echo "  ✓ storage/logs/experiment.pid 삭제"
  fi

  echo "  ※ data/ 폴더(원본)는 유지됨"
  echo ""
}

# ── ② Airflow DAG 실행 기록 초기화 ──────────────────────────────────────────
reset_airflow_history() {
  echo "  [Airflow] DAG 실행 기록 초기화 시도..."

  # Docker 컨테이너 확인 (우선)
  if command -v docker >/dev/null 2>&1; then
    local container_name
    container_name="$(docker ps --format '{{.Names}}' 2>/dev/null \
      | grep -E "airflow.webserver|airflow-webserver" | head -1 || true)"
    if [[ -n "${container_name}" ]]; then
      docker exec "${container_name}" airflow db clean \
        --tables dag_run,task_instance,log,xcom,rendered_task_instance_fields,task_reschedule \
        --yes 2>/dev/null \
        && echo "  ✓ Airflow DB 초기화 완료 (container: ${container_name})" \
        || echo "  △ Airflow DB 초기화 실패 (무시)"
      echo "    접속: http://localhost:${AIRFLOW_PORT}  (id: admin / pw: admin)"
      return 0
    fi
  fi

  # 로컬 venv Airflow 확인
  local airflow_bin=""
  if [[ -x "${ROOT_DIR}/.venv/Scripts/airflow.exe" ]]; then
    airflow_bin="${ROOT_DIR}/.venv/Scripts/airflow.exe"
  elif [[ -x "${ROOT_DIR}/.venv/bin/airflow" ]]; then
    airflow_bin="${ROOT_DIR}/.venv/bin/airflow"
  elif command -v airflow >/dev/null 2>&1; then
    airflow_bin="airflow"
  fi

  if [[ -n "${airflow_bin}" ]]; then
    "${airflow_bin}" db clean \
      --tables dag_run,task_instance,log,xcom,rendered_task_instance_fields \
      --yes 2>/dev/null \
      && echo "  ✓ Airflow DB 초기화 완료 (local)" \
      || echo "  △ Airflow DB 초기화 실패 (무시)"
    echo "    접속: http://localhost:${AIRFLOW_PORT}  (id: admin / pw: admin)"
  else
    echo "  △ Airflow 미실행 — 기록 초기화 건너뜀"
  fi
}

# ── ③ FastAPI 백그라운드 시작 ─────────────────────────────────────────────────
start_fastapi() {
  local py="$1"
  local log_dir="${ROOT_DIR}/storage/logs"
  local log="${log_dir}/fastapi.log"
  local pid_file="${log_dir}/fastapi.pid"
  mkdir -p "${log_dir}"

  # 기존 프로세스 종료
  if [[ -f "${pid_file}" ]]; then
    local old_pid
    old_pid="$(cat "${pid_file}" 2>/dev/null || true)"
    [[ -n "${old_pid}" ]] && kill "${old_pid}" 2>/dev/null || true
    rm -f "${pid_file}"
  fi
  pkill -f "uvicorn api.main" 2>/dev/null || true
  sleep 1

  PYTHONPATH="${ROOT_DIR}" \
  PYTHONUNBUFFERED=1 \
    ${py} -m uvicorn api.main:app \
      --host 0.0.0.0 \
      --port "${FASTAPI_PORT}" \
      > "${log}" 2>&1 &
  echo $! > "${pid_file}"
  echo "  ✓ FastAPI 시작 (pid=$(cat "${pid_file}")) → http://localhost:${FASTAPI_PORT}/docs"
  echo "    로그: storage/logs/fastapi.log"
}

# ── ④ Streamlit 백그라운드 시작 ──────────────────────────────────────────────
start_streamlit() {
  local py="$1"
  local log_dir="${ROOT_DIR}/storage/logs"
  local log="${log_dir}/streamlit.log"
  local pid_file="${log_dir}/streamlit.pid"
  mkdir -p "${log_dir}"

  if [[ -f "${pid_file}" ]]; then
    local old_pid
    old_pid="$(cat "${pid_file}" 2>/dev/null || true)"
    [[ -n "${old_pid}" ]] && kill "${old_pid}" 2>/dev/null || true
    rm -f "${pid_file}"
  fi
  pkill -f "streamlit run" 2>/dev/null || true
  sleep 1

  PYTHONPATH="${ROOT_DIR}" \
  PYTHONUNBUFFERED=1 \
  API_BASE_URL="http://localhost:${FASTAPI_PORT}" \
    ${py} -m streamlit run "${ROOT_DIR}/frontend/app.py" \
      --server.port "${STREAMLIT_PORT}" \
      --server.headless true \
      --server.fileWatcherType none \
      > "${log}" 2>&1 &
  echo $! > "${pid_file}"
  echo "  ✓ Streamlit 시작 (pid=$(cat "${pid_file}")) → http://localhost:${STREAMLIT_PORT}"
  echo "    로그: storage/logs/streamlit.log"
}

# ── ⑤ 서비스 준비 대기 ───────────────────────────────────────────────────────
wait_for_service() {
  local url="$1"
  local label="$2"
  local max_sec="${3:-15}"
  local elapsed=0
  printf "  ⏳ %s 대기 중" "${label}"
  while (( elapsed < max_sec )); do
    if curl -sf --max-time 2 "${url}" >/dev/null 2>&1; then
      echo " → 준비 완료 (${elapsed}s)"
      return 0
    fi
    sleep 2
    elapsed=$(( elapsed + 2 ))
    printf "."
  done
  echo " → 타임아웃 (${max_sec}s, 실험 계속)"
  return 0
}

# ── ⑥ 브라우저 열기 ──────────────────────────────────────────────────────────
open_uis_in_browser() {
  local fastapi_url="http://localhost:${FASTAPI_PORT}/docs"
  local streamlit_url="http://localhost:${STREAMLIT_PORT}"
  local airflow_url="http://localhost:${AIRFLOW_PORT}"

  echo ""
  echo "  ┌──────────────────────────────────────────────────────────────┐"
  echo "  │  UI 주소                                                     │"
  echo "  │  Streamlit : http://localhost:${STREAMLIT_PORT}                        │"
  echo "  │  FastAPI   : http://localhost:${FASTAPI_PORT}/docs                  │"
  echo "  │  Airflow   : http://localhost:${AIRFLOW_PORT}  (admin / admin)   │"
  echo "  └──────────────────────────────────────────────────────────────┘"
  echo ""

  if command -v powershell.exe >/dev/null 2>&1; then
    powershell.exe -NoProfile -Command "Start-Process '${streamlit_url}'" >/dev/null 2>&1 || true
    powershell.exe -NoProfile -Command "Start-Process '${fastapi_url}'" >/dev/null 2>&1 || true
    if curl -sf --max-time 2 "${airflow_url}/health" >/dev/null 2>&1; then
      powershell.exe -NoProfile -Command "Start-Process '${airflow_url}'" >/dev/null 2>&1 || true
      echo "  ✓ Airflow UI 열기 완료"
    else
      echo "  △ Airflow 미실행 — 브라우저 오픈 건너뜀"
    fi
  elif command -v xdg-open >/dev/null 2>&1; then
    xdg-open "${streamlit_url}" >/dev/null 2>&1 &
    xdg-open "${fastapi_url}" >/dev/null 2>&1 &
  fi
}

# ── 실행 계획 출력 ────────────────────────────────────────────────────────────
print_plan() {
  local per_line=$(( TOTAL_BATTERIES / LINE_COUNT ))
  local rem=$(( TOTAL_BATTERIES % LINE_COUNT ))

  echo "============================================================"
  echo "실무 유사 시뮬레이션 계획"
  echo "============================================================"
  echo "  생산라인    : ${LINE_COUNT}대"
  echo "  총 배터리   : ${TOTAL_BATTERIES}개 (라인당 ~${per_line}개)"
  echo "  컨슈머      : 2대 고정 (laser_a 전담 1대 / laser_b 전담 1대)"
  echo "  용접 주기   : ${INTERVAL_SEC}초"
  echo ""
  echo "  스레드 구성:"
  for (( i=1; i<=LINE_COUNT; i++ )); do
    local n=${per_line}
    if (( i <= rem )); then
      n=$(( per_line + 1 ))
    fi
    echo "    LINE_$(printf '%02d' $i)  DataFeeder(${n}개) + FileWatcher"
  done
  echo "    Consumer-1  laser_a 전담"
  echo "    Consumer-2  laser_b 전담"
  echo ""
  echo "  데이터 흐름:"
  echo "    data/20220417 → (DataFeeder, ${INTERVAL_SEC}초 간격)"
  echo "      → new_src/watched/LINE_XX/ → (FileWatcher)"
  echo "        → queue(Kafka 역할) → (Consumer)"
  echo "          → 드리프트 판정 + CSV 저장"
  echo ""
  echo "  Ctrl+C 로 생산라인 종료"
  echo "============================================================"
}

# ── 메인 ─────────────────────────────────────────────────────────────────────
main() {
  parse_args "$@"
  validate

  local py
  py="$(find_python)"

  # ── ① 사전 정리 ──────────────────────────────────────────────────────────
  if [[ "${DO_CLEANUP}" == "1" ]]; then
    cleanup_before_run
  fi

  # ── ② Airflow 기록 초기화 ────────────────────────────────────────────────
  reset_airflow_history
  echo ""

  # ── ③④ UI 서비스 시작 ────────────────────────────────────────────────────
  if [[ "${OPEN_UI}" == "1" ]]; then
    echo "┌─────────────────────────────────────────────────────────────┐"
    echo "│  [UI 서비스 시작]                                           │"
    echo "└─────────────────────────────────────────────────────────────┘"
    start_fastapi "${py}"
    start_streamlit "${py}"
    echo ""

    wait_for_service "http://localhost:${FASTAPI_PORT}/api/v1/health" "FastAPI"
    wait_for_service "http://localhost:${STREAMLIT_PORT}/_stcore/health" "Streamlit"

    # ── ⑥ 브라우저 열기 ──────────────────────────────────────────────────
    open_uis_in_browser
  fi

  # ── ⑤ 실험 실행 ─────────────────────────────────────────────────────────
  print_plan

  cd "${ROOT_DIR}"
  echo "시작..."
  echo ""

  PYTHONUNBUFFERED=1 \
  PYTHONIOENCODING=utf-8 \
  ${py} -u new_src/run_realtime_experiment.py \
    --batteries "${TOTAL_BATTERIES}" \
    --lines     "${LINE_COUNT}" \
    --consumers "${CONSUMER_COUNT}" \
    --interval  "${INTERVAL_SEC}"

  # ── 실험 완료 안내 ───────────────────────────────────────────────────────
  echo ""
  echo "┌─────────────────────────────────────────────────────────────┐"
  echo "│  실험 완료                                                  │"
  echo "└─────────────────────────────────────────────────────────────┘"
  echo "  결과 CSV : storage/metrics/realtime_experiment/"
  local latest_csv
  latest_csv="$(ls -t "${ROOT_DIR}/storage/metrics/realtime_experiment/"*.csv 2>/dev/null | head -1 || true)"
  if [[ -n "${latest_csv}" ]]; then
    echo "  최신 파일 : $(basename "${latest_csv}")"
  fi
  if [[ "${OPEN_UI}" == "1" ]]; then
    echo ""
    echo "  Streamlit에서 결과 확인 → http://localhost:${STREAMLIT_PORT}"
    echo "  (사이드바에서 'Realtime (new_src)' 탭 선택)"
    echo ""
    local fastapi_pid streamlit_pid
    fastapi_pid="$(cat "${ROOT_DIR}/storage/logs/fastapi.pid" 2>/dev/null || echo '?')"
    streamlit_pid="$(cat "${ROOT_DIR}/storage/logs/streamlit.pid" 2>/dev/null || echo '?')"
    echo "  서비스 종료: kill ${fastapi_pid} ${streamlit_pid}"
  fi
  echo ""
}

main "$@"
