#!/usr/bin/env bash
set -euo pipefail

# Session 6 load test runner
# Runs baseline/peak/burst scenarios with fixed daily quota Q.
#
# Usage:
#   bash scripts/session6_run_load_tests.sh --q 120 --line-count 3 --consumer-count 2
#
# Optional:
#   --date-folder 20220417
#   --baseline-speed 120
#   --peak-speed 220
#   --burst-speed 320
#   --host-data-dir /mnt/d/metacode_battery_drfit/data_runtime_flat
#   --down-after-run

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
METRICS_DIR="${ROOT_DIR}/storage/metrics/session6"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
REPORT_TXT="${METRICS_DIR}/load_report_${TIMESTAMP}.txt"
REPORT_CSV="${METRICS_DIR}/load_report_${TIMESTAMP}.csv"

Q=120
LINE_COUNT=3
CONSUMER_COUNT=2
DATE_FOLDER="20220417"
BASELINE_SPEED=120
PEAK_SPEED=220
BURST_SPEED=320
INFER_DELAY_A_MS=0
INFER_DELAY_B_MS=0
HOST_DATA_DIR_DEFAULT=""
HOST_DATA_DIR="${HOST_DATA_DIR:-}"
DOWN_AFTER_RUN=0

usage() {
  cat <<'EOF'
Usage:
  bash scripts/session6_run_load_tests.sh [options]

Options:
  --q <int>                 Total daily battery quota (Q). Default: 120
  --line-count <int>        Number of production lines. Default: 3
  --consumer-count <int>    Number of streaming consumers. Default: 2
  --date-folder <yyyymmdd>  Date folder. Default: 20220417
  --baseline-speed <int>    Replay speed for baseline. Default: 120
  --peak-speed <int>        Replay speed for peak. Default: 220
  --burst-speed <int>       Replay speed for burst. Default: 320
  --infer-delay-a-ms <num>  Simulated inference delay for laser_a. Default: 0
  --infer-delay-b-ms <num>  Simulated inference delay for laser_b. Default: 0
  --host-data-dir <path>    Host data root mounted to /data. Default: /mnt/d/metacode_battery_drfit/data_runtime_flat
  --down-after-run          Stop docker compose stack after test.
  -h, --help                Show help.
EOF
}

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: command not found: $1" >&2
    exit 1
  fi
}

container_running() {
  docker ps --format '{{.Names}}' | grep -qx "$1"
}

load_env_if_exists() {
  local env_file="${ROOT_DIR}/.env"
  if [[ -f "${env_file}" ]]; then
    # Handle CRLF safely.
    set -a
    # shellcheck disable=SC1090
    source <(tr -d '\r' < "${env_file}")
    set +a
  fi
}

detect_default_data_dir() {
  local candidates=(
    "/mnt/d/metacode_battery_drfit/data_runtime_flat"
    "/d/metacode_battery_drfit/data_runtime_flat"
  )
  local path
  for path in "${candidates[@]}"; do
    if [[ -d "${path}" ]]; then
      echo "${path}"
      return 0
    fi
  done
  return 1
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --q) Q="$2"; shift 2 ;;
      --line-count) LINE_COUNT="$2"; shift 2 ;;
      --consumer-count) CONSUMER_COUNT="$2"; shift 2 ;;
      --date-folder) DATE_FOLDER="$2"; shift 2 ;;
      --baseline-speed) BASELINE_SPEED="$2"; shift 2 ;;
      --peak-speed) PEAK_SPEED="$2"; shift 2 ;;
      --burst-speed) BURST_SPEED="$2"; shift 2 ;;
      --infer-delay-a-ms) INFER_DELAY_A_MS="$2"; shift 2 ;;
      --infer-delay-b-ms) INFER_DELAY_B_MS="$2"; shift 2 ;;
      --host-data-dir) HOST_DATA_DIR="$2"; shift 2 ;;
      --down-after-run) DOWN_AFTER_RUN=1; shift ;;
      -h|--help) usage; exit 0 ;;
      *)
        echo "ERROR: unknown option: $1" >&2
        usage
        exit 1
        ;;
    esac
  done
}

run_one() {
  local scenario="$1"
  local speed="$2"
  local run_tag="session6_${scenario}_q${Q}_l${LINE_COUNT}_c${CONSUMER_COUNT}_${TIMESTAMP}"
  local max_products=$((Q / LINE_COUNT))
  local expected_rows=$((Q * 2))

  log "Scenario=${scenario}, speed=${speed}, max_products_per_line=${max_products}, expected_rows=${expected_rows}"
  (
    cd "${ROOT_DIR}"
    HOST_DATA_DIR="${HOST_DATA_DIR}" \
    DATE_FOLDER="${DATE_FOLDER}" \
    LINE_COUNT="${LINE_COUNT}" \
    CONSUMER_COUNT="${CONSUMER_COUNT}" \
    MAX_PRODUCTS="${max_products}" \
    EXPECTED_ROWS="${expected_rows}" \
    REPLAY_SPEED="${speed}" \
    INFERENCE_DELAY_MS_LASER_A="${INFER_DELAY_A_MS}" \
    INFERENCE_DELAY_MS_LASER_B="${INFER_DELAY_B_MS}" \
    RUN_TAG="${run_tag}" \
    bash scripts/measure_p1_c1_stream_timing.sh
  )

  local result_txt="${ROOT_DIR}/storage/metrics/p1c1/p1c1_timing_${run_tag}.txt"
  if [[ ! -f "${result_txt}" ]]; then
    echo "ERROR: result file not found: ${result_txt}" >&2
    exit 1
  fi

  local producer_duration
  local first_db
  local last_db
  local drain
  local db_rows
  producer_duration="$(grep '^producer_duration_sec=' "${result_txt}" | cut -d= -f2)"
  first_db="$(grep '^time_to_first_db_sec=' "${result_txt}" | cut -d= -f2)"
  last_db="$(grep '^end_to_last_db_sec=' "${result_txt}" | cut -d= -f2)"
  drain="$(grep '^db_drain_after_producer_sec=' "${result_txt}" | cut -d= -f2)"
  db_rows="$(grep '^db_rows=' "${result_txt}" | cut -d= -f2)"

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "${scenario}" "${Q}" "${LINE_COUNT}" "${CONSUMER_COUNT}" "${speed}" \
    "${producer_duration}" "${first_db}" "${last_db}" "${drain}" "${db_rows}" >> "${REPORT_CSV}"
}

parse_args "$@"
require_cmd docker
require_cmd bash
load_env_if_exists

if [[ -z "${HOST_DATA_DIR}" ]]; then
  if detected="$(detect_default_data_dir)"; then
    HOST_DATA_DIR="${detected}"
  else
    HOST_DATA_DIR_DEFAULT="/mnt/d/metacode_battery_drfit/data_runtime_flat"
    HOST_DATA_DIR="${HOST_DATA_DIR_DEFAULT}"
  fi
fi

mkdir -p "${METRICS_DIR}"
printf 'scenario,q,line_count,consumer_count,replay_speed,producer_duration_sec,time_to_first_db_sec,end_to_last_db_sec,db_drain_after_producer_sec,db_rows\n' > "${REPORT_CSV}"

if (( Q <= 0 || LINE_COUNT <= 0 || CONSUMER_COUNT <= 0 )); then
  echo "ERROR: q/line-count/consumer-count must be positive." >&2
  exit 1
fi
if (( Q % LINE_COUNT != 0 )); then
  echo "ERROR: Q must be divisible by line-count. Q=${Q}, line-count=${LINE_COUNT}" >&2
  echo "Hint: choose Q as a multiple of line-count (example: 120 with line-count 3)." >&2
  exit 1
fi
if (( CONSUMER_COUNT % 2 != 0 )); then
  echo "ERROR: consumer-count must be even (odd=laser_a, even=laser_b)." >&2
  exit 1
fi
if [[ ! -d "${HOST_DATA_DIR}/${DATE_FOLDER}" ]]; then
  echo "ERROR: source folder not found: ${HOST_DATA_DIR}/${DATE_FOLDER}" >&2
  echo "Hint: pass --host-data-dir explicitly for your shell environment." >&2
  exit 1
fi

if [[ -z "${POSTGRES_PASSWORD:-}" ]]; then
  echo "ERROR: POSTGRES_PASSWORD is not set. Configure it in .env or export it before running." >&2
  exit 1
fi

for required in welding-kafka welding-postgres welding-spark-master; do
  if ! container_running "${required}"; then
    echo "ERROR: required container not running: ${required}" >&2
    echo "Run: bash scripts/start_always_on_pipeline.sh" >&2
    exit 1
  fi
done

cat > "${REPORT_TXT}" <<EOF
session6_load_test_report
timestamp=${TIMESTAMP}
q=${Q}
line_count=${LINE_COUNT}
consumer_count=${CONSUMER_COUNT}
date_folder=${DATE_FOLDER}
host_data_dir=${HOST_DATA_DIR}
baseline_speed=${BASELINE_SPEED}
peak_speed=${PEAK_SPEED}
burst_speed=${BURST_SPEED}
infer_delay_a_ms=${INFER_DELAY_A_MS}
infer_delay_b_ms=${INFER_DELAY_B_MS}
report_csv=${REPORT_CSV}
EOF

run_one "baseline" "${BASELINE_SPEED}"
run_one "peak" "${PEAK_SPEED}"
run_one "burst" "${BURST_SPEED}"

log "Load test completed."
log "Report TXT: ${REPORT_TXT}"
log "Report CSV: ${REPORT_CSV}"

if (( DOWN_AFTER_RUN == 1 )); then
  log "down-after-run enabled -> docker compose down"
  (cd "${ROOT_DIR}" && docker compose down --remove-orphans) || true
fi
