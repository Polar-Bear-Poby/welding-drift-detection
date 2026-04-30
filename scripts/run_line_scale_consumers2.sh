#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
METRICS_DIR="${ROOT_DIR}/storage/metrics/p1c1"
SUMMARY_CSV="${METRICS_DIR}/line_scale_c2_summary.csv"

TOTAL_BATTERIES="${TOTAL_BATTERIES:-120}"
LINE_SET="${LINE_SET:-2,4,6,8,10}"
CONSUMER_COUNT="${CONSUMER_COUNT:-2}"
REPLAY_SPEED="${REPLAY_SPEED:-300}"
DATE_FOLDER="${DATE_FOLDER:-20220417}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"

if [[ -z "${POSTGRES_PASSWORD}" ]]; then
  echo "ERROR: POSTGRES_PASSWORD must be set" >&2
  exit 1
fi

mkdir -p "${METRICS_DIR}"
if [[ ! -f "${SUMMARY_CSV}" ]]; then
  cat > "${SUMMARY_CSV}" <<'EOF'
run_tag,line_count,total_batteries,max_products_per_line,consumer_count,expected_rows,producer_duration_sec,time_to_first_db_sec,end_to_last_db_sec,db_drain_after_producer_sec,db_rows,result_file
EOF
fi

run_one() {
  local line_count="$1"
  if (( TOTAL_BATTERIES % line_count != 0 )); then
    echo "SKIP: line_count=${line_count} does not divide TOTAL_BATTERIES=${TOTAL_BATTERIES}"
    return 0
  fi

  local max_products=$((TOTAL_BATTERIES / line_count))
  local expected_rows=$((TOTAL_BATTERIES * 2))
  local run_tag="scale_l${line_count}_b${TOTAL_BATTERIES}_c${CONSUMER_COUNT}_$(date +%Y%m%d_%H%M%S)"

  echo "[RUN] line=${line_count} total_batteries=${TOTAL_BATTERIES} max_products=${max_products} consumers=${CONSUMER_COUNT}"
  (
    cd "${ROOT_DIR}"
    LINE_COUNT="${line_count}" \
    MAX_PRODUCTS="${max_products}" \
    EXPECTED_ROWS="${expected_rows}" \
    CONSUMER_COUNT="${CONSUMER_COUNT}" \
    REPLAY_SPEED="${REPLAY_SPEED}" \
    DATE_FOLDER="${DATE_FOLDER}" \
    RUN_TAG="${run_tag}" \
    POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
    bash scripts/measure_p1_c1_stream_timing.sh
  )

  local txt="${METRICS_DIR}/p1c1_timing_${run_tag}.txt"
  if [[ ! -f "${txt}" ]]; then
    echo "ERROR: missing result file: ${txt}" >&2
    exit 1
  fi

  local producer_duration_sec
  local time_to_first_db_sec
  local end_to_last_db_sec
  local db_drain_after_producer_sec
  local db_rows
  producer_duration_sec="$(grep '^producer_duration_sec=' "${txt}" | cut -d= -f2)"
  time_to_first_db_sec="$(grep '^time_to_first_db_sec=' "${txt}" | cut -d= -f2)"
  end_to_last_db_sec="$(grep '^end_to_last_db_sec=' "${txt}" | cut -d= -f2)"
  db_drain_after_producer_sec="$(grep '^db_drain_after_producer_sec=' "${txt}" | cut -d= -f2)"
  db_rows="$(grep '^db_rows=' "${txt}" | cut -d= -f2)"

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "${run_tag}" \
    "${line_count}" \
    "${TOTAL_BATTERIES}" \
    "${max_products}" \
    "${CONSUMER_COUNT}" \
    "${expected_rows}" \
    "${producer_duration_sec}" \
    "${time_to_first_db_sec}" \
    "${end_to_last_db_sec}" \
    "${db_drain_after_producer_sec}" \
    "${db_rows}" \
    "${txt}" >> "${SUMMARY_CSV}"

  echo "[DONE] line=${line_count} db_rows=${db_rows}/${expected_rows} producer=${producer_duration_sec}s e2e=${end_to_last_db_sec}s"
}

IFS=',' read -r -a lines <<< "${LINE_SET}"
for line_count in "${lines[@]}"; do
  line_count="$(echo "${line_count}" | xargs)"
  [[ -n "${line_count}" ]] || continue
  run_one "${line_count}"
done

echo "Completed. Summary: ${SUMMARY_CSV}"
