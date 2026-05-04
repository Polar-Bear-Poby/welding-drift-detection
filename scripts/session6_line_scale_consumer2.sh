#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
METRICS_DIR="${ROOT_DIR}/storage/metrics/session6"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_ID="line_scale_c2_q100_${RUN_TS}"
SUMMARY_CSV="${METRICS_DIR}/${RUN_ID}.csv"
GRAPH_MD="${METRICS_DIR}/${RUN_ID}_graph.md"
ORDERS_JSON="${METRICS_DIR}/${RUN_ID}_line_orders.json"
GRAPH_MD_PY="${GRAPH_MD}"
ORDERS_JSON_PY="${ORDERS_JSON}"
SUMMARY_CSV_PY="${SUMMARY_CSV}"

DATE_FOLDER="${DATE_FOLDER:-20220417}"
CONSUMER_COUNT=2
TOTAL_BATTERIES=100
LINE_SET="${LINE_SET:-3,4,6,8,10}"

# Input dataset candidates
SOURCE_DATE_DIR="${SOURCE_DATE_DIR:-}"
HOST_DATA_ROOT="${HOST_DATA_ROOT:-}"
OUTPUT_DATE_DIR=""

BASELINE_SPEED="${BASELINE_SPEED:-220}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"
if [[ -z "${POSTGRES_PASSWORD}" && -f "${ROOT_DIR}/.env" ]]; then
  # shellcheck disable=SC1090
  source <(tr -d '\r' < "${ROOT_DIR}/.env")
fi
if [[ -z "${POSTGRES_PASSWORD}" ]] && command -v docker >/dev/null 2>&1; then
  POSTGRES_PASSWORD="$(
    docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' welding-postgres 2>/dev/null \
      | awk -F= '$1=="POSTGRES_PASSWORD"{print $2; exit}'
  )"
fi
if [[ -z "${POSTGRES_PASSWORD:-}" ]]; then
  echo "ERROR: POSTGRES_PASSWORD must be set (env/.env/container env)." >&2
  exit 1
fi

if [[ -z "${HOST_DATA_ROOT}" ]]; then
  if [[ -d "/mnt/d/metacode_battery_drfit" ]]; then
    HOST_DATA_ROOT="/mnt/d/metacode_battery_drfit/data_runtime_session6"
  elif [[ -d "/d/metacode_battery_drfit" ]]; then
    HOST_DATA_ROOT="/d/metacode_battery_drfit/data_runtime_session6"
  else
    echo "ERROR: cannot determine host data root mount (/mnt/d or /d)." >&2
    exit 1
  fi
fi
OUTPUT_DATE_DIR="${HOST_DATA_ROOT}/${DATE_FOLDER}"

if [[ -x "${ROOT_DIR}/.venv/Scripts/python.exe" ]]; then
  PY_RUNNER="${ROOT_DIR}/.venv/Scripts/python.exe"
elif command -v uv >/dev/null 2>&1; then
  PY_RUNNER="uv run python"
elif command -v python >/dev/null 2>&1; then
  PY_RUNNER="python"
elif command -v python3 >/dev/null 2>&1; then
  PY_RUNNER="python3"
else
  echo "ERROR: python runtime not found (uv/python/python3)." >&2
  exit 1
fi

run_py() {
  if [[ "${PY_RUNNER}" == "uv run python" ]]; then
    uv run python "$@"
  else
    "${PY_RUNNER}" "$@"
  fi
}

to_windows_path() {
  local p="$1"
  if command -v cygpath >/dev/null 2>&1; then
    cygpath -w "$p"
    return
  fi
  if [[ "$p" =~ ^/mnt/([a-zA-Z])/(.*)$ ]]; then
    local drive="${BASH_REMATCH[1]}"
    local rest="${BASH_REMATCH[2]}"
    rest="${rest//\//\\}"
    printf '%s:\\%s\n' "$(echo "$drive" | tr '[:lower:]' '[:upper:]')" "$rest"
    return
  fi
  if [[ "$p" =~ ^/([a-zA-Z])/(.*)$ ]]; then
    local drive="${BASH_REMATCH[1]}"
    local rest="${BASH_REMATCH[2]}"
    rest="${rest//\//\\}"
    printf '%s:\\%s\n' "$(echo "$drive" | tr '[:lower:]' '[:upper:]')" "$rest"
    return
  fi
  printf '%s\n' "$p"
}

if [[ -z "${SOURCE_DATE_DIR}" ]]; then
  if [[ -d "/d/metacode_battery_drfit/data_runtime_by_channel/${DATE_FOLDER}" ]]; then
    SOURCE_DATE_DIR="/d/metacode_battery_drfit/data_runtime_by_channel/${DATE_FOLDER}"
  elif [[ -d "/d/metacode_battery_drfit/data_runtime_flat/${DATE_FOLDER}" ]]; then
    SOURCE_DATE_DIR="/d/metacode_battery_drfit/data_runtime_flat/${DATE_FOLDER}"
  elif [[ -d "/d/metacode_battery_drfit/data/${DATE_FOLDER}" ]]; then
    SOURCE_DATE_DIR="/d/metacode_battery_drfit/data/${DATE_FOLDER}"
  elif [[ -d "/mnt/d/metacode_battery_drfit/data_runtime_by_channel/${DATE_FOLDER}" ]]; then
    SOURCE_DATE_DIR="/mnt/d/metacode_battery_drfit/data_runtime_by_channel/${DATE_FOLDER}"
  elif [[ -d "/mnt/d/metacode_battery_drfit/data_runtime_flat/${DATE_FOLDER}" ]]; then
    SOURCE_DATE_DIR="/mnt/d/metacode_battery_drfit/data_runtime_flat/${DATE_FOLDER}"
  elif [[ -d "/mnt/d/metacode_battery_drfit/data/${DATE_FOLDER}" ]]; then
    SOURCE_DATE_DIR="/mnt/d/metacode_battery_drfit/data/${DATE_FOLDER}"
  else
    echo "ERROR: source date dir not found. set SOURCE_DATE_DIR explicitly." >&2
    exit 1
  fi
fi

mkdir -p "${METRICS_DIR}"
mkdir -p "${HOST_DATA_ROOT}"

echo "[1/4] Prepare top-100 paired dataset ..."
SRC_FOR_PREP="${SOURCE_DATE_DIR}"
OUT_FOR_PREP="${OUTPUT_DATE_DIR}"
if [[ "${PY_RUNNER}" == *.exe ]]; then
  SRC_FOR_PREP="$(to_windows_path "${SOURCE_DATE_DIR}")"
  OUT_FOR_PREP="$(to_windows_path "${OUTPUT_DATE_DIR}")"
  ORDERS_JSON_PY="$(to_windows_path "${ORDERS_JSON}")"
  GRAPH_MD_PY="$(to_windows_path "${GRAPH_MD}")"
  SUMMARY_CSV_PY="$(to_windows_path "${SUMMARY_CSV}")"
fi
(
  cd "${ROOT_DIR}"
  run_py scripts/prepare_session6_top100.py \
    --source-date-dir "${SRC_FOR_PREP}" \
    --output-date-dir "${OUT_FOR_PREP}" \
    --limit "${TOTAL_BATTERIES}"
)

echo "[2/4] Generate per-line random order arrays (0..99) ..."
run_py - <<PY
import json, random
line_set = [int(x.strip()) for x in "${LINE_SET}".split(",") if x.strip()]
orders = {}
for lc in line_set:
    for line_no in range(1, lc + 1):
        rng = random.Random(1000 + lc * 100 + line_no)
        seq = list(range(${TOTAL_BATTERIES}))
        rng.shuffle(seq)
        orders[f"L{lc:02d}_LINE_{line_no:02d}"] = seq
with open(r"${ORDERS_JSON_PY}", "w", encoding="utf-8") as f:
    json.dump(orders, f, ensure_ascii=False, indent=2)
print("orders_file=${ORDERS_JSON_PY}")
PY

echo "scenario,line_count,consumer_count,total_batteries,replay_speed,line_seeds,producer_duration_sec,time_to_first_db_sec,end_to_last_db_sec,db_drain_after_producer_sec,db_rows,expected_rows,result_txt" > "${SUMMARY_CSV}"

echo "[3/4] Run experiments ..."
IFS=',' read -r -a LINES <<< "${LINE_SET}"
for lc in "${LINES[@]}"; do
  lc="$(echo "${lc}" | xargs)"
  [[ -n "${lc}" ]] || continue

  # producer line shuffling seeds (one seed per line)
  line_seeds=""
  for ln in $(seq 1 "${lc}"); do
    seed=$((1000 + lc * 100 + ln))
    if [[ -z "${line_seeds}" ]]; then
      line_seeds="${seed}"
    else
      line_seeds="${line_seeds},${seed}"
    fi
  done

  run_tag="${RUN_ID}_l${lc}"
  expected_rows=$((TOTAL_BATTERIES * 2))
  echo "  - line_count=${lc}, seeds=${line_seeds}"

  (
    cd "${ROOT_DIR}"
    HOST_DATA_DIR="${HOST_DATA_ROOT}" \
    DATE_FOLDER="${DATE_FOLDER}" \
    LINE_COUNT="${lc}" \
    CONSUMER_COUNT="${CONSUMER_COUNT}" \
    MAX_PRODUCTS="${TOTAL_BATTERIES}" \
    TARGET_PRODUCTS="${TOTAL_BATTERIES}" \
    EXPECTED_ROWS="${expected_rows}" \
    REPLAY_SPEED="${BASELINE_SPEED}" \
    LINE_SEEDS="${line_seeds}" \
    RUN_TAG="${run_tag}" \
    POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
    PRODUCER_IMAGE="${PRODUCER_IMAGE:-welding-kafka-submission-producer:latest}" \
    bash scripts/measure_p1_c1_stream_timing.sh
  )

  result_txt="${ROOT_DIR}/storage/metrics/p1c1/p1c1_timing_${run_tag}.txt"
  producer_duration="$(grep '^producer_duration_sec=' "${result_txt}" | cut -d= -f2)"
  first_db="$(grep '^time_to_first_db_sec=' "${result_txt}" | cut -d= -f2)"
  end_to_last="$(grep '^end_to_last_db_sec=' "${result_txt}" | cut -d= -f2)"
  drain="$(grep '^db_drain_after_producer_sec=' "${result_txt}" | cut -d= -f2)"
  db_rows="$(grep '^db_rows=' "${result_txt}" | cut -d= -f2)"
  printf 'baseline,%s,%s,%s,%s,"%s",%s,%s,%s,%s,%s,%s,%s\n' \
    "${lc}" "${CONSUMER_COUNT}" "${TOTAL_BATTERIES}" "${BASELINE_SPEED}" "${line_seeds}" \
    "${producer_duration}" "${first_db}" "${end_to_last}" "${drain}" "${db_rows}" "${expected_rows}" "${result_txt}" >> "${SUMMARY_CSV}"
done

echo "[4/4] Build graph markdown ..."
run_py - <<PY
import csv
rows = []
with open(r"${SUMMARY_CSV_PY}", newline="", encoding="utf-8") as f:
    for r in csv.DictReader(f):
        try:
            rows.append((int(r["line_count"]), float(r["end_to_last_db_sec"] or 0)))
        except ValueError:
            pass
rows.sort(key=lambda x: x[0])
x = ", ".join(str(a) for a, _ in rows)
y = ", ".join(str(round(b, 2)) for _, b in rows)
with open(r"${GRAPH_MD_PY}", "w", encoding="utf-8") as out:
    out.write("# Line Scale (Consumer 2 fixed)\\n\\n")
    out.write("```mermaid\\n")
    out.write("xychart-beta\\n")
    out.write("    title \"E2E Time by Line Count (Q=100, C=2)\"\\n")
    out.write("    x-axis [")
    out.write(x)
    out.write("]\\n")
    out.write("    y-axis \"end_to_last_db_sec\" 0 --> ")
    out.write(str(int(max((b for _, b in rows), default=1) * 1.2) + 1))
    out.write("\\n")
    out.write("    line [")
    out.write(y)
    out.write("]\\n")
    out.write("```\\n")
print("graph_md=${GRAPH_MD_PY}")
PY

echo "Done."
echo "Summary CSV: ${SUMMARY_CSV}"
echo "Line order JSON: ${ORDERS_JSON}"
echo "Graph MD: ${GRAPH_MD}"
