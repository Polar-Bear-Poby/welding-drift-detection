#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${ROOT_DIR}/.env}"
ENV_UTILS="${ROOT_DIR}/scripts/lib/env_utils.sh"

if [[ -f "${ENV_UTILS}" ]]; then
  # shellcheck disable=SC1090
  source "${ENV_UTILS}"
fi

load_env_file() {
  if declare -F load_env_file_without_override >/dev/null 2>&1; then
    load_env_file_without_override "$1"
    return
  fi
}

load_env_file "${ENV_FILE}"

HOST_DATA_DIR="${HOST_DATA_DIR:-${ROOT_DIR}/data}"
DATE_FOLDER="${DATE_FOLDER:-20220417}"
DATA_DIR_IN_CONTAINER="${DATA_DIR_IN_CONTAINER:-/data}"
STORAGE_DIR_IN_CONTAINER="${STORAGE_DIR_IN_CONTAINER:-/storage}"
HOST_STORAGE_DIR="${HOST_STORAGE_DIR:-${ROOT_DIR}/storage}"

LINE_COUNT="${LINE_COUNT:-3}"
PRODUCER_COUNT="${PRODUCER_COUNT:-1}"
SPEED="${SPEED:-300}"
MAX_PRODUCTS="${MAX_PRODUCTS:-3}"
TARGET_PRODUCTS="${TARGET_PRODUCTS:-0}"
LINE_SEEDS="${LINE_SEEDS:-}"
REPLAY_SPEED="${REPLAY_SPEED:-${SPEED}}"
CONSUMER_COUNT="${CONSUMER_COUNT:-$((LINE_COUNT * 2))}"
EXPECTED_ROWS="${EXPECTED_ROWS:-$((MAX_PRODUCTS * LINE_COUNT * 2))}"
PAIRED_BATTERY_INDEX_JSON="${PAIRED_BATTERY_INDEX_JSON:-}"
SAMPLE_BATTERY_COUNT="${SAMPLE_BATTERY_COUNT:-0}"
SAMPLE_BATTERY_SEED="${SAMPLE_BATTERY_SEED:-42}"

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
NETWORK_NAME="${NETWORK_NAME:-welding-kafka-submission_welding-net}"
PRODUCER_IMAGE="${PRODUCER_IMAGE:-welding-kafka-submission-producer:latest}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-welding-kafka}"
SPARK_MASTER_CONTAINER="${SPARK_MASTER_CONTAINER:-welding-spark-master}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-welding-postgres}"

POSTGRES_DB="${POSTGRES_DB:-welding_drift}"
POSTGRES_USER="${POSTGRES_USER:-welding}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"

RUN_TS="${RUN_TS:-$(date +%Y%m%d_%H%M%S)}"
RUN_TAG="${RUN_TAG:-nline_even_${RUN_TS}}"
TOPIC_LASER_A="${TOPIC_LASER_A:-welding.raw.laser_a.benchmark.${RUN_TS}}"
TOPIC_LASER_B="${TOPIC_LASER_B:-welding.raw.laser_b.benchmark.${RUN_TS}}"
CHECKPOINT_DIR_BASE="/tmp/spark-checkpoints-${RUN_TAG}"
CONSUMER_LOG_BASE="/tmp/spark_streaming_${RUN_TAG}"
CONSUMER_START_WAIT_SEC="${CONSUMER_START_WAIT_SEC:-60}"
POLL_INTERVAL_SEC="${POLL_INTERVAL_SEC:-5}"
QUIET_WINDOW_SEC="${QUIET_WINDOW_SEC:-20}"
MEASURE_TIMEOUT_SEC="${MEASURE_TIMEOUT_SEC:-900}"
SPARK_STREAMING_CORES_MAX="${SPARK_STREAMING_CORES_MAX:-1}"
SPARK_STREAMING_EXECUTOR_CORES="${SPARK_STREAMING_EXECUTOR_CORES:-1}"
SPARK_STREAMING_EXECUTOR_MEMORY="${SPARK_STREAMING_EXECUTOR_MEMORY:-1g}"
SPARK_TRIGGER_INTERVAL_SEC="${SPARK_TRIGGER_INTERVAL_SEC:-2}"
ALLOW_RUN_ID_FALLBACK_UUID="${ALLOW_RUN_ID_FALLBACK_UUID:-0}"
STRICT_CHANNEL_TOPIC_MATCH="${STRICT_CHANNEL_TOPIC_MATCH:-1}"
LOAD_COMPLETE_GRACE_SEC="${LOAD_COMPLETE_GRACE_SEC:-600}"
MISSING_HEARTBEAT_GRACE_SEC="${MISSING_HEARTBEAT_GRACE_SEC:-1200}"
LOAD_COMPLETE_PARTIAL_SUCCESS_EXPERIMENTAL="${LOAD_COMPLETE_PARTIAL_SUCCESS_EXPERIMENTAL:-1}"
LOAD_COMPLETE_MIN_PARTIAL_RATIO="${LOAD_COMPLETE_MIN_PARTIAL_RATIO:-0.60}"
INFERENCE_DELAY_MS_LASER_A="${INFERENCE_DELAY_MS_LASER_A:-0}"
INFERENCE_DELAY_MS_LASER_B="${INFERENCE_DELAY_MS_LASER_B:-0}"

STOP_ALWAYS_ON_PRODUCER="${STOP_ALWAYS_ON_PRODUCER:-1}"
RESTORE_ALWAYS_ON_PRODUCER="${RESTORE_ALWAYS_ON_PRODUCER:-0}"
RESTORE_DEFAULT_STREAMING="${RESTORE_DEFAULT_STREAMING:-0}"
TOPIC_RAW_LASER_A="${TOPIC_RAW_LASER_A:-welding.raw.laser_a.v1}"
TOPIC_RAW_LASER_B="${TOPIC_RAW_LASER_B:-welding.raw.laser_b.v1}"
DOWN_AFTER_RUN="${DOWN_AFTER_RUN:-0}"
STOP_RUNTIME_AFTER_RUN="${STOP_RUNTIME_AFTER_RUN:-0}"
OPEN_UI="${OPEN_UI:-1}"
OPEN_LOG_TERMINALS="${OPEN_LOG_TERMINALS:-0}"
AUTO_UI_CHECK="${AUTO_UI_CHECK:-1}"
EXPERIMENT_MODE="${EXPERIMENT_MODE:-0}"
UI_CHECK_TIMEOUT_SEC="${UI_CHECK_TIMEOUT_SEC:-60}"
AIRFLOW_UI_URL="${AIRFLOW_UI_URL:-http://localhost:8080}"
KAFKA_UI_URL="${KAFKA_UI_URL:-http://localhost:8089}"
AIRFLOW_SCHEDULER_CONTAINER="${AIRFLOW_SCHEDULER_CONTAINER:-welding-airflow-scheduler}"
AIRFLOW_FAIL_GUARD="${AIRFLOW_FAIL_GUARD:-1}"
AIRFLOW_FAIL_WAIT_SEC="${AIRFLOW_FAIL_WAIT_SEC:-300}"
AIRFLOW_FAIL_GUARD_DAGS="${AIRFLOW_FAIL_GUARD_DAGS:-welding_batch_ingest,welding_batch_backfill}"

METRICS_DIR="${HOST_STORAGE_DIR}/metrics/p1c1"
SUMMARY_TXT="${METRICS_DIR}/p1c1_timing_${RUN_TAG}.txt"
SUMMARY_CSV="${METRICS_DIR}/p1c1_timing_history.csv"

producer_was_running=0

usage() {
  cat <<'EOF'
Usage:
  bash scripts/measure_p1_c1_stream_timing.sh [--experimental] [--down-after-run] [--stop-runtime-after-run] [--open-ui] [--no-open-ui] [--open-log-terminals] [--no-open-log-terminals] [--sample-battery-count N] [--sample-battery-seed N] [--airflow-fail-wait-sec N] [--no-airflow-fail-guard] [--help]

Options:
  --experimental   Allow replay/re-ingest of already processed DATE_FOLDER for experiment runs.
  --down-after-run   Stop Airflow and all docker compose containers after run.
  --stop-runtime-after-run
                    Stop producer/broker/consumer runtime only after run.
  --open-ui         Open Airflow/Kafka UI in browser (default: enabled).
  --no-open-ui      Do not open UI pages.
  --open-log-terminals
                    Open separate terminals for Airflow scheduler and Spark consumer logs.
  --no-open-log-terminals
                    Disable separate log terminals.
  --sample-battery-count N
                    Randomly pick N battery ids that exist in both laser_a and laser_b.
  --sample-battery-seed N
                    Random seed for --sample-battery-count (default: 42).
  --airflow-fail-wait-sec N
                    If guarded Airflow task failure is detected, wait N sec then stop (default: 300).
  --no-airflow-fail-guard
                    Disable guarded failure stop for Airflow monitored DAGs.
  --help             Show this help message.
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --experimental)
        EXPERIMENT_MODE=1
        shift
        ;;
      --down-after-run)
        DOWN_AFTER_RUN=1
        shift
        ;;
      --stop-runtime-after-run)
        STOP_RUNTIME_AFTER_RUN=1
        shift
        ;;
      --open-ui)
        OPEN_UI=1
        shift
        ;;
      --no-open-ui)
        OPEN_UI=0
        shift
        ;;
      --open-log-terminals)
        OPEN_LOG_TERMINALS=1
        shift
        ;;
      --no-open-log-terminals)
        OPEN_LOG_TERMINALS=0
        shift
        ;;
      --sample-battery-count)
        SAMPLE_BATTERY_COUNT="${2:?missing value for --sample-battery-count}"
        shift 2
        ;;
      --sample-battery-seed)
        SAMPLE_BATTERY_SEED="${2:?missing value for --sample-battery-seed}"
        shift 2
        ;;
      --airflow-fail-wait-sec)
        AIRFLOW_FAIL_WAIT_SEC="${2:?missing value for --airflow-fail-wait-sec}"
        shift 2
        ;;
      --no-airflow-fail-guard)
        AIRFLOW_FAIL_GUARD=0
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "ERROR: unknown argument: $1" >&2
        usage
        exit 1
        ;;
    esac
  done
}

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

wait_http_ready() {
  local url="$1"
  local timeout_sec="$2"
  local start_epoch
  start_epoch="$(date +%s)"
  while true; do
    if command -v curl >/dev/null 2>&1; then
      if curl -fsS -o /dev/null "${url}" 2>/dev/null; then
        return 0
      fi
    fi
    local now_epoch
    now_epoch="$(date +%s)"
    if (( now_epoch - start_epoch >= timeout_sec )); then
      return 1
    fi
    sleep 2
  done
}

open_in_browser() {
  local url="$1"
  if command -v powershell.exe >/dev/null 2>&1; then
    if powershell.exe -NoProfile -Command "Start-Process '${url}'" >/dev/null 2>&1; then
      return 0
    fi
  fi
  if command -v cmd.exe >/dev/null 2>&1; then
    if cmd.exe /c start "" "${url}" >/dev/null 2>&1; then
      return 0
    fi
  fi
  if command -v explorer.exe >/dev/null 2>&1; then
    if explorer.exe "${url}" >/dev/null 2>&1; then
      return 0
    fi
  fi
  if command -v xdg-open >/dev/null 2>&1; then
    if xdg-open "${url}" >/dev/null 2>&1; then
      return 0
    fi
  fi
  log "WARN: could not auto-open browser for ${url}. open manually if needed."
  return 1
}

open_and_check_ui() {
  if [[ "${OPEN_UI}" != "1" ]]; then
    return
  fi
  log "Opening UI pages: ${AIRFLOW_UI_URL}, ${KAFKA_UI_URL}"
  open_in_browser "${AIRFLOW_UI_URL}"
  open_in_browser "${KAFKA_UI_URL}"
  if [[ "${AUTO_UI_CHECK}" == "1" ]]; then
    if wait_http_ready "${AIRFLOW_UI_URL}" "${UI_CHECK_TIMEOUT_SEC}"; then
      log "UI ready: Airflow (${AIRFLOW_UI_URL})"
    else
      log "WARN: Airflow UI not reachable within ${UI_CHECK_TIMEOUT_SEC}s (${AIRFLOW_UI_URL})"
    fi
    if wait_http_ready "${KAFKA_UI_URL}" "${UI_CHECK_TIMEOUT_SEC}"; then
      log "UI ready: Kafka UI (${KAFKA_UI_URL})"
    else
      log "WARN: Kafka UI not reachable within ${UI_CHECK_TIMEOUT_SEC}s (${KAFKA_UI_URL})"
    fi
  fi
}

open_log_terminal() {
  local title="$1"
  local command="$2"
  local command_ps="${command//\'/''}"

  if command -v powershell.exe >/dev/null 2>&1; then
    if powershell.exe -NoProfile -Command "Start-Process powershell -WindowStyle Normal -ArgumentList '-NoExit','-Command','${command_ps}'" >/dev/null 2>&1; then
      return 0
    fi
  fi
  if command -v cmd.exe >/dev/null 2>&1; then
    if cmd.exe /c start "${title}" cmd.exe /k "${command}" >/dev/null 2>&1; then
      return 0
    fi
  fi
  log "WARN: could not open log terminal for ${title}"
  return 1
}

open_log_terminals_if_enabled() {
  if [[ "${OPEN_LOG_TERMINALS}" != "1" ]]; then
    return
  fi

  log "Opening separate log terminals (airflow scheduler + spark consumers)."
  open_log_terminal "airflow-scheduler" "docker logs -f ${AIRFLOW_SCHEDULER_CONTAINER}"
  for consumer_id in $(seq 1 "${CONSUMER_COUNT}"); do
    local log_path="${CONSUMER_LOG_BASE}_consumer_${consumer_id}.log"
    local cmd="docker exec ${SPARK_MASTER_CONTAINER} bash -lc \"tail -n 0 -F '${log_path}'\""
    open_log_terminal "spark-consumer-${consumer_id}" "${cmd}"
  done
}

start_producer_tailer() {
  local log_file="$1"
  local label="$2"
  # Stream per-producer file logs into this script stdout so outer out.log collects merged progress.
  ( tail -n 0 -F "${log_file}" 2>/dev/null | sed -u "s/^/[${label}] /" ) &
}

stop_producer_tailers() {
  local tailer_pid
  for tailer_pid in "$@"; do
    if [[ -n "${tailer_pid}" ]] && kill -0 "${tailer_pid}" 2>/dev/null; then
      kill "${tailer_pid}" 2>/dev/null || true
      wait "${tailer_pid}" 2>/dev/null || true
    fi
  done
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: required command not found: $1" >&2
    exit 1
  fi
}

container_running() {
  local name="$1"
  docker ps --format '{{.Names}}' | grep -qx "${name}"
}

ensure_container_running() {
  local name="$1"
  if ! container_running "${name}"; then
    echo "ERROR: required container is not running: ${name}" >&2
    exit 1
  fi
}

pg_agg_for_topics_since() {
  local topic_laser_a="$1"
  local topic_laser_b="$2"
  local start_iso="$3"
  docker exec -i "${POSTGRES_CONTAINER}" psql \
    -U "${POSTGRES_USER}" \
    -d "${POSTGRES_DB}" \
    -t -A -F ',' \
    -v ON_ERROR_STOP=1 \
    -c "SELECT
          COUNT(*),
          COALESCE(to_char(MIN(processed_at AT TIME ZONE 'UTC'),'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),''),
          COALESCE(to_char(MAX(processed_at AT TIME ZONE 'UTC'),'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),'')
        FROM welding.pattern_summary
        WHERE source_file IN ('kafka://${topic_laser_a}', 'kafka://${topic_laser_b}')
          AND processed_at >= '${start_iso}'::timestamptz;"
}

airflow_guard_dag_in_clause() {
  local dags_raw="${AIRFLOW_FAIL_GUARD_DAGS// /}"
  local dag
  local out=()
  IFS=',' read -r -a dags <<< "${dags_raw}"
  for dag in "${dags[@]}"; do
    if [[ -n "${dag}" ]]; then
      out+=("'${dag}'")
    fi
  done
  if (( ${#out[@]} == 0 )); then
    echo "'welding_batch_ingest'"
    return
  fi
  local IFS=','
  echo "${out[*]}"
}

airflow_failure_count_since() {
  local start_iso="$1"
  local dag_in_clause="$2"
  docker exec -i "${POSTGRES_CONTAINER}" psql \
    -U "${POSTGRES_USER}" \
    -d "${POSTGRES_DB}" \
    -t -A \
    -v ON_ERROR_STOP=1 \
    -c "SELECT COUNT(*)
        FROM welding.task_instance ti
        JOIN welding.dag_run dr
          ON dr.dag_id = ti.dag_id
         AND dr.run_id = ti.run_id
        WHERE ti.state IN ('failed', 'upstream_failed')
          AND dr.start_date >= '${start_iso}'::timestamptz
          AND ti.dag_id IN (${dag_in_clause});"
}

airflow_failure_details_since() {
  local start_iso="$1"
  local dag_in_clause="$2"
  docker exec -i "${POSTGRES_CONTAINER}" psql \
    -U "${POSTGRES_USER}" \
    -d "${POSTGRES_DB}" \
    -t -A -F '|' \
    -v ON_ERROR_STOP=1 \
    -c "SELECT ti.dag_id,
               ti.run_id,
               ti.task_id,
               ti.state,
               COALESCE(to_char(ti.start_date AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),''),
               COALESCE(to_char(ti.end_date   AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),'')
        FROM welding.task_instance ti
        JOIN welding.dag_run dr
          ON dr.dag_id = ti.dag_id
         AND dr.run_id = ti.run_id
        WHERE ti.state IN ('failed', 'upstream_failed')
          AND dr.start_date >= '${start_iso}'::timestamptz
          AND ti.dag_id IN (${dag_in_clause})
        ORDER BY COALESCE(ti.end_date, ti.start_date, dr.start_date) DESC
        LIMIT 30;"
}

check_airflow_failure_guard_or_stop() {
  local start_iso="$1"
  local dag_in_clause="$2"
  if [[ "${AIRFLOW_FAIL_GUARD}" != "1" ]]; then
    return 0
  fi
  local fail_count
  fail_count="$(airflow_failure_count_since "${start_iso}" "${dag_in_clause}" | tr -d '\r' | head -n 1)"
  fail_count="${fail_count//[[:space:]]/}"
  if [[ -z "${fail_count}" ]]; then
    return 0
  fi
  if (( fail_count > 0 )); then
    log "Airflow failure guard triggered (failed/upstream_failed=${fail_count}). waiting ${AIRFLOW_FAIL_WAIT_SEC}s before stop."
    sleep "${AIRFLOW_FAIL_WAIT_SEC}"
    log "Airflow failure details (dag|run_id|task_id|state|start_utc|end_utc):"
    airflow_failure_details_since "${start_iso}" "${dag_in_clause}" | sed 's/^/[airflow-fail] /'
    return 1
  fi
  return 0
}

stop_all_streaming_consumers() {
  docker exec "${SPARK_MASTER_CONTAINER}" bash -lc \
    "pids=\$(pgrep -f 'spark_streaming.py' 2>/dev/null | grep -vw \"\$\$\" || true);
     if [ -n \"\$pids\" ]; then
       kill -TERM \$pids || true;
       sleep 5;
       pids2=\$(pgrep -f 'spark_streaming.py' 2>/dev/null | grep -vw \"\$\$\" || true);
       if [ -n \"\$pids2\" ]; then
         kill -KILL \$pids2 || true;
       fi;
     fi" >/dev/null 2>&1 || true
}

start_streaming_consumer() {
  local topic_name="$1"
  local checkpoint_dir="$2"
  local log_path="$3"
  local channel_filter="$4"
  local kafka_group_id="$5"
  local consumer_id="${6:-0}"
  local consumer_ivy="/tmp/.ivy2-consumer-${consumer_id}"

  docker exec "${SPARK_MASTER_CONTAINER}" bash -lc \
    "mkdir -p '${consumer_ivy}';
     rm -rf '${checkpoint_dir}';
     nohup env TOPIC_RAW='${topic_name}' SPARK_CHECKPOINT_DIR='${checkpoint_dir}' CHANNEL_FILTER='${channel_filter}' KAFKA_GROUP_ID='${kafka_group_id}' SPARK_TRIGGER_INTERVAL_SEC='${SPARK_TRIGGER_INTERVAL_SEC}' ALLOW_RUN_ID_FALLBACK_UUID='${ALLOW_RUN_ID_FALLBACK_UUID}' STRICT_CHANNEL_TOPIC_MATCH='${STRICT_CHANNEL_TOPIC_MATCH}' LOAD_COMPLETE_GRACE_SEC='${LOAD_COMPLETE_GRACE_SEC}' MISSING_HEARTBEAT_GRACE_SEC='${MISSING_HEARTBEAT_GRACE_SEC}' LOAD_COMPLETE_PARTIAL_SUCCESS_EXPERIMENTAL='${LOAD_COMPLETE_PARTIAL_SUCCESS_EXPERIMENTAL}' LOAD_COMPLETE_MIN_PARTIAL_RATIO='${LOAD_COMPLETE_MIN_PARTIAL_RATIO}' POSTGRES_PASSWORD='${POSTGRES_PASSWORD}' \
       INFERENCE_DELAY_MS_LASER_A='${INFERENCE_DELAY_MS_LASER_A}' INFERENCE_DELAY_MS_LASER_B='${INFERENCE_DELAY_MS_LASER_B}' \
       /opt/spark/bin/spark-submit \
       --master spark://spark-master:7077 \
       --conf spark.cores.max=${SPARK_STREAMING_CORES_MAX} \
       --conf spark.executor.cores=${SPARK_STREAMING_EXECUTOR_CORES} \
       --conf spark.executor.memory=${SPARK_STREAMING_EXECUTOR_MEMORY} \
       --conf spark.jars.ivy='${consumer_ivy}' \
       --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
       /opt/spark/apps/spark_streaming.py \
       >'${log_path}' 2>&1 &
     echo \$!"
}

wait_consumer_ready() {
  local topic_name="$1"
  local log_path="$2"
  local timeout_sec="$3"
  local waited=0
  while (( waited < timeout_sec )); do
    if docker exec "${SPARK_MASTER_CONTAINER}" bash -lc \
      "grep -q 'Subscribing to Kafka topic: ${topic_name}' '${log_path}'"; then
      return 0
    fi
    sleep 2
    waited=$((waited + 2))
  done
  return 1
}

restore_default_streaming_if_needed() {
  if [[ "${RESTORE_DEFAULT_STREAMING}" != "1" ]]; then
    return
  fi
  stop_all_streaming_consumers
  docker exec "${SPARK_MASTER_CONTAINER}" bash -lc \
    "for consumer_id in \$(seq 1 ${CONSUMER_COUNT}); do
        consumer_ivy=\"/tmp/.ivy2-consumer-\${consumer_id}\";
        mkdir -p \"\${consumer_ivy}\";
        if [ \$((consumer_id % 2)) -eq 1 ]; then
          topic='${TOPIC_RAW_LASER_A}';
          channel='laser_a';
          group_id='welding-stream-laser-a';
       else
         topic='${TOPIC_RAW_LASER_B}';
         channel='laser_b';
         group_id='welding-stream-laser-b';
       fi;
        rm -rf \"/tmp/spark-checkpoints-consumer-\${consumer_id}\";
        nohup env TOPIC_RAW=\"\${topic}\" CHANNEL_FILTER=\"\${channel}\" KAFKA_GROUP_ID=\"\${group_id}\" SPARK_CHECKPOINT_DIR=\"/tmp/spark-checkpoints-consumer-\${consumer_id}\" SPARK_TRIGGER_INTERVAL_SEC='${SPARK_TRIGGER_INTERVAL_SEC}' ALLOW_RUN_ID_FALLBACK_UUID='${ALLOW_RUN_ID_FALLBACK_UUID}' STRICT_CHANNEL_TOPIC_MATCH='${STRICT_CHANNEL_TOPIC_MATCH}' LOAD_COMPLETE_GRACE_SEC='${LOAD_COMPLETE_GRACE_SEC}' MISSING_HEARTBEAT_GRACE_SEC='${MISSING_HEARTBEAT_GRACE_SEC}' LOAD_COMPLETE_PARTIAL_SUCCESS_EXPERIMENTAL='${LOAD_COMPLETE_PARTIAL_SUCCESS_EXPERIMENTAL}' LOAD_COMPLETE_MIN_PARTIAL_RATIO='${LOAD_COMPLETE_MIN_PARTIAL_RATIO}' POSTGRES_PASSWORD='${POSTGRES_PASSWORD}' \
          INFERENCE_DELAY_MS_LASER_A='${INFERENCE_DELAY_MS_LASER_A}' INFERENCE_DELAY_MS_LASER_B='${INFERENCE_DELAY_MS_LASER_B}' \
          /opt/spark/bin/spark-submit \
         --master spark://spark-master:7077 \
         --conf spark.cores.max=${SPARK_STREAMING_CORES_MAX} \
         --conf spark.executor.cores=${SPARK_STREAMING_EXECUTOR_CORES} \
         --conf spark.executor.memory=${SPARK_STREAMING_EXECUTOR_MEMORY} \
         --conf spark.jars.ivy=\"\${consumer_ivy}\" \
         --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
         /opt/spark/apps/spark_streaming.py \
         >/tmp/spark_streaming_consumer_\${consumer_id}.log 2>&1 &
      done;" >/dev/null 2>&1 || true
}

cleanup() {
  local exit_code="${1:-0}"
  stop_all_streaming_consumers
  if [[ "${STOP_RUNTIME_AFTER_RUN}" == "1" ]]; then
    log "STOP_RUNTIME_AFTER_RUN=1 -> stopping producer/broker/consumer runtime containers."
    (cd "${ROOT_DIR}" && bash scripts/stop_experiment_runtime.sh >/dev/null 2>&1) || true
  fi
  if [[ "${DOWN_AFTER_RUN}" == "1" ]]; then
    log "DOWN_AFTER_RUN=1 -> stopping Airflow and all project containers."
    (cd "${ROOT_DIR}" && docker compose down --remove-orphans >/dev/null 2>&1) || true
    return
  fi
  if [[ "${producer_was_running}" == "1" && "${RESTORE_ALWAYS_ON_PRODUCER}" == "1" ]]; then
    docker start welding-producer >/dev/null 2>&1 || true
  fi
  restore_default_streaming_if_needed
  return "${exit_code}"
}

parse_args "$@"
trap 'cleanup $?' EXIT

require_cmd docker
ensure_container_running "${KAFKA_CONTAINER}"
ensure_container_running "${SPARK_MASTER_CONTAINER}"
ensure_container_running "${POSTGRES_CONTAINER}"
open_and_check_ui

if declare -F ensure_postgres_password >/dev/null 2>&1; then
  ensure_postgres_password "${ENV_FILE}" "${POSTGRES_CONTAINER}" "welding_local_auto_pw"
fi
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"
if [[ -z "${POSTGRES_PASSWORD}" ]]; then
  echo "ERROR: POSTGRES_PASSWORD could not be resolved." >&2
  exit 1
fi

if [[ "${EXPERIMENT_MODE}" == "1" ]]; then
  target_date_iso="${DATE_FOLDER:0:4}-${DATE_FOLDER:4:2}-${DATE_FOLDER:6:2}"
  log "EXPERIMENT_MODE=1 -> clear replay-completed heartbeat for ${target_date_iso}"
  docker exec -i "${POSTGRES_CONTAINER}" psql \
    -U "${POSTGRES_USER}" \
    -d "${POSTGRES_DB}" \
    -v ON_ERROR_STOP=1 \
    -c "DELETE FROM welding.pipeline_heartbeat
        WHERE component_name='airflow.welding_batch_ingest'
          AND details->>'status'='REPLAY_COMPLETED'
          AND details->>'target_date'='${target_date_iso}';" >/dev/null
fi

if (( CONSUMER_COUNT < 2 || CONSUMER_COUNT % 2 != 0 )); then
  echo "ERROR: CONSUMER_COUNT must be an even number >= 2 (current=${CONSUMER_COUNT})" >&2
  exit 1
fi
if (( PRODUCER_COUNT < 1 )); then
  echo "ERROR: PRODUCER_COUNT must be >= 1 (current=${PRODUCER_COUNT})" >&2
  exit 1
fi
if (( PRODUCER_COUNT > 1 )) && (( LINE_COUNT != PRODUCER_COUNT )); then
  log "LINE_COUNT(${LINE_COUNT}) adjusted to PRODUCER_COUNT(${PRODUCER_COUNT}) for 1 producer = 1 line mode."
  LINE_COUNT="${PRODUCER_COUNT}"
fi

if [[ ! -d "${HOST_DATA_DIR}/${DATE_FOLDER}" ]]; then
  if [[ -d "${ROOT_DIR}/data/${DATE_FOLDER}" ]]; then
    HOST_DATA_DIR="${ROOT_DIR}/data"
  elif [[ -d "/mnt/d/metacode_battery_drfit/welding-kafka-submission/data/${DATE_FOLDER}" ]]; then
    HOST_DATA_DIR="/mnt/d/metacode_battery_drfit/welding-kafka-submission/data"
  elif [[ -d "/d/metacode_battery_drfit/welding-kafka-submission/data/${DATE_FOLDER}" ]]; then
    HOST_DATA_DIR="/d/metacode_battery_drfit/welding-kafka-submission/data"
  elif [[ -d "/mnt/d/metacode_battery_drfit/data_runtime_by_channel/${DATE_FOLDER}" ]]; then
    HOST_DATA_DIR="/mnt/d/metacode_battery_drfit/data_runtime_by_channel"
  elif [[ -d "/d/metacode_battery_drfit/data_runtime_by_channel/${DATE_FOLDER}" ]]; then
    HOST_DATA_DIR="/d/metacode_battery_drfit/data_runtime_by_channel"
  fi
fi

if [[ ! -d "${HOST_DATA_DIR}/${DATE_FOLDER}" ]]; then
  echo "ERROR: source date folder does not exist: ${HOST_DATA_DIR}/${DATE_FOLDER}" >&2
  exit 1
fi

if [[ -z "${PAIRED_BATTERY_INDEX_JSON}" ]]; then
  PAIRED_BATTERY_INDEX_JSON="${DATA_DIR_IN_CONTAINER}/${DATE_FOLDER}/paired_batteries_index.json"
fi

if (( SAMPLE_BATTERY_COUNT > 0 )); then
  host_index_json="${HOST_DATA_DIR}/${DATE_FOLDER}/paired_batteries_index.json"
  if [[ ! -f "${host_index_json}" ]]; then
    log "Building paired battery index: ${host_index_json}"
    if command -v uv >/dev/null 2>&1; then
      uv run python "${ROOT_DIR}/scripts/build_paired_battery_index.py" \
        --data-root "${HOST_DATA_DIR}" \
        --date-folder "${DATE_FOLDER}" \
        --output "${host_index_json}"
    else
      python "${ROOT_DIR}/scripts/build_paired_battery_index.py" \
        --data-root "${HOST_DATA_DIR}" \
        --date-folder "${DATE_FOLDER}" \
        --output "${host_index_json}"
    fi
  fi
fi

mkdir -p "${METRICS_DIR}"
if [[ ! -f "${SUMMARY_CSV}" ]]; then
  cat > "${SUMMARY_CSV}" <<EOF
run_tag,topic_laser_a,topic_laser_b,date_folder,line_count,consumer_count,max_products,target_products,line_seeds,expected_rows,producer_start_utc,producer_end_utc,producer_duration_sec,db_first_utc,db_last_utc,time_to_first_db_sec,end_to_last_db_sec,db_drain_after_producer_sec,db_rows
EOF
fi

log "Preparing benchmark topics: ${TOPIC_LASER_A}, ${TOPIC_LASER_B}"
docker exec "${KAFKA_CONTAINER}" kafka-topics \
  --bootstrap-server kafka:9092 \
  --create \
  --if-not-exists \
  --topic "${TOPIC_LASER_A}" \
  --partitions 8 \
  --replication-factor 1 >/dev/null
docker exec "${KAFKA_CONTAINER}" kafka-topics \
  --bootstrap-server kafka:9092 \
  --create \
  --if-not-exists \
  --topic "${TOPIC_LASER_B}" \
  --partitions 8 \
  --replication-factor 1 >/dev/null

if [[ "${STOP_ALWAYS_ON_PRODUCER}" == "1" ]] && container_running "welding-producer"; then
  producer_was_running=1
  log "Stopping always-on producer container for clean benchmark window."
  docker stop welding-producer >/dev/null
fi

log "Ensuring benchmark consumers are running (consumer_count=${CONSUMER_COUNT}, odd=laser_a, even=laser_b)."
stop_all_streaming_consumers
for consumer_id in $(seq 1 "${CONSUMER_COUNT}"); do
  checkpoint="${CHECKPOINT_DIR_BASE}_consumer_${consumer_id}"
  log_path="${CONSUMER_LOG_BASE}_consumer_${consumer_id}.log"

  if (( consumer_id % 2 == 1 )); then
    topic="${TOPIC_LASER_A}"
    channel_filter="laser_a"
    group_id="welding-stream-laser-a"
  else
    topic="${TOPIC_LASER_B}"
    channel_filter="laser_b"
    group_id="welding-stream-laser-b"
  fi

  start_streaming_consumer "${topic}" "${checkpoint}" "${log_path}" "${channel_filter}" "${group_id}" "${consumer_id}" >/dev/null

  if ! wait_consumer_ready "${topic}" "${log_path}" "${CONSUMER_START_WAIT_SEC}"; then
    echo "ERROR: benchmark consumer id=${consumer_id} topic=${topic} did not become ready in ${CONSUMER_START_WAIT_SEC}s" >&2
    docker exec "${SPARK_MASTER_CONTAINER}" bash -lc "tail -n 80 '${log_path}'" || true
    exit 1
  fi
done
log "Benchmark consumers started. topic_laser_a=${TOPIC_LASER_A}, topic_laser_b=${TOPIC_LASER_B}"
open_log_terminals_if_enabled

producer_start_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
producer_start_epoch="$(date +%s)"
guard_dag_in_clause="$(airflow_guard_dag_in_clause)"
log "Airflow guard enabled=${AIRFLOW_FAIL_GUARD} dags=${AIRFLOW_FAIL_GUARD_DAGS} wait_sec=${AIRFLOW_FAIL_WAIT_SEC}"
log "Producer run start: producer_count=${PRODUCER_COUNT} line_count=${LINE_COUNT} max_products=${MAX_PRODUCTS} target_products=${TARGET_PRODUCTS} speed=${REPLAY_SPEED} line_seeds=${LINE_SEEDS:-none}"

producer_args=(
  --data-dir "${DATA_DIR_IN_CONTAINER}/${DATE_FOLDER}"
  --kafka "${KAFKA_BOOTSTRAP}"
  --topic-laser-a "${TOPIC_LASER_A}"
  --topic-laser-b "${TOPIC_LASER_B}"
  --line-count "${LINE_COUNT}"
  --speed "${REPLAY_SPEED}"
  --no-schedule-wait
)
if [[ "${MAX_PRODUCTS}" != "0" ]]; then
  producer_args+=(--max-products "${MAX_PRODUCTS}")
fi
if [[ "${TARGET_PRODUCTS}" != "0" ]]; then
  producer_args+=(--target-products "${TARGET_PRODUCTS}")
fi
if [[ -n "${LINE_SEEDS}" ]]; then
  producer_args+=(--line-seed "${LINE_SEEDS}")
fi
if (( SAMPLE_BATTERY_COUNT > 0 )); then
  producer_args+=(
    --paired-battery-index-json "${PAIRED_BATTERY_INDEX_JSON}"
    --sample-battery-count "${SAMPLE_BATTERY_COUNT}"
    --sample-battery-seed "${SAMPLE_BATTERY_SEED}"
  )
fi

if (( PRODUCER_COUNT == 1 )); then
  docker run --rm \
    --network "${NETWORK_NAME}" \
    -v "${HOST_DATA_DIR}:${DATA_DIR_IN_CONTAINER}:ro" \
    -v "${HOST_STORAGE_DIR}:${STORAGE_DIR_IN_CONTAINER}" \
    -v "${ROOT_DIR}/producer.py:/app/producer.py:ro" \
    "${PRODUCER_IMAGE}" \
    "${producer_args[@]}" 2>&1 | sed -u 's/^/[producer-line-01] /' &
  single_producer_pid="$!"
  while kill -0 "${single_producer_pid}" 2>/dev/null; do
    if ! check_airflow_failure_guard_or_stop "${producer_start_iso}" "${guard_dag_in_clause}"; then
      log "Stopping single producer due to Airflow failure guard."
      kill "${single_producer_pid}" 2>/dev/null || true
      wait "${single_producer_pid}" 2>/dev/null || true
      exit 1
    fi
    sleep "${POLL_INTERVAL_SEC}"
  done
  wait "${single_producer_pid}"
else
  if (( TARGET_PRODUCTS <= 0 )); then
    echo "ERROR: TARGET_PRODUCTS must be > 0 when PRODUCER_COUNT > 1" >&2
    exit 1
  fi

  producer_pids=()
  producer_logs=()
  producer_tailer_pids=()
  base=$((TARGET_PRODUCTS / PRODUCER_COUNT))
  rem=$((TARGET_PRODUCTS % PRODUCER_COUNT))

  for producer_idx in $(seq 1 "${PRODUCER_COUNT}"); do
    shard_index=$((producer_idx - 1))
    target_for_this="${base}"
    if (( shard_index < rem )); then
      target_for_this=$((target_for_this + 1))
    fi
    if (( target_for_this <= 0 )); then
      continue
    fi

    producer_log="/tmp/producer_${RUN_TAG}_line_${producer_idx}.log"
    producer_logs+=("${producer_log}")
    log "Launching producer line=${producer_idx} shard=${shard_index}/${PRODUCER_COUNT} target_products=${target_for_this}"

    producer_cmd=(
      docker run --rm
      --network "${NETWORK_NAME}"
      -v "${HOST_DATA_DIR}:${DATA_DIR_IN_CONTAINER}:ro"
      -v "${HOST_STORAGE_DIR}:${STORAGE_DIR_IN_CONTAINER}"
      -v "${ROOT_DIR}/producer.py:/app/producer.py:ro"
      "${PRODUCER_IMAGE}"
      "${producer_args[@]}"
      --line-count 1
      --line-number "${producer_idx}"
      --shard-index "${shard_index}"
      --shard-total "${PRODUCER_COUNT}"
      --target-products "${target_for_this}"
    )

    "${producer_cmd[@]}" >"${producer_log}" 2>&1 &
    producer_pids+=("$!")
    start_producer_tailer "${producer_log}" "producer-line-${producer_idx}"
    producer_tailer_pids+=("$!")
  done

  failed=0
  while true; do
    running=0
    for pid in "${producer_pids[@]}"; do
      if kill -0 "${pid}" 2>/dev/null; then
        running=1
        break
      fi
    done
    if (( running == 0 )); then
      break
    fi
    if ! check_airflow_failure_guard_or_stop "${producer_start_iso}" "${guard_dag_in_clause}"; then
      log "Stopping all producer shards due to Airflow failure guard."
      for pid in "${producer_pids[@]}"; do
        kill "${pid}" 2>/dev/null || true
      done
      sleep 2
      for pid in "${producer_pids[@]}"; do
        kill -9 "${pid}" 2>/dev/null || true
      done
      stop_producer_tailers "${producer_tailer_pids[@]}"
      exit 1
    fi
    sleep "${POLL_INTERVAL_SEC}"
  done
  for pid in "${producer_pids[@]}"; do
    if ! wait "${pid}"; then
      failed=1
    fi
  done
  stop_producer_tailers "${producer_tailer_pids[@]}"
  if (( failed == 1 )); then
    for plog in "${producer_logs[@]}"; do
      if [[ -f "${plog}" ]]; then
        log "Producer log tail: ${plog}"
        tail -n 80 "${plog}" || true
      fi
    done
    echo "ERROR: one or more producer containers failed." >&2
    exit 1
  fi
fi

producer_end_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
producer_end_epoch="$(date +%s)"
producer_duration_sec="$((producer_end_epoch - producer_start_epoch))"
log "Producer run done: duration=${producer_duration_sec}s"

log "Polling DB write completion for topics=${TOPIC_LASER_A},${TOPIC_LASER_B} (expected_rows=${EXPECTED_ROWS})"
poll_start_epoch="$(date +%s)"
last_count=-1
stable_since_epoch="$(date +%s)"
db_count=0
db_first_iso=""
db_last_iso=""

while true; do
  if ! check_airflow_failure_guard_or_stop "${producer_start_iso}" "${guard_dag_in_clause}"; then
    log "Stopping DB polling due to Airflow failure guard."
    exit 1
  fi
  row="$(pg_agg_for_topics_since "${TOPIC_LASER_A}" "${TOPIC_LASER_B}" "${producer_start_iso}" | tr -d '\r' | head -n 1)"
  db_count="$(echo "${row}" | cut -d',' -f1)"
  db_first_iso="$(echo "${row}" | cut -d',' -f2)"
  db_last_iso="$(echo "${row}" | cut -d',' -f3)"

  now_epoch="$(date +%s)"
  if [[ "${db_count}" != "${last_count}" ]]; then
    stable_since_epoch="${now_epoch}"
    last_count="${db_count}"
  fi

  if [[ "${db_count}" -ge "${EXPECTED_ROWS}" ]] && (( now_epoch - stable_since_epoch >= QUIET_WINDOW_SEC )); then
    break
  fi
  if (( now_epoch - poll_start_epoch >= MEASURE_TIMEOUT_SEC )); then
    log "Polling timeout (${MEASURE_TIMEOUT_SEC}s). using current DB state."
    break
  fi
  sleep "${POLL_INTERVAL_SEC}"
done

time_to_first_db_sec=""
end_to_last_db_sec=""
db_drain_after_producer_sec=""

if [[ -n "${db_first_iso}" ]]; then
  db_first_epoch="$(date -u -d "${db_first_iso}" +%s)"
  time_to_first_db_sec="$((db_first_epoch - producer_start_epoch))"
fi
if [[ -n "${db_last_iso}" ]]; then
  db_last_epoch="$(date -u -d "${db_last_iso}" +%s)"
  end_to_last_db_sec="$((db_last_epoch - producer_start_epoch))"
  db_drain_after_producer_sec="$((db_last_epoch - producer_end_epoch))"
fi

cat > "${SUMMARY_TXT}" <<EOF
run_tag=${RUN_TAG}
topic_laser_a=${TOPIC_LASER_A}
topic_laser_b=${TOPIC_LASER_B}
date_folder=${DATE_FOLDER}
line_count=${LINE_COUNT}
producer_count=${PRODUCER_COUNT}
consumer_count=${CONSUMER_COUNT}
inference_delay_ms_laser_a=${INFERENCE_DELAY_MS_LASER_A}
inference_delay_ms_laser_b=${INFERENCE_DELAY_MS_LASER_B}
max_products=${MAX_PRODUCTS}
target_products=${TARGET_PRODUCTS}
line_seeds=${LINE_SEEDS}
expected_rows=${EXPECTED_ROWS}
producer_start_utc=${producer_start_iso}
producer_end_utc=${producer_end_iso}
producer_duration_sec=${producer_duration_sec}
db_first_utc=${db_first_iso}
db_last_utc=${db_last_iso}
time_to_first_db_sec=${time_to_first_db_sec}
end_to_last_db_sec=${end_to_last_db_sec}
db_drain_after_producer_sec=${db_drain_after_producer_sec}
db_rows=${db_count}
consumer_log_prefix=${CONSUMER_LOG_BASE}
EOF

printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
  "${RUN_TAG}" \
  "${TOPIC_LASER_A}" \
  "${TOPIC_LASER_B}" \
  "${DATE_FOLDER}" \
  "${LINE_COUNT}" \
  "${CONSUMER_COUNT}" \
  "${MAX_PRODUCTS}" \
  "${TARGET_PRODUCTS}" \
  "${LINE_SEEDS}" \
  "${EXPECTED_ROWS}" \
  "${producer_start_iso}" \
  "${producer_end_iso}" \
  "${producer_duration_sec}" \
  "${db_first_iso}" \
  "${db_last_iso}" \
  "${time_to_first_db_sec}" \
  "${end_to_last_db_sec}" \
  "${db_drain_after_producer_sec}" \
  "${db_count}" >> "${SUMMARY_CSV}"

log "Benchmark summary saved: ${SUMMARY_TXT}"
log "History updated: ${SUMMARY_CSV}"
log "Result: producer_duration=${producer_duration_sec}s db_rows=${db_count}/${EXPECTED_ROWS} time_to_first_db_sec=${time_to_first_db_sec} end_to_last_db_sec=${end_to_last_db_sec} drain_after_producer_sec=${db_drain_after_producer_sec}"
