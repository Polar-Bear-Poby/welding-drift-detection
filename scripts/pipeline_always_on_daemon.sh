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
HOST_STORAGE_DIR="${HOST_STORAGE_DIR:-${ROOT_DIR}/storage}"
DATA_DIR_IN_CONTAINER="${DATA_DIR_IN_CONTAINER:-/data}"
STORAGE_DIR_IN_CONTAINER="${STORAGE_DIR_IN_CONTAINER:-/storage}"

LINE_COUNT="${LINE_COUNT:-3}"
REPLAY_SPEED="${REPLAY_SPEED:-100}"
POLL_INTERVAL_SEC="${POLL_INTERVAL_SEC:-30}"
DELETE_PARQUET_AFTER_BATCH="${DELETE_PARQUET_AFTER_BATCH:-1}"
EXPERIMENT_MODE="${EXPERIMENT_MODE:-0}"
EXPERIMENT_RESET_STATE="${EXPERIMENT_RESET_STATE:-0}"
EXPERIMENT_DATE="${EXPERIMENT_DATE:-}"

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
NETWORK_NAME="${NETWORK_NAME:-welding-kafka-submission_welding-net}"
PRODUCER_IMAGE="${PRODUCER_IMAGE:-welding-producer:latest}"
TOPIC_RAW_LASER_A="${TOPIC_RAW_LASER_A:-welding.raw.laser_a.v1}"
TOPIC_RAW_LASER_B="${TOPIC_RAW_LASER_B:-welding.raw.laser_b.v1}"
SPARK_MASTER_CONTAINER="${SPARK_MASTER_CONTAINER:-welding-spark-master}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-welding-postgres}"

POSTGRES_DB="${POSTGRES_DB:-welding_drift}"
POSTGRES_USER="${POSTGRES_USER:-welding}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"

DAG_IDS="${DAG_IDS:-welding_batch_ingest,welding_daily_quality_report,welding_streaming_monitor}"
DAG_LOOKBACK_DAYS="${DAG_LOOKBACK_DAYS:-30}"

METRICS_DIR="${HOST_STORAGE_DIR}/metrics"
STATE_DIR="${METRICS_DIR}/always_on"
STATE_FILE="${STATE_DIR}/processed_dates.txt"
RUN_HISTORY_CSV="${STATE_DIR}/processing_history.csv"
DAEMON_LOG="${STATE_DIR}/daemon.log"

mkdir -p "${HOST_STORAGE_DIR}" "${METRICS_DIR}" "${STATE_DIR}"
touch "${STATE_FILE}"

if [[ ! -f "${RUN_HISTORY_CSV}" ]]; then
  cat > "${RUN_HISTORY_CSV}" <<EOF
run_tag,date_folder,status,producer_start_utc,producer_end_utc,producer_duration_sec,spark_start_utc,spark_end_utc,spark_duration_sec,total_duration_sec,deleted_parquet_files,spark_output_host
EOF
fi

log() {
  local msg="$*"
  printf '[%s] %s\n' "$(date '+%F %T')" "${msg}" | tee -a "${DAEMON_LOG}"
}

if [[ "${EXPERIMENT_RESET_STATE}" == "1" ]]; then
  if [[ -n "${EXPERIMENT_DATE}" ]]; then
    tmp_file="${STATE_FILE}.tmp.$$"
    grep -vx "${EXPERIMENT_DATE}" "${STATE_FILE}" > "${tmp_file}" || true
    mv "${tmp_file}" "${STATE_FILE}"
    log "EXPERIMENT_MODE reset: removed processed date entry ${EXPERIMENT_DATE}"
  else
    : > "${STATE_FILE}"
    log "EXPERIMENT_MODE reset: cleared all processed date entries"
  fi
fi

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: required command not found: $1" >&2
    exit 1
  fi
}

container_running() {
  local container_name="$1"
  docker ps --format '{{.Names}}' | grep -qx "${container_name}"
}

runtime_ready() {
  local missing=0
  if ! docker network inspect "${NETWORK_NAME}" >/dev/null 2>&1; then
    log "Runtime not ready: missing docker network ${NETWORK_NAME}"
    missing=1
  fi
  if ! container_running "welding-kafka"; then
    log "Runtime not ready: container not running -> welding-kafka"
    missing=1
  fi
  if ! container_running "${SPARK_MASTER_CONTAINER}"; then
    log "Runtime not ready: container not running -> ${SPARK_MASTER_CONTAINER}"
    missing=1
  fi
  if ! container_running "${POSTGRES_CONTAINER}"; then
    log "Runtime not ready: container not running -> ${POSTGRES_CONTAINER}"
    missing=1
  fi
  [[ "${missing}" -eq 0 ]]
}

require_cmd docker

if declare -F ensure_postgres_password >/dev/null 2>&1; then
  ensure_postgres_password "${ENV_FILE}" "${POSTGRES_CONTAINER}" "welding_local_auto_pw"
fi
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"
if [[ -z "${POSTGRES_PASSWORD}" ]]; then
  echo "ERROR: POSTGRES_PASSWORD could not be resolved." >&2
  exit 1
fi

csv_to_sql_in_list() {
  local csv="$1"
  local out=""
  local item
  IFS=',' read -r -a items <<< "${csv}"
  for item in "${items[@]}"; do
    item="$(echo "${item}" | xargs)"
    if [[ -n "${item}" ]]; then
      if [[ -n "${out}" ]]; then
        out+=","
      fi
      out+="'${item}'"
    fi
  done
  echo "${out}"
}

collect_dag_metrics_snapshot() {
  local tag="$1"
  local dag_id_sql
  dag_id_sql="$(csv_to_sql_in_list "${DAG_IDS}")"
  local dag_run_csv="${METRICS_DIR}/dag_run_metrics_${tag}.csv"
  local task_csv="${METRICS_DIR}/dag_task_metrics_${tag}.csv"

  docker exec -i "${POSTGRES_CONTAINER}" psql \
    -U "${POSTGRES_USER}" \
    -d "${POSTGRES_DB}" \
    -v ON_ERROR_STOP=1 <<SQL > "${dag_run_csv}"
COPY (
  SELECT
    dr.dag_id,
    dr.run_id,
    dr.state,
    dr.logical_date,
    dr.start_date AS dag_start,
    dr.end_date AS dag_end,
    MIN(ti.start_date) AS first_task_start,
    MAX(ti.end_date) AS last_task_end,
    ROUND(EXTRACT(EPOCH FROM (COALESCE(dr.end_date, NOW()) - dr.start_date))::numeric, 3) AS dag_duration_sec,
    ROUND(EXTRACT(EPOCH FROM (COALESCE(MAX(ti.end_date), NOW()) - MIN(ti.start_date)))::numeric, 3) AS task_span_sec,
    COUNT(ti.task_id) AS task_instance_count
  FROM dag_run dr
  LEFT JOIN task_instance ti
    ON ti.dag_id = dr.dag_id
   AND ti.run_id = dr.run_id
  WHERE dr.dag_id IN (${dag_id_sql})
    AND dr.start_date >= NOW() - INTERVAL '${DAG_LOOKBACK_DAYS} days'
  GROUP BY dr.dag_id, dr.run_id, dr.state, dr.logical_date, dr.start_date, dr.end_date
  ORDER BY dr.start_date DESC
) TO STDOUT WITH CSV HEADER;
SQL

  docker exec -i "${POSTGRES_CONTAINER}" psql \
    -U "${POSTGRES_USER}" \
    -d "${POSTGRES_DB}" \
    -v ON_ERROR_STOP=1 <<SQL > "${task_csv}"
COPY (
  SELECT
    ti.dag_id,
    ti.run_id,
    ti.task_id,
    ti.state,
    ti.try_number,
    ti.start_date,
    ti.end_date,
    ROUND(EXTRACT(EPOCH FROM (COALESCE(ti.end_date, NOW()) - ti.start_date))::numeric, 3) AS task_duration_sec
  FROM task_instance ti
  WHERE ti.dag_id IN (${dag_id_sql})
    AND ti.start_date IS NOT NULL
    AND ti.start_date >= NOW() - INTERVAL '${DAG_LOOKBACK_DAYS} days'
  ORDER BY ti.start_date DESC
) TO STDOUT WITH CSV HEADER;
SQL
}

run_one_date_folder() {
  local date_folder="$1"
  local run_ts run_tag
  run_ts="$(date +%Y%m%d_%H%M%S)"
  run_tag="${date_folder}_${run_ts}"

  local spark_output_rel="spark_batch_${run_tag}"
  local spark_output_host="${HOST_STORAGE_DIR}/${spark_output_rel}"
  local spark_output_container="${STORAGE_DIR_IN_CONTAINER}/${spark_output_rel}"

  local run_start_epoch run_end_epoch total_duration_sec
  local producer_start_epoch producer_end_epoch producer_duration_sec
  local spark_start_epoch spark_end_epoch spark_duration_sec
  local producer_start_iso producer_end_iso spark_start_iso spark_end_iso
  local deleted_count status

  status="SUCCESS"
  deleted_count=0
  run_start_epoch="$(date +%s)"

  producer_start_epoch="$(date +%s)"
  producer_start_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  log "Process date=${date_folder}: producer replay start (line_count=${LINE_COUNT})"
  if ! docker run --rm \
      --network "${NETWORK_NAME}" \
      -v "${HOST_DATA_DIR}:${DATA_DIR_IN_CONTAINER}:ro" \
      -v "${HOST_STORAGE_DIR}:${STORAGE_DIR_IN_CONTAINER}" \
      -v "${ROOT_DIR}/producer.py:/app/producer.py:ro" \
      "${PRODUCER_IMAGE}" \
      --data-dir "${DATA_DIR_IN_CONTAINER}/${date_folder}" \
      --kafka "${KAFKA_BOOTSTRAP}" \
      --topic-laser-a "${TOPIC_RAW_LASER_A}" \
      --topic-laser-b "${TOPIC_RAW_LASER_B}" \
      --line-count "${LINE_COUNT}" \
      --speed "${REPLAY_SPEED}" \
      --no-schedule-wait; then
    status="FAIL_PRODUCER"
  fi
  producer_end_epoch="$(date +%s)"
  producer_end_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  producer_duration_sec="$((producer_end_epoch - producer_start_epoch))"

  spark_start_epoch="$(date +%s)"
  spark_start_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if [[ "${status}" == "SUCCESS" ]]; then
    log "Process date=${date_folder}: spark batch start"
    if ! docker exec "${SPARK_MASTER_CONTAINER}" /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/apps/spark_batch.py \
        --input-dir "${DATA_DIR_IN_CONTAINER}/${date_folder}" \
        --output-dir "${spark_output_container}" \
        --write-postgres \
        --postgres-host postgres \
        --postgres-port 5432 \
        --postgres-db "${POSTGRES_DB}" \
        --postgres-user "${POSTGRES_USER}" \
        --postgres-password "${POSTGRES_PASSWORD}"; then
      status="FAIL_SPARK_BATCH"
    fi
  fi
  spark_end_epoch="$(date +%s)"
  spark_end_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  spark_duration_sec="$((spark_end_epoch - spark_start_epoch))"

  if [[ "${DELETE_PARQUET_AFTER_BATCH}" == "1" ]]; then
    case "${spark_output_host}" in
      "${HOST_STORAGE_DIR}"/*) ;;
      *)
        log "Unsafe delete path detected, skip delete: ${spark_output_host}"
        ;;
    esac
    if [[ -d "${spark_output_host}" ]]; then
      deleted_count="$(find "${spark_output_host}" -type f \( -name "*.parquet" -o -name "*.snappy.parquet" -o -name "_SUCCESS" -o -name "_started_*" -o -name "_committed_*" \) | wc -l | tr -d ' ')"
      if [[ "${deleted_count}" != "0" ]]; then
        find "${spark_output_host}" -type f \( -name "*.parquet" -o -name "*.snappy.parquet" -o -name "_SUCCESS" -o -name "_started_*" -o -name "_committed_*" \) -delete
      fi
      find "${spark_output_host}" -type d -empty -delete || true
    fi
  fi

  # Capture DAG/task timing snapshot for process analysis.
  if ! collect_dag_metrics_snapshot "${run_tag}"; then
    log "Warning: failed to collect DAG metrics snapshot for ${run_tag}"
  fi

  run_end_epoch="$(date +%s)"
  total_duration_sec="$((run_end_epoch - run_start_epoch))"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "${run_tag}" \
    "${date_folder}" \
    "${status}" \
    "${producer_start_iso}" \
    "${producer_end_iso}" \
    "${producer_duration_sec}" \
    "${spark_start_iso}" \
    "${spark_end_iso}" \
    "${spark_duration_sec}" \
    "${total_duration_sec}" \
    "${deleted_count}" \
    "${spark_output_host}" >> "${RUN_HISTORY_CSV}"

  if [[ "${status}" == "SUCCESS" ]]; then
    echo "${date_folder}" >> "${STATE_FILE}"
    log "Process date=${date_folder}: done (total=${total_duration_sec}s, deleted=${deleted_count})"
  else
    log "Process date=${date_folder}: failed status=${status} (total=${total_duration_sec}s)"
  fi
}

STOP_REQUESTED=0
on_stop() {
  STOP_REQUESTED=1
  log "Stop signal received. daemon loop will exit."
}
trap on_stop INT TERM

log "Always-on daemon started. data_dir=${HOST_DATA_DIR}, poll=${POLL_INTERVAL_SEC}s"

while [[ "${STOP_REQUESTED}" -eq 0 ]]; do
  if ! runtime_ready; then
    sleep "${POLL_INTERVAL_SEC}"
    continue
  fi

  if [[ ! -d "${HOST_DATA_DIR}" ]]; then
    log "Data directory not found: ${HOST_DATA_DIR}"
    sleep "${POLL_INTERVAL_SEC}"
    continue
  fi

  mapfile -t date_folders < <(
    find "${HOST_DATA_DIR}" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' \
      | grep -E '^[0-9]{8}$' \
      | sort
  )

  for date_folder in "${date_folders[@]}"; do
    [[ "${STOP_REQUESTED}" -eq 1 ]] && break
    if grep -qx "${date_folder}" "${STATE_FILE}"; then
      continue
    fi
    run_one_date_folder "${date_folder}"
  done

  [[ "${STOP_REQUESTED}" -eq 1 ]] && break
  sleep "${POLL_INTERVAL_SEC}"
done

log "Always-on daemon stopped."

