#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   bash scripts/run_20220417_replay_and_dag_metrics.sh
#   bash scripts/run_20220417_replay_and_dag_metrics.sh --down-after-run
#
# Optional env overrides:
#   DATE_FOLDER=20220417
#   LINE_COUNT=3
#   REPLAY_SPEED=100
#   DAG_LOOKBACK_DAYS=30
#   DAG_IDS="welding_batch_ingest,welding_daily_quality_report,welding_streaming_monitor"
#   HOST_DATA_DIR=/abs/path/to/data
#   HOST_STORAGE_DIR=/abs/path/to/storage

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${ROOT_DIR}/.env}"
DOWN_AFTER_RUN="${DOWN_AFTER_RUN:-0}"

load_env_file() {
  local env_file="$1"
  if [[ ! -f "${env_file}" ]]; then
    return 0
  fi
  set -a
  # shellcheck disable=SC1090
  source <(
    sed 's/\r$//' "${env_file}" \
      | grep -v '^[[:space:]]*#' \
      | grep -v '^[[:space:]]*$'
  )
  set +a
}

load_env_file "${ENV_FILE}"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/run_20220417_replay_and_dag_metrics.sh [--down-after-run] [--help]

Options:
  --down-after-run   Stop Airflow and all docker compose containers after run.
  --help             Show this help message.
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --down-after-run)
        DOWN_AFTER_RUN=1
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

DATE_FOLDER="${DATE_FOLDER:-20220417}"
LINE_COUNT="${LINE_COUNT:-3}"
REPLAY_SPEED="${REPLAY_SPEED:-100}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
NETWORK_NAME="${NETWORK_NAME:-welding-kafka-submission_welding-net}"
TOPIC_RAW_LASER_A="${TOPIC_RAW_LASER_A:-welding.raw.laser_a.v1}"
TOPIC_RAW_LASER_B="${TOPIC_RAW_LASER_B:-welding.raw.laser_b.v1}"

PRODUCER_IMAGE="${PRODUCER_IMAGE:-welding-producer:latest}"
SPARK_MASTER_CONTAINER="${SPARK_MASTER_CONTAINER:-welding-spark-master}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-welding-postgres}"

POSTGRES_DB="${POSTGRES_DB:-welding_drift}"
POSTGRES_USER="${POSTGRES_USER:-welding}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"

HOST_DATA_DIR="${HOST_DATA_DIR:-${ROOT_DIR}/data}"
HOST_STORAGE_DIR="${HOST_STORAGE_DIR:-${ROOT_DIR}/storage}"
DATA_DIR_IN_CONTAINER="${DATA_DIR_IN_CONTAINER:-/data}"
STORAGE_DIR_IN_CONTAINER="${STORAGE_DIR_IN_CONTAINER:-/storage}"

DAG_IDS="${DAG_IDS:-welding_batch_ingest,welding_daily_quality_report,welding_streaming_monitor}"
DAG_LOOKBACK_DAYS="${DAG_LOOKBACK_DAYS:-30}"

RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_TAG="${DATE_FOLDER}_${RUN_TS}"
SPARK_OUTPUT_REL="spark_batch_${RUN_TAG}"
SPARK_OUTPUT_HOST="${HOST_STORAGE_DIR}/${SPARK_OUTPUT_REL}"
SPARK_OUTPUT_CONTAINER="${STORAGE_DIR_IN_CONTAINER}/${SPARK_OUTPUT_REL}"
METRICS_DIR="${HOST_STORAGE_DIR}/metrics"

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: required command not found: $1" >&2
    exit 1
  fi
}

require_container_running() {
  local container_name="$1"
  if ! docker ps --format '{{.Names}}' | grep -qx "${container_name}"; then
    echo "ERROR: container is not running: ${container_name}" >&2
    exit 1
  fi
}

require_network_exists() {
  local network_name="$1"
  if ! docker network inspect "${network_name}" >/dev/null 2>&1; then
    echo "ERROR: docker network does not exist: ${network_name}" >&2
    exit 1
  fi
}

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

require_cmd docker
parse_args "$@"

if [[ -z "${POSTGRES_PASSWORD}" ]]; then
  POSTGRES_PASSWORD="$(
    docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "${POSTGRES_CONTAINER}" 2>/dev/null \
      | awk -F= '$1=="POSTGRES_PASSWORD"{print $2; exit}'
  )"
fi
if [[ -z "${POSTGRES_PASSWORD}" ]]; then
  echo "ERROR: POSTGRES_PASSWORD must be set (env/.env/container env)." >&2
  exit 1
fi

if [[ ! -d "${HOST_DATA_DIR}/${DATE_FOLDER}" ]]; then
  echo "ERROR: source date folder does not exist: ${HOST_DATA_DIR}/${DATE_FOLDER}" >&2
  exit 1
fi

mkdir -p "${HOST_STORAGE_DIR}" "${METRICS_DIR}"

require_container_running "${SPARK_MASTER_CONTAINER}"
require_container_running "${POSTGRES_CONTAINER}"
require_container_running "welding-kafka"
require_network_exists "${NETWORK_NAME}"

producer_start_epoch="$(date +%s)"
producer_start_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
log "Producer replay start: date=${DATE_FOLDER}, line_count=${LINE_COUNT}"
docker run --rm \
  --network "${NETWORK_NAME}" \
  -v "${HOST_DATA_DIR}:${DATA_DIR_IN_CONTAINER}:ro" \
  -v "${HOST_STORAGE_DIR}:${STORAGE_DIR_IN_CONTAINER}" \
  -v "${ROOT_DIR}/producer.py:/app/producer.py:ro" \
  "${PRODUCER_IMAGE}" \
  --data-dir "${DATA_DIR_IN_CONTAINER}/${DATE_FOLDER}" \
  --kafka "${KAFKA_BOOTSTRAP}" \
  --topic-laser-a "${TOPIC_RAW_LASER_A}" \
  --topic-laser-b "${TOPIC_RAW_LASER_B}" \
  --line-count "${LINE_COUNT}" \
  --speed "${REPLAY_SPEED}" \
  --no-schedule-wait
producer_end_epoch="$(date +%s)"
producer_end_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
producer_duration_sec="$((producer_end_epoch - producer_start_epoch))"
log "Producer replay done: duration=${producer_duration_sec}s"

spark_start_epoch="$(date +%s)"
spark_start_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
log "Spark batch start: input=${DATA_DIR_IN_CONTAINER}/${DATE_FOLDER}, output=${SPARK_OUTPUT_CONTAINER}"
docker exec "${SPARK_MASTER_CONTAINER}" /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/apps/spark_batch.py \
  --input-dir "${DATA_DIR_IN_CONTAINER}/${DATE_FOLDER}" \
  --output-dir "${SPARK_OUTPUT_CONTAINER}" \
  --write-postgres \
  --postgres-host postgres \
  --postgres-port 5432 \
  --postgres-db "${POSTGRES_DB}" \
  --postgres-user "${POSTGRES_USER}" \
  --postgres-password "${POSTGRES_PASSWORD}"
spark_end_epoch="$(date +%s)"
spark_end_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
spark_duration_sec="$((spark_end_epoch - spark_start_epoch))"
log "Spark batch done: duration=${spark_duration_sec}s"

log "Delete generated 16-split parquet files: ${SPARK_OUTPUT_HOST}"
deleted_count=0
case "${SPARK_OUTPUT_HOST}" in
  "${HOST_STORAGE_DIR}"/*) ;;
  *)
    echo "ERROR: unsafe delete path detected: ${SPARK_OUTPUT_HOST}" >&2
    exit 1
    ;;
esac
if [[ -d "${SPARK_OUTPUT_HOST}" ]]; then
  deleted_count="$(find "${SPARK_OUTPUT_HOST}" -type f \( -name "*.parquet" -o -name "*.snappy.parquet" -o -name "_SUCCESS" -o -name "_started_*" -o -name "_committed_*" \) | wc -l | tr -d ' ')"
  if [[ "${deleted_count}" != "0" ]]; then
    find "${SPARK_OUTPUT_HOST}" -type f \( -name "*.parquet" -o -name "*.snappy.parquet" -o -name "_SUCCESS" -o -name "_started_*" -o -name "_committed_*" \) -delete
  fi
  find "${SPARK_OUTPUT_HOST}" -type d -empty -delete || true
fi
log "Deleted parquet/marker files: ${deleted_count}"

DAG_ID_SQL_LIST="$(csv_to_sql_in_list "${DAG_IDS}")"
DAG_RUN_METRICS_CSV="${METRICS_DIR}/dag_run_metrics_${RUN_TS}.csv"
TASK_METRICS_CSV="${METRICS_DIR}/dag_task_metrics_${RUN_TS}.csv"
SCRIPT_METRICS_TXT="${METRICS_DIR}/script_run_metrics_${RUN_TS}.txt"

log "Collect DAG run metrics -> ${DAG_RUN_METRICS_CSV}"
docker exec -i "${POSTGRES_CONTAINER}" psql \
  -U "${POSTGRES_USER}" \
  -d "${POSTGRES_DB}" \
  -v ON_ERROR_STOP=1 <<SQL > "${DAG_RUN_METRICS_CSV}"
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
  WHERE dr.dag_id IN (${DAG_ID_SQL_LIST})
    AND dr.start_date >= NOW() - INTERVAL '${DAG_LOOKBACK_DAYS} days'
  GROUP BY dr.dag_id, dr.run_id, dr.state, dr.logical_date, dr.start_date, dr.end_date
  ORDER BY dr.start_date DESC
) TO STDOUT WITH CSV HEADER;
SQL

log "Collect DAG task metrics -> ${TASK_METRICS_CSV}"
docker exec -i "${POSTGRES_CONTAINER}" psql \
  -U "${POSTGRES_USER}" \
  -d "${POSTGRES_DB}" \
  -v ON_ERROR_STOP=1 <<SQL > "${TASK_METRICS_CSV}"
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
  WHERE ti.dag_id IN (${DAG_ID_SQL_LIST})
    AND ti.start_date IS NOT NULL
    AND ti.start_date >= NOW() - INTERVAL '${DAG_LOOKBACK_DAYS} days'
  ORDER BY ti.start_date DESC
) TO STDOUT WITH CSV HEADER;
SQL

cat > "${SCRIPT_METRICS_TXT}" <<EOF
run_tag=${RUN_TAG}
date_folder=${DATE_FOLDER}
line_count=${LINE_COUNT}
producer_start=${producer_start_iso}
producer_end=${producer_end_iso}
producer_duration_sec=${producer_duration_sec}
spark_start=${spark_start_iso}
spark_end=${spark_end_iso}
spark_duration_sec=${spark_duration_sec}
spark_output_host=${SPARK_OUTPUT_HOST}
deleted_parquet_files=${deleted_count}
dag_ids=${DAG_IDS}
dag_lookback_days=${DAG_LOOKBACK_DAYS}
dag_run_metrics_csv=${DAG_RUN_METRICS_CSV}
task_metrics_csv=${TASK_METRICS_CSV}
EOF

log "Done."
log "Script metrics: ${SCRIPT_METRICS_TXT}"
log "DAG run metrics: ${DAG_RUN_METRICS_CSV}"
log "Task metrics: ${TASK_METRICS_CSV}"

if [[ "${DOWN_AFTER_RUN}" == "1" ]]; then
  log "DOWN_AFTER_RUN=1 -> stopping Airflow and all project containers."
  docker compose down --remove-orphans >/dev/null 2>&1 || true
fi

