#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${ROOT_DIR}/.env}"
ENV_UTILS="${ROOT_DIR}/scripts/lib/env_utils.sh"
STATE_DIR="${ROOT_DIR}/storage/metrics/always_on"
PID_FILE="${STATE_DIR}/daemon.pid"
LOG_FILE="${STATE_DIR}/daemon.log"

SPARK_MASTER_CONTAINER="${SPARK_MASTER_CONTAINER:-welding-spark-master}"
AIRFLOW_WEB_CONTAINER="${AIRFLOW_WEB_CONTAINER:-welding-airflow-webserver}"
BUILD_PRODUCER_IMAGE="${BUILD_PRODUCER_IMAGE:-1}"
FORCE_BUILD_PRODUCER="${FORCE_BUILD_PRODUCER:-0}"
PRODUCER_IMAGE="${PRODUCER_IMAGE:-welding-producer:latest}"
UNPAUSE_BATCH_DAG="${UNPAUSE_BATCH_DAG:-0}"
TOPIC_RAW_LASER_A="${TOPIC_RAW_LASER_A:-welding.raw.laser_a.v1}"
TOPIC_RAW_LASER_B="${TOPIC_RAW_LASER_B:-welding.raw.laser_b.v1}"
LINE_COUNT="${LINE_COUNT:-3}"
CONSUMER_COUNT="${CONSUMER_COUNT:-$((LINE_COUNT * 2))}"
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
EXPERIMENT_MODE="${EXPERIMENT_MODE:-0}"
EXPERIMENT_RESET_STATE="${EXPERIMENT_RESET_STATE:-0}"
EXPERIMENT_DATE="${EXPERIMENT_DATE:-}"

if [[ -f "${ENV_UTILS}" ]]; then
  # shellcheck disable=SC1090
  source "${ENV_UTILS}"
fi
if declare -F load_env_file_without_override >/dev/null 2>&1; then
  load_env_file_without_override "${ENV_FILE}"
fi
if declare -F ensure_postgres_password >/dev/null 2>&1; then
  ensure_postgres_password "${ENV_FILE}" "welding-postgres" "welding_local_auto_pw"
fi

mkdir -p "${STATE_DIR}"

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

usage() {
  cat <<'EOF'
Usage:
  bash scripts/start_always_on_pipeline.sh [--experimental] [--experimental-date YYYYMMDD] [--help]

Options:
  --experimental             Enable experiment mode and reset processed date state.
  --experimental-date DATE   Reset only the specified DATE (YYYYMMDD) from processed state.
  --help                     Show this help message.
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --experimental)
        EXPERIMENT_MODE=1
        EXPERIMENT_RESET_STATE=1
        shift
        ;;
      --experimental-date)
        EXPERIMENT_MODE=1
        EXPERIMENT_RESET_STATE=1
        EXPERIMENT_DATE="${2:-}"
        if [[ -z "${EXPERIMENT_DATE}" ]]; then
          echo "ERROR: --experimental-date requires YYYYMMDD value" >&2
          exit 1
        fi
        shift 2
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

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: required command not found: $1" >&2
    exit 1
  fi
}

require_cmd docker
parse_args "$@"

if (( CONSUMER_COUNT < 2 || CONSUMER_COUNT % 2 != 0 )); then
  echo "ERROR: CONSUMER_COUNT must be an even number >= 2 (current=${CONSUMER_COUNT})" >&2
  exit 1
fi

compose_service_running() {
  local service_name="$1"
  docker compose --env-file "${ENV_FILE}" ps --services --status running 2>/dev/null | grep -qx "${service_name}"
}

is_any_core_service_missing() {
  local svc
  local services=(
    zookeeper
    kafka
    postgres
    spark-master
    spark-worker
    airflow-webserver
    airflow-scheduler
    airflow-dag-processor
    airflow-triggerer
  )
  for svc in "${services[@]}"; do
    if ! compose_service_running "${svc}"; then
      return 0
    fi
  done
  return 1
}

log "Checking core container status..."
cd "${ROOT_DIR}"
if [[ "${BUILD_PRODUCER_IMAGE}" == "1" && "${FORCE_BUILD_PRODUCER}" == "1" ]]; then
  log "Building producer image..."
  docker compose --env-file "${ENV_FILE}" build producer
elif [[ "${BUILD_PRODUCER_IMAGE}" == "1" ]]; then
  if ! docker image inspect "${PRODUCER_IMAGE}" >/dev/null 2>&1; then
    log "Producer image not found. Building producer image..."
    docker compose --env-file "${ENV_FILE}" build producer
  else
    log "Producer image already exists (${PRODUCER_IMAGE}), skip build."
  fi
fi
if is_any_core_service_missing; then
  log "Missing services detected. Starting required containers..."
  docker compose --env-file "${ENV_FILE}" up -d \
    zookeeper kafka kafka-init kafka-ui \
    postgres \
    spark-master spark-worker \
    airflow-init airflow-webserver airflow-scheduler airflow-dag-processor airflow-triggerer
else
  log "All core containers are already running. Skipping docker compose up."
fi

log "Ensuring broker-distributed Spark consumers are running (line_count=${LINE_COUNT}, consumer_count=${CONSUMER_COUNT})..."
docker exec "${SPARK_MASTER_CONTAINER}" bash -lc \
  "pids=\$(pgrep -f 'spark_streaming.py' 2>/dev/null | grep -vw \"\$\$\" || true);
   if [ -n \"\$pids\" ]; then
     kill -TERM \$pids || true;
     sleep 5;
     pids2=\$(pgrep -f 'spark_streaming.py' 2>/dev/null | grep -vw \"\$\$\" || true);
     if [ -n \"\$pids2\" ]; then
       kill -KILL \$pids2 || true;
     fi;
   fi;
   for consumer_id in \$(seq 1 ${CONSUMER_COUNT}); do
     if [ \$((consumer_id % 2)) -eq 1 ]; then
       topic='${TOPIC_RAW_LASER_A}';
        channel='laser_a';
       group_id='welding-stream-laser-a';
     else
       topic='${TOPIC_RAW_LASER_B}';
        channel='laser_b';
       group_id='welding-stream-laser-b';
     fi;
      consumer_ivy=\"/tmp/.ivy2-consumer-\${consumer_id}\";
      mkdir -p \"\${consumer_ivy}\";
      rm -rf \"/tmp/spark-checkpoints-consumer-\${consumer_id}\";
       nohup env TOPIC_RAW=\"\${topic}\" CHANNEL_FILTER=\"\${channel}\" KAFKA_GROUP_ID=\"\${group_id}\" SPARK_CHECKPOINT_DIR=\"/tmp/spark-checkpoints-consumer-\${consumer_id}\" SPARK_TRIGGER_INTERVAL_SEC=\"${SPARK_TRIGGER_INTERVAL_SEC}\" ALLOW_RUN_ID_FALLBACK_UUID=\"${ALLOW_RUN_ID_FALLBACK_UUID}\" STRICT_CHANNEL_TOPIC_MATCH=\"${STRICT_CHANNEL_TOPIC_MATCH}\" LOAD_COMPLETE_GRACE_SEC=\"${LOAD_COMPLETE_GRACE_SEC}\" MISSING_HEARTBEAT_GRACE_SEC=\"${MISSING_HEARTBEAT_GRACE_SEC}\" LOAD_COMPLETE_PARTIAL_SUCCESS_EXPERIMENTAL=\"${LOAD_COMPLETE_PARTIAL_SUCCESS_EXPERIMENTAL}\" LOAD_COMPLETE_MIN_PARTIAL_RATIO=\"${LOAD_COMPLETE_MIN_PARTIAL_RATIO}\" \
         /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.cores.max=${SPARK_STREAMING_CORES_MAX} \
        --conf spark.executor.cores=${SPARK_STREAMING_EXECUTOR_CORES} \
        --conf spark.executor.memory=${SPARK_STREAMING_EXECUTOR_MEMORY} \
        --conf spark.jars.ivy=\"\${consumer_ivy}\" \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
        /opt/spark/apps/spark_streaming.py \
        >\"/tmp/spark_streaming_consumer_\${consumer_id}.log\" 2>&1 &
   done;
   echo 'spark_streaming.py consumers started';"

log "Unpausing Airflow DAGs for scheduled monitoring/reporting..."
docker exec "${AIRFLOW_WEB_CONTAINER}" airflow dags unpause welding_streaming_monitor >/dev/null 2>&1 || true
docker exec "${AIRFLOW_WEB_CONTAINER}" airflow dags unpause welding_daily_quality_report >/dev/null 2>&1 || true
if [[ "${UNPAUSE_BATCH_DAG}" == "1" ]]; then
  docker exec "${AIRFLOW_WEB_CONTAINER}" airflow dags unpause welding_batch_ingest >/dev/null 2>&1 || true
fi

if [[ -f "${PID_FILE}" ]]; then
  old_pid="$(cat "${PID_FILE}" 2>/dev/null || true)"
  if [[ -n "${old_pid}" ]] && kill -0 "${old_pid}" >/dev/null 2>&1; then
    log "Always-on daemon already running (pid=${old_pid})."
    log "Log file: ${LOG_FILE}"
    exit 0
  fi
fi

log "Starting always-on daemon..."
nohup env \
  EXPERIMENT_MODE="${EXPERIMENT_MODE}" \
  EXPERIMENT_RESET_STATE="${EXPERIMENT_RESET_STATE}" \
  EXPERIMENT_DATE="${EXPERIMENT_DATE}" \
  bash "${ROOT_DIR}/scripts/pipeline_always_on_daemon.sh" >> "${LOG_FILE}" 2>&1 &
daemon_pid="$!"
echo "${daemon_pid}" > "${PID_FILE}"

log "Always-on mode started."
log "Daemon PID: ${daemon_pid}"
log "Daemon log: ${LOG_FILE}"
