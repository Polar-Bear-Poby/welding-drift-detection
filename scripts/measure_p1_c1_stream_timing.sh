#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

HOST_DATA_DIR="${HOST_DATA_DIR:-${ROOT_DIR}/data}"
DATE_FOLDER="${DATE_FOLDER:-20220417}"
DATA_DIR_IN_CONTAINER="${DATA_DIR_IN_CONTAINER:-/data}"
STORAGE_DIR_IN_CONTAINER="${STORAGE_DIR_IN_CONTAINER:-/storage}"
HOST_STORAGE_DIR="${HOST_STORAGE_DIR:-${ROOT_DIR}/storage}"

LINE_COUNT="${LINE_COUNT:-3}"
SPEED="${SPEED:-300}"
MAX_PRODUCTS="${MAX_PRODUCTS:-3}"
REPLAY_SPEED="${REPLAY_SPEED:-${SPEED}}"
CONSUMER_COUNT="${CONSUMER_COUNT:-$((LINE_COUNT * 2))}"
EXPECTED_ROWS="${EXPECTED_ROWS:-$((MAX_PRODUCTS * LINE_COUNT * 2))}"

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
NETWORK_NAME="${NETWORK_NAME:-welding-kafka-submission_welding-net}"
PRODUCER_IMAGE="${PRODUCER_IMAGE:-welding-producer:latest}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-welding-kafka}"
SPARK_MASTER_CONTAINER="${SPARK_MASTER_CONTAINER:-welding-spark-master}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-welding-postgres}"

POSTGRES_DB="${POSTGRES_DB:-welding_drift}"
POSTGRES_USER="${POSTGRES_USER:-welding}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"
if [[ -z "${POSTGRES_PASSWORD}" ]]; then
  echo "ERROR: POSTGRES_PASSWORD must be set" >&2
  exit 1
fi

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
INFERENCE_DELAY_MS_LASER_A="${INFERENCE_DELAY_MS_LASER_A:-0}"
INFERENCE_DELAY_MS_LASER_B="${INFERENCE_DELAY_MS_LASER_B:-0}"

STOP_ALWAYS_ON_PRODUCER="${STOP_ALWAYS_ON_PRODUCER:-1}"
RESTORE_ALWAYS_ON_PRODUCER="${RESTORE_ALWAYS_ON_PRODUCER:-1}"
RESTORE_DEFAULT_STREAMING="${RESTORE_DEFAULT_STREAMING:-1}"
TOPIC_RAW_LASER_A="${TOPIC_RAW_LASER_A:-welding.raw.laser_a.v1}"
TOPIC_RAW_LASER_B="${TOPIC_RAW_LASER_B:-welding.raw.laser_b.v1}"
DOWN_AFTER_RUN="${DOWN_AFTER_RUN:-0}"

METRICS_DIR="${HOST_STORAGE_DIR}/metrics/p1c1"
SUMMARY_TXT="${METRICS_DIR}/p1c1_timing_${RUN_TAG}.txt"
SUMMARY_CSV="${METRICS_DIR}/p1c1_timing_history.csv"

producer_was_running=0

usage() {
  cat <<'EOF'
Usage:
  bash scripts/measure_p1_c1_stream_timing.sh [--down-after-run] [--help]

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

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*"
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

  docker exec "${SPARK_MASTER_CONTAINER}" bash -lc \
    "mkdir -p /tmp/.ivy2;
     rm -rf '${checkpoint_dir}';
     nohup env TOPIC_RAW='${topic_name}' SPARK_CHECKPOINT_DIR='${checkpoint_dir}' CHANNEL_FILTER='${channel_filter}' KAFKA_GROUP_ID='${kafka_group_id}' POSTGRES_PASSWORD='${POSTGRES_PASSWORD}' \
       INFERENCE_DELAY_MS_LASER_A='${INFERENCE_DELAY_MS_LASER_A}' INFERENCE_DELAY_MS_LASER_B='${INFERENCE_DELAY_MS_LASER_B}' \
       /opt/spark/bin/spark-submit \
       --master spark://spark-master:7077 \
       --conf spark.cores.max=${SPARK_STREAMING_CORES_MAX} \
       --conf spark.executor.cores=${SPARK_STREAMING_EXECUTOR_CORES} \
       --conf spark.executor.memory=${SPARK_STREAMING_EXECUTOR_MEMORY} \
       --conf spark.jars.ivy=/tmp/.ivy2 \
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
    "mkdir -p /tmp/.ivy2;
     for consumer_id in \$(seq 1 ${CONSUMER_COUNT}); do
       if [ \$((consumer_id % 2)) -eq 1 ]; then
         topic='${TOPIC_RAW_LASER_A}';
         channel='1';
         group_id='welding-stream-laser-a';
       else
         topic='${TOPIC_RAW_LASER_B}';
         channel='0';
         group_id='welding-stream-laser-b';
       fi;
        rm -rf \"/tmp/spark-checkpoints-consumer-\${consumer_id}\";
        nohup env TOPIC_RAW=\"\${topic}\" CHANNEL_FILTER=\"\${channel}\" KAFKA_GROUP_ID=\"\${group_id}\" SPARK_CHECKPOINT_DIR=\"/tmp/spark-checkpoints-consumer-\${consumer_id}\" POSTGRES_PASSWORD='${POSTGRES_PASSWORD}' \
          INFERENCE_DELAY_MS_LASER_A='${INFERENCE_DELAY_MS_LASER_A}' INFERENCE_DELAY_MS_LASER_B='${INFERENCE_DELAY_MS_LASER_B}' \
          /opt/spark/bin/spark-submit \
         --master spark://spark-master:7077 \
         --conf spark.cores.max=${SPARK_STREAMING_CORES_MAX} \
         --conf spark.executor.cores=${SPARK_STREAMING_EXECUTOR_CORES} \
         --conf spark.executor.memory=${SPARK_STREAMING_EXECUTOR_MEMORY} \
         --conf spark.jars.ivy=/tmp/.ivy2 \
         --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
         /opt/spark/apps/spark_streaming.py \
         >/tmp/spark_streaming_consumer_\${consumer_id}.log 2>&1 &
     done;" >/dev/null 2>&1 || true
}

cleanup() {
  local exit_code="${1:-0}"
  stop_all_streaming_consumers
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

if (( CONSUMER_COUNT < 2 || CONSUMER_COUNT % 2 != 0 )); then
  echo "ERROR: CONSUMER_COUNT must be an even number >= 2 (current=${CONSUMER_COUNT})" >&2
  exit 1
fi

if [[ ! -d "${HOST_DATA_DIR}/${DATE_FOLDER}" ]]; then
  echo "ERROR: source date folder does not exist: ${HOST_DATA_DIR}/${DATE_FOLDER}" >&2
  exit 1
fi

mkdir -p "${METRICS_DIR}"
if [[ ! -f "${SUMMARY_CSV}" ]]; then
  cat > "${SUMMARY_CSV}" <<EOF
run_tag,topic_laser_a,topic_laser_b,date_folder,line_count,consumer_count,max_products,expected_rows,producer_start_utc,producer_end_utc,producer_duration_sec,db_first_utc,db_last_utc,time_to_first_db_sec,end_to_last_db_sec,db_drain_after_producer_sec,db_rows
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
    channel_filter="1"
    group_id="welding-stream-laser-a"
  else
    topic="${TOPIC_LASER_B}"
    channel_filter="0"
    group_id="welding-stream-laser-b"
  fi

  start_streaming_consumer "${topic}" "${checkpoint}" "${log_path}" "${channel_filter}" "${group_id}" >/dev/null

  if ! wait_consumer_ready "${topic}" "${log_path}" "${CONSUMER_START_WAIT_SEC}"; then
    echo "ERROR: benchmark consumer id=${consumer_id} topic=${topic} did not become ready in ${CONSUMER_START_WAIT_SEC}s" >&2
    docker exec "${SPARK_MASTER_CONTAINER}" bash -lc "tail -n 80 '${log_path}'" || true
    exit 1
  fi
done
log "Benchmark consumers started. topic_laser_a=${TOPIC_LASER_A}, topic_laser_b=${TOPIC_LASER_B}"

producer_start_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
producer_start_epoch="$(date +%s)"
log "Producer run start: line_count=${LINE_COUNT} max_products=${MAX_PRODUCTS} speed=${REPLAY_SPEED}"

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

docker run --rm \
  --network "${NETWORK_NAME}" \
  -v "${HOST_DATA_DIR}:${DATA_DIR_IN_CONTAINER}:ro" \
  -v "${HOST_STORAGE_DIR}:${STORAGE_DIR_IN_CONTAINER}" \
  -v "${ROOT_DIR}/producer.py:/app/producer.py:ro" \
  "${PRODUCER_IMAGE}" \
  "${producer_args[@]}"

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
consumer_count=${CONSUMER_COUNT}
inference_delay_ms_laser_a=${INFERENCE_DELAY_MS_LASER_A}
inference_delay_ms_laser_b=${INFERENCE_DELAY_MS_LASER_B}
max_products=${MAX_PRODUCTS}
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

printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
  "${RUN_TAG}" \
  "${TOPIC_LASER_A}" \
  "${TOPIC_LASER_B}" \
  "${DATE_FOLDER}" \
  "${LINE_COUNT}" \
  "${CONSUMER_COUNT}" \
  "${MAX_PRODUCTS}" \
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
