#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-${ROOT_DIR}/.env}"
ENV_UTILS="${ROOT_DIR}/scripts/lib/env_utils.sh"

LINE_COUNT="${LINE_COUNT:-2}"
PRODUCER_COUNT="${PRODUCER_COUNT:-}"
CONSUMER_COUNT="${CONSUMER_COUNT:-2}"
TOTAL_BATTERIES="${TOTAL_BATTERIES:-120}"
DATE_FOLDER="${DATE_FOLDER:-20220417}"
# 1.0 = real manufacturing pace (1배속). Increase only when you want accelerated replay.
REPLAY_SPEED="${REPLAY_SPEED:-1.0}"
LINE_SEEDS="${LINE_SEEDS:-42,73,128}"
SAMPLE_BATTERY_COUNT="${SAMPLE_BATTERY_COUNT:-0}"
SAMPLE_BATTERY_SEED="${SAMPLE_BATTERY_SEED:-42}"
# UI is always opened by default for experiment visibility.
# Even if callers pass --no-ui, script keeps UI open behavior.
OPEN_UI="${OPEN_UI:-1}"
OPEN_LOG_TERMINALS="${OPEN_LOG_TERMINALS:-0}"
EXPERIMENT_MODE="${EXPERIMENT_MODE:-0}"
DOWN_AFTER_RUN="${DOWN_AFTER_RUN:-0}"
STOP_RUNTIME_AFTER_RUN="${STOP_RUNTIME_AFTER_RUN:-0}"

ORCHESTRATOR_MODE="${ORCHESTRATOR_MODE:-dag}"
WAIT_DAG_CHAIN="${WAIT_DAG_CHAIN:-1}"
DAG_POLL_INTERVAL_SEC="${DAG_POLL_INTERVAL_SEC:-10}"
DAG_TIMEOUT_SEC="${DAG_TIMEOUT_SEC:-10800}"

AIRFLOW_WEB_CONTAINER="${AIRFLOW_WEB_CONTAINER:-welding-airflow-webserver}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-welding-postgres}"
POSTGRES_DB="${POSTGRES_DB:-welding_drift}"
POSTGRES_USER="${POSTGRES_USER:-welding}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-welding-kafka}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
KAFKA_UI_URL="${KAFKA_UI_URL:-http://localhost:8089}"
AIRFLOW_UI_URL="${AIRFLOW_UI_URL:-http://localhost:8080}"
STREAMLIT_UI_URL="${STREAMLIT_UI_URL:-http://localhost:8501}"
CONSUMER_GROUP_LASER_A="${CONSUMER_GROUP_LASER_A:-welding-stream-laser-a}"
CONSUMER_GROUP_LASER_B="${CONSUMER_GROUP_LASER_B:-welding-stream-laser-b}"
PRODUCER_ASSET_DAG_ID="${PRODUCER_ASSET_DAG_ID:-welding_producer_asset_dag}"
BROKER_ASSET_DAG_ID="${BROKER_ASSET_DAG_ID:-welding_broker_asset_dag}"
CONSUMER_ASSET_DAG_ID="${CONSUMER_ASSET_DAG_ID:-welding_consumer_asset_dag}"
DB_ASSET_DAG_ID="${DB_ASSET_DAG_ID:-welding_db_asset_dag}"
TOPIC_RAW_LASER_A="${TOPIC_RAW_LASER_A:-welding.raw.laser_a.v1}"
TOPIC_RAW_LASER_B="${TOPIC_RAW_LASER_B:-welding.raw.laser_b.v1}"
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
EXPECTED_CHANNELS_PER_PRODUCT="${EXPECTED_CHANNELS_PER_PRODUCT:-2}"
CHANNEL_SCOPE="${CHANNEL_SCOPE:-combined}"

resolve_container_name() {
  local preferred="$1"
  local suffix="$2"
  local found
  found="$(docker ps --format '{{.Names}}' | awk -v p="${preferred}" -v s="${suffix}" '$0==p || $0~s {print; exit}')"
  if [[ -n "${found}" ]]; then
    echo "${found}"
  else
    echo "${preferred}"
  fi
}

usage() {
  cat <<'EOF'
Usage:
  bash scripts/run_experiment.sh [options]

Options:
  --line-count N            Production line count (default: 2)
  --producer-count N        Producer count (must equal line-count in dag mode; default: line-count)
  --consumer-count N        Consumer count (even, default: 2)
  --batteries N             Total batteries to replay (default: 120)
  --date YYYYMMDD           Date folder to replay (default: 20220417)
  --speed N                 Replay speed (default: 1.0)
  --line-seeds TEXT         Line seeds string (default: 42,73,128)
  --sample-battery-count N  (direct mode only) paired battery sampling count
  --sample-battery-seed N   (direct mode only) paired battery sampling seed
  --experimental            Allow replay/re-ingest for already processed date
  --mode dag|direct         Orchestrator mode (default: dag)
  --wait-dag-chain          Wait ingest->spark->post_qc completion (default: on in dag mode)
  --no-wait-dag-chain       Trigger ingest only and return
  --dag-timeout-sec N       DAG chain max wait seconds (default: 10800)
  --dag-poll-sec N          DAG polling interval seconds (default: 10)
  --no-ui                   Do not open Airflow/Kafka UI
  --open-log-terminals      Open separate terminals for producer and consumer logs
  --no-open-log-terminals   Disable separate log terminals
  --down-after-run          Stop all compose containers after run
  --stop-runtime-after-run  Stop runtime after run (direct mode only)
  --help                    Show this help
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --line-count) LINE_COUNT="${2:?}"; shift 2 ;;
      --producer-count) PRODUCER_COUNT="${2:?}"; shift 2 ;;
      --consumer-count) CONSUMER_COUNT="${2:?}"; shift 2 ;;
      --batteries) TOTAL_BATTERIES="${2:?}"; shift 2 ;;
      --date) DATE_FOLDER="${2:?}"; shift 2 ;;
      --speed) REPLAY_SPEED="${2:?}"; shift 2 ;;
      --line-seeds) LINE_SEEDS="${2:?}"; shift 2 ;;
      --sample-battery-count) SAMPLE_BATTERY_COUNT="${2:?}"; shift 2 ;;
      --sample-battery-seed) SAMPLE_BATTERY_SEED="${2:?}"; shift 2 ;;
      --experimental) EXPERIMENT_MODE=1; shift ;;
      --mode) ORCHESTRATOR_MODE="${2:?}"; shift 2 ;;
      --wait-dag-chain) WAIT_DAG_CHAIN=1; shift ;;
      --no-wait-dag-chain) WAIT_DAG_CHAIN=0; shift ;;
      --dag-timeout-sec) DAG_TIMEOUT_SEC="${2:?}"; shift 2 ;;
      --dag-poll-sec) DAG_POLL_INTERVAL_SEC="${2:?}"; shift 2 ;;
      --no-ui)
        OPEN_UI=1
        log "WARN: --no-ui is ignored. UI is always opened by policy."
        shift
        ;;
      --open-log-terminals) OPEN_LOG_TERMINALS=1; shift ;;
      --no-open-log-terminals) OPEN_LOG_TERMINALS=0; shift ;;
      --down-after-run) DOWN_AFTER_RUN=1; shift ;;
      --stop-runtime-after-run) STOP_RUNTIME_AFTER_RUN=1; shift ;;
      -h|--help) usage; exit 0 ;;
      *) echo "ERROR: unknown option: $1" >&2; usage; exit 1 ;;
    esac
  done
}

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

open_in_browser() {
  local url="$1"
  if command -v powershell.exe >/dev/null 2>&1; then
    powershell.exe -NoProfile -Command "Start-Process '${url}'" >/dev/null 2>&1 || true
    return 0
  fi
  if command -v xdg-open >/dev/null 2>&1; then
    xdg-open "${url}" >/dev/null 2>&1 || true
    return 0
  fi
  return 0
}

open_ui_if_needed() {
  log "Opening UI pages: ${AIRFLOW_UI_URL}, ${KAFKA_UI_URL}"
  # Open Airflow with admin credentials pre-filled so no manual login is required.
  local airflow_autologin_url
  airflow_autologin_url="${AIRFLOW_UI_URL}"
  open_in_browser "${airflow_autologin_url}"
  open_in_browser "${KAFKA_UI_URL}"
  open_in_browser "${STREAMLIT_UI_URL}"
}

open_log_terminal_if_needed() {
  local title="$1"
  local command="$2"
  local command_ps="${command//\'/''}"
  if [[ "${OPEN_LOG_TERMINALS}" != "1" ]]; then
    return 0
  fi
  if command -v powershell.exe >/dev/null 2>&1; then
    powershell.exe -NoProfile -Command "Start-Process powershell -WindowStyle Normal -ArgumentList '-NoExit','-Command','${command_ps}'" >/dev/null 2>&1 || true
  fi
}

container_running() {
  local name="$1"
  docker ps --format '{{.Names}}' | grep -qx "${name}"
}

core_containers_ready() {
  local required=(
    welding-zookeeper
    welding-kafka
    welding-postgres
    welding-spark-master
    welding-spark-worker
    welding-airflow-webserver
    welding-airflow-scheduler
    welding-kafka-submission-airflow-dag-processor-1
    welding-kafka-submission-airflow-triggerer-1
  )
  local n
  for n in "${required[@]}"; do
    if ! container_running "${n}"; then
      return 1
    fi
  done
  return 0
}

normalize_host_path_for_compose() {
  local raw="$1"
  if [[ -z "${raw}" ]]; then
    echo ""
    return 0
  fi
  # Convert Windows drive path for Git Bash/MSYS docker compose calls:
  # D:/foo/bar -> /d/foo/bar
  if [[ "${raw}" =~ ^([A-Za-z]):/(.*)$ ]]; then
    local drive="${BASH_REMATCH[1]}"
    local rest="${BASH_REMATCH[2]}"
    echo "/${drive,,}/${rest}"
    return 0
  fi
  if [[ "${raw}" =~ ^([A-Za-z]):\\(.*)$ ]]; then
    local drive="${BASH_REMATCH[1]}"
    local rest="${BASH_REMATCH[2]//\\//}"
    echo "/${drive,,}/${rest}"
    return 0
  fi
  echo "${raw}"
}

compose_up_core() {
  local data_dir_norm storage_dir_norm
  data_dir_norm="$(normalize_host_path_for_compose "${DATA_DIR:-}")"
  storage_dir_norm="$(normalize_host_path_for_compose "${STORAGE_DIR:-}")"
  COMPOSE_CONVERT_WINDOWS_PATHS=1 \
  MSYS_NO_PATHCONV=1 \
  DATA_DIR="${data_dir_norm}" \
  STORAGE_DIR="${storage_dir_norm}" \
    docker compose --env-file "${ENV_FILE}" up -d \
      zookeeper kafka kafka-init kafka-ui spark-master spark-worker \
      airflow-init airflow-webserver airflow-scheduler airflow-dag-processor airflow-triggerer \
      api frontend > /dev/null

  # IMPORTANT:
  # Do not start the default compose `producer` service in DAG experiments.
  # It can flood topics with messages not bound to the current ingest_run_id,
  # causing consumer backlog and intermittent summary shortfall (e.g., 28/30).
  # We still build the image because producer asset DAG uses `docker run` with it.
  COMPOSE_CONVERT_WINDOWS_PATHS=1 \
  MSYS_NO_PATHCONV=1 \
  DATA_DIR="${data_dir_norm}" \
  STORAGE_DIR="${storage_dir_norm}" \
    docker compose --env-file "${ENV_FILE}" build producer > /dev/null

  # Keep a non-running producer container so DAG-side `docker run --volumes-from`
  # and data-date discovery logic can reference `welding-producer` safely.
  COMPOSE_CONVERT_WINDOWS_PATHS=1 \
  MSYS_NO_PATHCONV=1 \
  DATA_DIR="${data_dir_norm}" \
  STORAGE_DIR="${storage_dir_norm}" \
    docker compose --env-file "${ENV_FILE}" create producer > /dev/null
}

db_query_scalar() {
  local sql="$1"
  docker exec -i "${POSTGRES_CONTAINER}" psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -At -c "${sql}" | tr -d '\r' | tail -n 1
}

wait_for_dag_registered() {
  local dag_id="$1"
  local timeout_sec="$2"
  local start_epoch
  start_epoch="$(date +%s)"
  while true; do
    if docker exec "${AIRFLOW_WEB_CONTAINER}" airflow dags list 2>/dev/null | grep -q "^${dag_id}[[:space:]]"; then
      return 0
    fi
    local now_epoch
    now_epoch="$(date +%s)"
    if (( now_epoch - start_epoch >= timeout_sec )); then
      return 1
    fi
    sleep 3
  done
}

unpause_dag_if_needed() {
  local dag_id="$1"
  docker exec "${AIRFLOW_WEB_CONTAINER}" airflow dags unpause "${dag_id}" >/dev/null 2>&1 || true
}

get_group_member_count() {
  local group_id="$1"
  docker exec "${KAFKA_CONTAINER}" bash -lc \
    "kafka-consumer-groups --bootstrap-server ${KAFKA_BOOTSTRAP} --group ${group_id} --describe 2>/dev/null | \
     awk 'NR>1 && NF>=7 && \$7 != \"-\" {print \$7}' | sort -u | wc -l" | tr -d ' \r\n'
}

wait_for_consumers_ready() {
  local expected_per_channel="$1"
  local timeout_sec="$2"
  local start_epoch
  start_epoch="$(date +%s)"
  while true; do
    local a_count b_count
    a_count="$(get_group_member_count "${CONSUMER_GROUP_LASER_A}")"
    b_count="$(get_group_member_count "${CONSUMER_GROUP_LASER_B}")"
    if [[ -n "${a_count}" && -n "${b_count}" ]] && (( a_count == expected_per_channel && b_count == expected_per_channel )); then
      log "Consumer groups ready: ${CONSUMER_GROUP_LASER_A}=${a_count}, ${CONSUMER_GROUP_LASER_B}=${b_count}"
      return 0
    fi
    local now_epoch
    now_epoch="$(date +%s)"
    if (( now_epoch - start_epoch >= timeout_sec )); then
      log "ERROR: consumer groups not ready in ${timeout_sec}s (a=${a_count:-n/a}, b=${b_count:-n/a}, expected=${expected_per_channel})"
      return 1
    fi
    sleep 5
  done
}

restart_streaming_consumers() {
  log "Restarting Spark streaming consumers to exact count=${CONSUMER_COUNT} (odd=laser_a, even=laser_b)..."
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
          group_id='${CONSUMER_GROUP_LASER_A}';
       else
          topic='${TOPIC_RAW_LASER_B}';
          channel='laser_b';
          group_id='${CONSUMER_GROUP_LASER_B}';
        fi;
        consumer_ivy=\"/tmp/.ivy2-consumer-\${consumer_id}\";
        mkdir -p \"\${consumer_ivy}\";
         # [보존] 체크포인트를 삭제하지 않는다.
         # Spark 재시작 시 마지막으로 커밋한 Kafka 오프셋부터 이어서 처리한다.
         # DB 적재 완료 전에 오류가 나도 Kafka 보존 기간(7일) 내 재처리 가능하다.
         # 체크포인트 초기화는 실험 시작 시 docker compose down -v 가 수행한다.
         nohup env TOPIC_RAW=\"\${topic}\" CHANNEL_FILTER=\"\${channel}\" KAFKA_GROUP_ID=\"\${group_id}\" SPARK_CHECKPOINT_DIR=\"/tmp/spark-checkpoints-consumer-\${consumer_id}\" SPARK_TRIGGER_INTERVAL_SEC=\"${SPARK_TRIGGER_INTERVAL_SEC}\" ALLOW_RUN_ID_FALLBACK_UUID=\"${ALLOW_RUN_ID_FALLBACK_UUID}\" STRICT_CHANNEL_TOPIC_MATCH=\"${STRICT_CHANNEL_TOPIC_MATCH}\" LOAD_COMPLETE_GRACE_SEC=\"${LOAD_COMPLETE_GRACE_SEC}\" MISSING_HEARTBEAT_GRACE_SEC=\"${MISSING_HEARTBEAT_GRACE_SEC}\" LOAD_COMPLETE_PARTIAL_SUCCESS_EXPERIMENTAL=\"${LOAD_COMPLETE_PARTIAL_SUCCESS_EXPERIMENTAL}\" LOAD_COMPLETE_MIN_PARTIAL_RATIO=\"${LOAD_COMPLETE_MIN_PARTIAL_RATIO}\" \
           /opt/spark/bin/spark-submit \
         --master spark://spark-master:7077 \
         --conf spark.cores.max=${SPARK_STREAMING_CORES_MAX} \
         --conf spark.executor.cores=${SPARK_STREAMING_EXECUTOR_CORES} \
         --conf spark.executor.memory=${SPARK_STREAMING_EXECUTOR_MEMORY} \
         --conf spark.jars.ivy=\"\${consumer_ivy}\" \
         --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
         /opt/spark/apps/spark_streaming.py \
         >\"/storage/logs/spark_consumer_\${consumer_id}.log\" 2>&1 &
     done;
     echo 'spark_streaming.py consumers restarted';"
}

wait_for_dag_run_state() {
  local dag_id="$1"
  local run_id="$2"
  local timeout_sec="$3"
  local start_epoch
  start_epoch="$(date +%s)"
  while true; do
    local state
    state="$(db_query_scalar "SELECT state FROM welding.dag_run WHERE dag_id='${dag_id}' AND run_id='${run_id}' ORDER BY start_date DESC LIMIT 1;")"
    if [[ "${state}" == "success" ]]; then
      log "${dag_id}/${run_id} -> success"
      return 0
    fi
    if [[ "${state}" == "failed" ]]; then
      log "ERROR: ${dag_id}/${run_id} -> failed"
      return 1
    fi
    if [[ "${state}" == "up_for_retry" ]]; then
      log "WARN: ${dag_id}/${run_id} -> up_for_retry"
    fi
    local now_epoch
    now_epoch="$(date +%s)"
    if (( now_epoch - start_epoch >= timeout_sec )); then
      log "ERROR: timeout waiting ${dag_id}/${run_id} (last_state=${state:-none})"
      return 1
    fi
    sleep "${DAG_POLL_INTERVAL_SEC}"
  done
}

wait_for_child_run_id() {
  local child_dag_id="$1"
  local parent_run_id="$2"
  local timeout_sec="$3"
  local start_epoch
  start_epoch="$(date +%s)"
  while true; do
    local child_run_id
    child_run_id="$(db_query_scalar "SELECT run_id FROM welding.dag_run WHERE dag_id='${child_dag_id}' AND COALESCE(conf->>'source_dag_run_id','')='${parent_run_id}' ORDER BY start_date DESC LIMIT 1;")"
    if [[ -n "${child_run_id}" ]]; then
      echo "${child_run_id}"
      return 0
    fi
    local now_epoch
    now_epoch="$(date +%s)"
    if (( now_epoch - start_epoch >= timeout_sec )); then
      return 1
    fi
    sleep "${DAG_POLL_INTERVAL_SEC}"
  done
}

wait_for_spark_heartbeat() {
  local target_date_iso="$1"
  local min_utc="$2"
  local timeout_sec="$3"
  local start_epoch
  start_epoch="$(date +%s)"
  while true; do
    local spark_run_id
    spark_run_id="$(db_query_scalar "SELECT COALESCE(details->>'spark_run_id','') FROM welding.pipeline_heartbeat WHERE component_name='airflow.welding_spark_processing' AND details->>'status'='SPARK_PROCESS_COMPLETED' AND details->>'target_date'='${target_date_iso}' AND heartbeat_at >= '${min_utc}'::timestamptz ORDER BY heartbeat_at DESC LIMIT 1;")"
    if [[ -n "${spark_run_id}" ]]; then
      echo "${spark_run_id}"
      return 0
    fi
    local now_epoch
    now_epoch="$(date +%s)"
    if (( now_epoch - start_epoch >= timeout_sec )); then
      return 1
    fi
    sleep "${DAG_POLL_INTERVAL_SEC}"
  done
}

wait_for_heartbeat() {
  local component="$1"
  local status="$2"
  local target_date_iso="$3"
  local min_utc="$4"
  local timeout_sec="$5"
  local start_epoch
  start_epoch="$(date +%s)"
  while true; do
    local val
    val="$(db_query_scalar "SELECT COALESCE(details->>'spark_run_id', details->>'run_id', '') FROM welding.pipeline_heartbeat WHERE component_name='${component}' AND details->>'status'='${status}' AND details->>'target_date'='${target_date_iso}' AND heartbeat_at >= '${min_utc}'::timestamptz ORDER BY heartbeat_at DESC LIMIT 1;")"
    if [[ -n "${val}" ]]; then
      echo "${val}"
      return 0
    fi
    local now_epoch
    now_epoch="$(date +%s)"
    if (( now_epoch - start_epoch >= timeout_sec )); then
      return 1
    fi
    sleep "${DAG_POLL_INTERVAL_SEC}"
  done
}

wait_for_post_qc_heartbeat() {
  local target_date_iso="$1"
  local spark_run_id="$2"
  local min_utc="$3"
  local timeout_sec="$4"
  local start_epoch
  start_epoch="$(date +%s)"
  while true; do
    local qc_status
    qc_status="$(db_query_scalar "SELECT COALESCE(details->>'qc_status','') FROM welding.pipeline_heartbeat WHERE component_name='airflow.welding_post_quality_control' AND details->>'status'='REPLAY_COMPLETED' AND details->>'target_date'='${target_date_iso}' AND details->>'spark_run_id'='${spark_run_id}' AND heartbeat_at >= '${min_utc}'::timestamptz ORDER BY heartbeat_at DESC LIMIT 1;")"
    if [[ -n "${qc_status}" ]]; then
      echo "${qc_status}"
      return 0
    fi
    local now_epoch
    now_epoch="$(date +%s)"
    if (( now_epoch - start_epoch >= timeout_sec )); then
      return 1
    fi
    sleep "${DAG_POLL_INTERVAL_SEC}"
  done
}

wait_for_db_qc_heartbeat() {
  local target_date_iso="$1"
  local spark_run_id="$2"
  local min_utc="$3"
  local timeout_sec="$4"
  local start_epoch
  start_epoch="$(date +%s)"
  while true; do
    local qc_status
    qc_status="$(db_query_scalar "SELECT COALESCE(details->>'qc_status','') FROM welding.pipeline_heartbeat WHERE component_name='airflow.welding_db_asset_dag' AND details->>'status'='QC_COMPLETED' AND details->>'target_date'='${target_date_iso}' AND details->>'spark_run_id'='${spark_run_id}' AND heartbeat_at >= '${min_utc}'::timestamptz ORDER BY heartbeat_at DESC LIMIT 1;")"
    if [[ -n "${qc_status}" ]]; then
      echo "${qc_status}"
      return 0
    fi
    local now_epoch
    now_epoch="$(date +%s)"
    if (( now_epoch - start_epoch >= timeout_sec )); then
      return 1
    fi
    sleep "${DAG_POLL_INTERVAL_SEC}"
  done
}

run_direct_mode() {
  local expected_rows
  expected_rows=$((TOTAL_BATTERIES * 2))
  local extra_args=()
  if [[ "${EXPERIMENT_MODE}" == "1" ]]; then
    extra_args+=(--experimental)
  fi
  if [[ "${DOWN_AFTER_RUN}" == "1" ]]; then
    extra_args+=(--down-after-run)
  fi
  if [[ "${STOP_RUNTIME_AFTER_RUN}" == "1" ]]; then
    extra_args+=(--stop-runtime-after-run)
  fi

  cd "${ROOT_DIR}"
  ENV_FILE="${ENV_FILE}" \
  LINE_COUNT="${LINE_COUNT}" \
  PRODUCER_COUNT="${PRODUCER_COUNT}" \
  CONSUMER_COUNT="${CONSUMER_COUNT}" \
  TARGET_PRODUCTS="${TOTAL_BATTERIES}" \
  MAX_PRODUCTS="0" \
  EXPECTED_ROWS="${expected_rows}" \
  DATE_FOLDER="${DATE_FOLDER}" \
  REPLAY_SPEED="${REPLAY_SPEED}" \
  LINE_SEEDS="${LINE_SEEDS}" \
  SAMPLE_BATTERY_COUNT="${SAMPLE_BATTERY_COUNT}" \
  SAMPLE_BATTERY_SEED="${SAMPLE_BATTERY_SEED}" \
  OPEN_UI="${OPEN_UI}" \
  OPEN_LOG_TERMINALS="${OPEN_LOG_TERMINALS}" \
  bash scripts/measure_p1_c1_stream_timing.sh "${extra_args[@]}"
}

run_dag_mode() {
  if (( SAMPLE_BATTERY_COUNT > 0 )); then
    log "WARN: --sample-battery-count is direct mode only. ignoring in dag mode."
  fi
  if [[ "${STOP_RUNTIME_AFTER_RUN}" == "1" ]]; then
    log "WARN: --stop-runtime-after-run is direct mode only. ignoring in dag mode."
  fi

  cd "${ROOT_DIR}"

  # 항상 기존 컨테이너와 볼륨을 완전히 내리고 새로 시작한다.
  # 이전 실험의 Kafka 오프셋·체크포인트·DB 데이터가 남아있으면
  # 새 실험이 오염되거나 부분 처리로 끝나는 원인이 된다.
  log "Bringing down any existing containers and volumes (clean slate)..."
  local data_dir_norm_down storage_dir_norm_down
  data_dir_norm_down="$(normalize_host_path_for_compose "${DATA_DIR:-}")"
  storage_dir_norm_down="$(normalize_host_path_for_compose "${STORAGE_DIR:-}")"
  COMPOSE_CONVERT_WINDOWS_PATHS=1 MSYS_NO_PATHCONV=1 \
  DATA_DIR="${data_dir_norm_down}" STORAGE_DIR="${storage_dir_norm_down}" \
    docker compose --env-file "${ENV_FILE}" down -v --remove-orphans > /dev/null 2>&1 || true
  log "Existing containers removed. Starting fresh..."
  compose_up_core

  AIRFLOW_WEB_CONTAINER="$(resolve_container_name "${AIRFLOW_WEB_CONTAINER}" "(_|-)airflow-webserver$")"
  POSTGRES_CONTAINER="$(resolve_container_name "${POSTGRES_CONTAINER}" "(_|-)welding-postgres$")"
  KAFKA_CONTAINER="$(resolve_container_name "${KAFKA_CONTAINER}" "(_|-)kafka$")"
  SPARK_MASTER_CONTAINER="$(resolve_container_name "${SPARK_MASTER_CONTAINER:-welding-spark-master}" "(_|-)spark-master$")"

  # ── Airflow 관리자 계정 자동 생성 ─────────────────────────────────────────
  # 컨테이너가 올라오고 DB 마이그레이션이 끝난 직후이므로 계정이 없을 수 있다.
  # 이미 존재하면 "already exists" 경고를 뱉고 종료 코드 0으로 성공한다.
  log "Ensuring Airflow admin user exists (id=admin / pw=admin)..."
  docker exec "${AIRFLOW_WEB_CONTAINER}" airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@welding.local > /dev/null 2>&1 || true
  log "Airflow login → ${AIRFLOW_UI_URL}  (id: admin / pw: admin)"

  # Prevent default compose producer service from flooding topics without ingest_run_id.
  docker stop welding-producer > /dev/null 2>&1 || true

  open_ui_if_needed
  # Two foreground log terminals for experiment visibility:
  # 1) Producer-side logs
  # 2) Consumer-side logs
  open_log_terminal_if_needed \
    "producer-log" \
    "bash -lc \"mkdir -p '${STORAGE_DIR:-${ROOT_DIR}/storage}/logs'; touch '${STORAGE_DIR:-${ROOT_DIR}/storage}/logs/producer.log'; tail -F '${STORAGE_DIR:-${ROOT_DIR}/storage}/logs/producer.log'\""
  open_log_terminal_if_needed \
    "consumer-log" \
    "docker exec -it ${SPARK_MASTER_CONTAINER} bash -lc \"tail -F /storage/logs/spark_consumer_1.log /storage/logs/spark_consumer_2.log\""

  local import_errors
  import_errors="$(docker exec "${AIRFLOW_WEB_CONTAINER}" airflow dags list-import-errors || true)"
  if [[ "${import_errors}" != *"No data found"* ]]; then
    log "ERROR: DAG import errors detected."
    echo "${import_errors}"
    return 1
  fi

  local expected_per_channel
  expected_per_channel=$((CONSUMER_COUNT / 2))
  restart_streaming_consumers
  wait_for_consumers_ready "${expected_per_channel}" 120

  local producer_run_id now_ts exp_flag target_date_iso trigger_start_utc
  now_ts="$(date +%Y%m%d_%H%M%S)"
  producer_run_id="manual_producer_${DATE_FOLDER}_${now_ts}"
  target_date_iso="${DATE_FOLDER:0:4}-${DATE_FOLDER:4:2}-${DATE_FOLDER:6:2}"
  trigger_start_utc="$(date -u '+%Y-%m-%d %H:%M:%S+00')"
  if [[ "${EXPERIMENT_MODE}" == "1" ]]; then
    exp_flag="true"
  else
    exp_flag="false"
  fi

  local conf_json
conf_json=$(cat <<JSON
{"target_date":"${DATE_FOLDER}","target_products_total":${TOTAL_BATTERIES},"line_count":${LINE_COUNT},"producer_count":${PRODUCER_COUNT},"replay_speed":${REPLAY_SPEED},"line_seeds":"${LINE_SEEDS}","experimental":${exp_flag},"expected_channels":${EXPECTED_CHANNELS_PER_PRODUCT},"channel_scope":"${CHANNEL_SCOPE}"}
JSON
)

  if ! wait_for_dag_registered "${PRODUCER_ASSET_DAG_ID}" 180; then
    log "ERROR: DAG not registered in time: ${PRODUCER_ASSET_DAG_ID}"
    return 1
  fi
  unpause_dag_if_needed "${PRODUCER_ASSET_DAG_ID}"
  unpause_dag_if_needed "${BROKER_ASSET_DAG_ID}"
  unpause_dag_if_needed "${CONSUMER_ASSET_DAG_ID}"
  unpause_dag_if_needed "${DB_ASSET_DAG_ID}"

  log "Triggering DAG ${PRODUCER_ASSET_DAG_ID} run_id=${producer_run_id}"
  docker exec "${AIRFLOW_WEB_CONTAINER}" airflow dags trigger \
    "${PRODUCER_ASSET_DAG_ID}" \
    --run-id "${producer_run_id}" \
    --conf "${conf_json}" >/dev/null

  if [[ "${WAIT_DAG_CHAIN}" != "1" ]]; then
    log "Triggered only (no wait). producer run_id=${producer_run_id}"
    return 0
  fi

  log "Waiting producer DAG completion..."
  wait_for_dag_run_state "${PRODUCER_ASSET_DAG_ID}" "${producer_run_id}" "${DAG_TIMEOUT_SEC}"

  log "Waiting broker heartbeat..."
  if [[ -z "$(wait_for_heartbeat "airflow.welding_broker_asset_dag" "BROKER_READY" "${target_date_iso}" "${trigger_start_utc}" "${DAG_TIMEOUT_SEC}")" ]]; then
    log "ERROR: broker heartbeat not found"
    return 1
  fi

  log "Waiting consumer heartbeat..."
  local spark_run_id
  spark_run_id="$(wait_for_heartbeat "airflow.welding_consumer_asset_dag" "CONSUMER_PROCESSED" "${target_date_iso}" "${trigger_start_utc}" "${DAG_TIMEOUT_SEC}")"
  if [[ -z "${spark_run_id}" ]]; then
    log "ERROR: consumer heartbeat not found"
    return 1
  fi
  log "Detected consumer heartbeat spark_run_id=${spark_run_id}"

  log "Waiting DB/QC heartbeat..."
  local qc_status
  qc_status="$(wait_for_db_qc_heartbeat "${target_date_iso}" "${spark_run_id}" "${trigger_start_utc}" "${DAG_TIMEOUT_SEC}")"
  if [[ -z "${qc_status}" ]]; then
    log "ERROR: db/qc heartbeat not found for spark_run_id=${spark_run_id}"
    return 1
  fi
  log "Detected db/qc heartbeat qc_status=${qc_status}"

  log "DAG/asset flow completed."
  log "producer_run_id=${producer_run_id}"
  log "spark_run_id=${spark_run_id}"
  log "db_qc_status=${qc_status}"
}

main() {
  if [[ -f "${ENV_UTILS}" ]]; then
    # shellcheck disable=SC1090
    source "${ENV_UTILS}"
  fi
  if declare -F load_env_file_without_override >/dev/null 2>&1; then
    load_env_file_without_override "${ENV_FILE}"
  fi

  parse_args "$@"
  OPEN_UI=1

  if [[ -z "${PRODUCER_COUNT}" ]]; then
    PRODUCER_COUNT="${LINE_COUNT}"
  fi

  if (( LINE_COUNT < 1 || PRODUCER_COUNT < 1 || CONSUMER_COUNT < 2 || CONSUMER_COUNT % 2 != 0 )); then
    echo "ERROR: invalid counts. line>=1, producer>=1, consumer must be even and >=2." >&2
    exit 1
  fi
  if (( TOTAL_BATTERIES < 1 )); then
    echo "ERROR: TOTAL_BATTERIES must be >=1" >&2
    exit 1
  fi

  if [[ "${ORCHESTRATOR_MODE}" == "dag" ]] && (( PRODUCER_COUNT != LINE_COUNT )); then
    echo "ERROR: dag mode requires producer_count == line_count." >&2
    exit 1
  fi

  if [[ "${ORCHESTRATOR_MODE}" == "direct" ]]; then
    log "orchestrator_mode=direct (legacy)"
    open_ui_if_needed
    run_direct_mode
  elif [[ "${ORCHESTRATOR_MODE}" == "dag" ]]; then
    log "orchestrator_mode=dag (asset-first orchestration)"
    run_dag_mode
  else
    echo "ERROR: invalid --mode '${ORCHESTRATOR_MODE}', expected dag|direct" >&2
    exit 1
  fi

  if [[ "${DOWN_AFTER_RUN}" == "1" ]]; then
    log "DOWN_AFTER_RUN=1 -> docker compose down"
    (cd "${ROOT_DIR}" && COMPOSE_CONVERT_WINDOWS_PATHS=1 MSYS_NO_PATHCONV=1 docker compose --env-file "${ENV_FILE}" down) >/dev/null || true
  fi
}

main "$@"
