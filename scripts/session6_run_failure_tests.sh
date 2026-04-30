#!/usr/bin/env bash
set -euo pipefail

# Session 6 failure scenario runner
# Scenario: kill one Spark streaming consumer -> trigger Airflow consumer health monitor -> verify auto recovery.
#
# Usage:
#   bash scripts/session6_run_failure_tests.sh --expected-consumers 2

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
METRICS_DIR="${ROOT_DIR}/storage/metrics/session6"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
REPORT_TXT="${METRICS_DIR}/failure_report_${TIMESTAMP}.txt"
REPORT_CSV="${METRICS_DIR}/failure_report_${TIMESTAMP}.csv"

EXPECTED_CONSUMERS=2
RECOVERY_TIMEOUT_SEC=300
POLL_INTERVAL_SEC=5
TRIGGER_DAG=1
DOWN_AFTER_RUN=0
POSTGRES_CONTAINER="welding-postgres"
POSTGRES_DB="${POSTGRES_DB:-welding_drift}"
POSTGRES_USER="${POSTGRES_USER:-welding}"
KAFKA_CONTAINER="welding-kafka"
SPARK_MASTER_CONTAINER="welding-spark-master"
AIRFLOW_WEB_CONTAINER="welding-airflow-webserver"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/session6_run_failure_tests.sh [options]

Options:
  --expected-consumers <int>   Total expected consumers (even). Default: 2
  --recovery-timeout-sec <int> Timeout to wait for recovery. Default: 300
  --poll-interval-sec <int>    Poll interval. Default: 5
  --no-trigger-dag             Do not trigger Airflow DAG manually (wait for schedule).
  --down-after-run             Stop docker compose stack after test.
  -h, --help                   Show help.
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
    set -a
    # shellcheck disable=SC1090
    source <(tr -d '\r' < "${env_file}")
    set +a
  fi
}

group_member_count() {
  local group_id="$1"
  docker exec "${KAFKA_CONTAINER}" kafka-consumer-groups \
    --bootstrap-server kafka:9092 \
    --group "${group_id}" \
    --describe 2>/dev/null | awk '
      BEGIN { c=0 }
      NR>1 && NF>6 && $7 != "-" { seen[$7]=1 }
      END {
        for (k in seen) c++;
        print c+0;
      }'
}

db_heartbeat_ts() {
  local sql="$1"
  docker exec -i "${POSTGRES_CONTAINER}" psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -t -A -c "${sql}" | tr -d '\r' | head -n 1
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --expected-consumers) EXPECTED_CONSUMERS="$2"; shift 2 ;;
      --recovery-timeout-sec) RECOVERY_TIMEOUT_SEC="$2"; shift 2 ;;
      --poll-interval-sec) POLL_INTERVAL_SEC="$2"; shift 2 ;;
      --no-trigger-dag) TRIGGER_DAG=0; shift ;;
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

parse_args "$@"
require_cmd docker
load_env_if_exists
mkdir -p "${METRICS_DIR}"

if (( EXPECTED_CONSUMERS < 2 || EXPECTED_CONSUMERS % 2 != 0 )); then
  echo "ERROR: expected-consumers must be even and >=2" >&2
  exit 1
fi

for required in "${KAFKA_CONTAINER}" "${SPARK_MASTER_CONTAINER}" "${AIRFLOW_WEB_CONTAINER}" "${POSTGRES_CONTAINER}"; do
  if ! container_running "${required}"; then
    echo "ERROR: required container not running: ${required}" >&2
    echo "Run: bash scripts/start_always_on_pipeline.sh" >&2
    exit 1
  fi
done

expected_per_channel=$((EXPECTED_CONSUMERS / 2))

before_a="$(group_member_count "welding-stream-laser-a")"
before_b="$(group_member_count "welding-stream-laser-b")"
log "Before failure: laser_a_members=${before_a}, laser_b_members=${before_b}"

victim_pid="$(docker exec "${SPARK_MASTER_CONTAINER}" bash -lc "pgrep -f 'spark_streaming.py' | head -n 1" | tr -d '\r')"
if [[ -z "${victim_pid}" ]]; then
  echo "ERROR: no spark_streaming.py process found to kill" >&2
  exit 1
fi

kill_epoch="$(date +%s)"
kill_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
log "Killing spark streaming PID=${victim_pid} at ${kill_iso}"
docker exec "${SPARK_MASTER_CONTAINER}" bash -lc "kill -TERM ${victim_pid}"

if (( TRIGGER_DAG == 1 )); then
  run_id="manual__session6_failure_${TIMESTAMP}"
  log "Triggering Airflow DAG welding_consumer_health_monitor run_id=${run_id}"
  docker exec "${AIRFLOW_WEB_CONTAINER}" airflow dags trigger welding_consumer_health_monitor --run-id "${run_id}" >/dev/null
fi

recovered=0
recovery_epoch=""
recovery_iso=""
deadline=$((kill_epoch + RECOVERY_TIMEOUT_SEC))
while (( "$(date +%s)" <= deadline )); do
  cur_a="$(group_member_count "welding-stream-laser-a")"
  cur_b="$(group_member_count "welding-stream-laser-b")"
  log "Polling members: laser_a=${cur_a}/${expected_per_channel}, laser_b=${cur_b}/${expected_per_channel}"
  if (( cur_a >= expected_per_channel && cur_b >= expected_per_channel )); then
    recovered=1
    recovery_epoch="$(date +%s)"
    recovery_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    break
  fi
  sleep "${POLL_INTERVAL_SEC}"
done

detect_ts="$(db_heartbeat_ts "SELECT to_char(MAX(heartbeat_at AT TIME ZONE 'UTC'),'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') FROM welding.pipeline_heartbeat WHERE component_name='airflow.consumer_health_monitor.check' AND details->>'all_healthy'='false' AND heartbeat_at >= '${kill_iso}'::timestamptz;")"
restart_ts="$(db_heartbeat_ts "SELECT to_char(MAX(heartbeat_at AT TIME ZONE 'UTC'),'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') FROM welding.pipeline_heartbeat WHERE component_name='airflow.consumer_health_monitor' AND details->>'status'='restart_ok' AND heartbeat_at >= '${kill_iso}'::timestamptz;")"

if (( recovered == 1 )); then
  mttr_sec=$((recovery_epoch - kill_epoch))
  result="recovered"
else
  mttr_sec=""
  result="timeout"
fi

printf 'timestamp,expected_consumers,before_a,before_b,recovered,recovery_time_sec,kill_utc,recovery_utc,detect_utc,restart_ok_utc,result\n' > "${REPORT_CSV}"
printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
  "${TIMESTAMP}" "${EXPECTED_CONSUMERS}" "${before_a}" "${before_b}" \
  "${recovered}" "${mttr_sec}" "${kill_iso}" "${recovery_iso}" "${detect_ts}" "${restart_ts}" "${result}" >> "${REPORT_CSV}"

cat > "${REPORT_TXT}" <<EOF
session6_failure_test_report
timestamp=${TIMESTAMP}
expected_consumers=${EXPECTED_CONSUMERS}
expected_per_channel=${expected_per_channel}
before_laser_a=${before_a}
before_laser_b=${before_b}
kill_utc=${kill_iso}
detected_unhealthy_utc=${detect_ts}
restart_ok_utc=${restart_ts}
recovered=${recovered}
recovery_utc=${recovery_iso}
recovery_time_sec=${mttr_sec}
result=${result}
report_csv=${REPORT_CSV}
EOF

log "Failure test completed: result=${result}"
log "Report TXT: ${REPORT_TXT}"
log "Report CSV: ${REPORT_CSV}"

if (( DOWN_AFTER_RUN == 1 )); then
  log "down-after-run enabled -> docker compose down"
  (cd "${ROOT_DIR}" && docker compose down --remove-orphans) || true
fi
