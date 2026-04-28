#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${ROOT_DIR}/storage/metrics/always_on"
PID_FILE="${STATE_DIR}/daemon.pid"

SPARK_MASTER_CONTAINER="${SPARK_MASTER_CONTAINER:-welding-spark-master}"
FULL_DOWN="${FULL_DOWN:-0}" # 1이면 docker compose down 실행

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
  local container_name="$1"
  docker ps --format '{{.Names}}' | grep -qx "${container_name}"
}

any_compose_service_running() {
  docker compose ps --services --status running 2>/dev/null | grep -q .
}

require_cmd docker

log "Stopping always-on daemon..."
if [[ -f "${PID_FILE}" ]]; then
  daemon_pid="$(cat "${PID_FILE}" 2>/dev/null || true)"
  if [[ -n "${daemon_pid}" ]] && kill -0 "${daemon_pid}" >/dev/null 2>&1; then
    kill -TERM "${daemon_pid}" || true
    for _ in {1..10}; do
      if ! kill -0 "${daemon_pid}" >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done
    if kill -0 "${daemon_pid}" >/dev/null 2>&1; then
      log "Daemon did not stop in time; force killing pid=${daemon_pid}"
      kill -KILL "${daemon_pid}" || true
    fi
  fi
  rm -f "${PID_FILE}"
else
  log "No daemon pid file found."
fi

log "Stopping spark_streaming.py gracefully..."
if container_running "${SPARK_MASTER_CONTAINER}"; then
  docker exec "${SPARK_MASTER_CONTAINER}" bash -lc \
    "pids=\$(pgrep -f 'spark_streaming.py' || true);
     if [ -n \"\$pids\" ]; then
       kill -TERM \$pids || true;
       sleep 5;
       pids2=\$(pgrep -f 'spark_streaming.py' || true);
       if [ -n \"\$pids2\" ]; then
         kill -KILL \$pids2 || true;
       fi;
       echo 'spark_streaming.py stopped';
     else
       echo 'spark_streaming.py not running';
     fi" >/dev/null 2>&1 || true
else
  log "Spark master container is not running, skip spark_streaming.py stop."
fi

cd "${ROOT_DIR}"
if any_compose_service_running; then
  if [[ "${FULL_DOWN}" == "1" ]]; then
    log "Stopping containers with docker compose down..."
    docker compose down
  else
    log "Stopping containers with docker compose stop..."
    docker compose stop
  fi
else
  log "No running docker compose services detected. Skipping container stop/down."
fi

log "Always-on mode stopped."
