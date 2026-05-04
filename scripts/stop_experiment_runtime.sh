#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}

stop_streaming_processes() {
  if docker ps --format '{{.Names}}' | grep -qx "welding-spark-master"; then
    log "Stopping spark_streaming.py processes in welding-spark-master ..."
    docker exec welding-spark-master bash -lc \
      "pids=\$(pgrep -f 'spark_streaming.py' 2>/dev/null | grep -vw \"\$\$\" || true);
       if [ -n \"\$pids\" ]; then
         kill -TERM \$pids || true;
         sleep 5;
         pids2=\$(pgrep -f 'spark_streaming.py' 2>/dev/null | grep -vw \"\$\$\" || true);
         if [ -n \"\$pids2\" ]; then
           kill -KILL \$pids2 || true;
         fi;
       fi" >/dev/null 2>&1 || true
  fi
}

stop_services() {
  log "Stopping producer/broker/consumer runtime containers ..."
  (
    cd "${ROOT_DIR}"
    docker compose stop producer kafka zookeeper spark-master spark-worker >/dev/null 2>&1 || true
  )
}

main() {
  stop_streaming_processes
  stop_services
  log "Done. Stopped: producer, kafka, zookeeper, spark-master, spark-worker."
}

main "$@"

