docker exec welding-spark-master bash -lc "
pids=\$(pgrep -f 'spark_streaming.py' 2>/dev/null | grep -vw \"\$\$\" || true)
if [ -n \"\$pids\" ]; then
  kill -9 \$pids || true
fi
sleep 2
for consumer_id in 1 2; do
  if [ \$((consumer_id % 2)) -eq 1 ]; then
    topic='welding.raw.laser_a.v1'
    channel='laser_a'
    group_id='welding-stream-laser-a'
  else
    topic='welding.raw.laser_b.v1'
    channel='laser_b'
    group_id='welding-stream-laser-b'
  fi
  consumer_ivy=\"/tmp/.ivy2-consumer-\${consumer_id}\"
  mkdir -p \"\${consumer_ivy}\"
  rm -rf \"/tmp/spark-checkpoints-consumer-\${consumer_id}\"
  nohup env SPARK_STARTING_OFFSETS="earliest" TOPIC_RAW=\"\${topic}\" CHANNEL_FILTER=\"\${channel}\" KAFKA_GROUP_ID=\"\${group_id}\" SPARK_CHECKPOINT_DIR=\"/tmp/spark-checkpoints-consumer-\${consumer_id}\" SPARK_TRIGGER_INTERVAL_SEC=2 ALLOW_RUN_ID_FALLBACK_UUID=0 STRICT_CHANNEL_TOPIC_MATCH=1 LOAD_COMPLETE_GRACE_SEC=600 MISSING_HEARTBEAT_GRACE_SEC=1200 LOAD_COMPLETE_PARTIAL_SUCCESS_EXPERIMENTAL=1 LOAD_COMPLETE_MIN_PARTIAL_RATIO=0.60 /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.cores.max=1 --conf spark.executor.cores=1 --conf spark.executor.memory=1g --conf spark.jars.ivy=\"\${consumer_ivy}\" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 /opt/spark/apps/spark_streaming.py >\"/tmp/spark_streaming_consumer_\${consumer_id}.log\" 2>&1 &
done
"
