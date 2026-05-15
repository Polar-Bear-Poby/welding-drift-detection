#!/usr/bin/env bash
# Restart both Spark streaming consumers with corrected spark_streaming.py
pkill -f spark_streaming.py || true
sleep 3

for i in 1 2; do
  if [ $((i % 2)) -eq 1 ]; then
    topic="welding.raw.laser_a.v1"
    channel="laser_a"
    group="welding-stream-laser-a"
  else
    topic="welding.raw.laser_b.v1"
    channel="laser_b"
    group="welding-stream-laser-b"
  fi
  ivy_dir="/tmp/.ivy2-consumer-${i}"
  mkdir -p "${ivy_dir}"
  rm -rf "/tmp/spark-checkpoints-consumer-${i}"
  TOPIC_RAW="${topic}" \
  CHANNEL_FILTER="${channel}" \
  KAFKA_GROUP_ID="${group}" \
  SPARK_CHECKPOINT_DIR="/tmp/spark-checkpoints-consumer-${i}" \
  SPARK_TRIGGER_INTERVAL_SEC=2 \
  SPARK_STARTING_OFFSETS=earliest \
  nohup /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.cores.max=1 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=1g \
    --conf spark.jars.ivy="${ivy_dir}" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
    /opt/spark/apps/spark_streaming.py \
    > "/tmp/spark_streaming_consumer_${i}.log" 2>&1 &
done
echo "Consumers restarted (consumer 1=laser_a, consumer 2=laser_b)"
