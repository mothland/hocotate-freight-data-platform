kafka-topics.sh --create \
  --topic telemetry.raw \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1 \
  --if-not-exists

kafka-topics.sh --list --bootstrap-server localhost:9092

kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic telemetry.raw \
  --from-beginning
