# Section 4 - Notes

## Kafka

- https://github.com/apache/kafka
- https://kafka.apache.org/quickstart
- https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html

```bash
# Reset checkpoints
$ rm -rf checkpoints

# Start containers
$ docker compose up

# In another shell...
$ docker ps
$ docker exec -it rockthejvm-sparkstreaming-kafka bash
$ cd /opt/kafka_2.13-2.8.1/

# Create a Kafka topic
$ bin/kafka-topics.sh \
  --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic rockthejvm

# Create a producer and send events to the rockthejvm topic
$ bin/kafka-console-producer.sh \
  --broker-list localhost:9092 \
  --topic rockthejvm

# Create a consumer and receive events from the rockthejvm topic
$ bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic rockthejvm
```
