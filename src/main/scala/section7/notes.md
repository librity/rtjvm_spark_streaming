# Section 7 - Notes

## Kafka

```bash
# Start containers
$ docker compose up

# In another shell...
$ docker ps
$ docker exec -it rockthejvm-sparkstreaming-kafka bash
$ cd /opt/kafka_2.13-2.8.1/

# Create the science topic
$ bin/kafka-topics.sh \
  --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic science

# Create a producer and send events to the science topic
$ bin/kafka-console-producer.sh \
  --broker-list localhost:9092 \
  --topic science

# Create a consumer and receive events from the science topic
$ bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic science
```
