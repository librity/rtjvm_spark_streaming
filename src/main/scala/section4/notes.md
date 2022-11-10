# Section 4 - Notes

## Spark

- https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
- https://spark.apache.org/docs/latest/job-scheduling.html
- https://www.educba.com/spark-executor/

## Kafka

- https://github.com/apache/kafka
- https://kafka.apache.org/quickstart

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

## Cassandra

- Free and open-source, distributed, wide-column store, NoSQL database management system.
- https://wikiless.org/wiki/Apache_Cassandra?lang=en
- https://gitbox.apache.org/repos/asf?p=cassandra.git
- https://cassandra.apache.org/_/index.html

```bash
# Start containers
$ docker compose up

# In another shell...
$ ./cql.sh
```

In the Cassandra shell:

```cassandraql
-- Create keyspace
CREATE KEYSPACE public
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 };

-- Create table
CREATE TABLE public.cars
(
    "Name"       TEXT PRIMARY KEY,
    "Horsepower" INT,
);

-- Run queries as you would
SELECT *
FROM public.cars;

-- Delete all rows
TRUNCATE public.cars;
```
