# Rock The JVM - Apache Spark Streaming

Master Spark's Streaming Library with Scala.

- https://rockthejvm.com/p/spark-streaming
- https://github.com/rockthejvm/spark-streaming
- https://github.com/rockthejvm/spark-streaming/releases/tag/start

## Certificate

[//]: # (![Certificate of Completion]&#40;.github/certificate.png&#41;)

## Sections

1. [Scala Recap](src/main/scala/section1)

## IntelliJ IDEA

- https://www.jetbrains.com/idea/

## Docker

- https://docs.docker.com/desktop/install/ubuntu/
- https://docs.docker.com/engine/install/ubuntu/#set-up-the-repository

## Environment Containers

Start all required containers:

```bash
$ docker compose up
```

Get a shell in each container:

```bash
# Get a shell in any container
$ docker exec -it CONTAINER_NAME bash
# Get a PostgreSQL shell
$ ./psql.sh
# Get a Cassandra shell
$ ./cql.sh
```

To reset docker container state, purge them with:

```bash
$ ./docker-clean.sh
```

## Netcat

We will need netcat to test streaming data:

```bash
$ which nc
$ nc -lk 12345
```
