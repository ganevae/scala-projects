#!/bin/bash

# Wait for Kafka to be ready
while ! nc -z kafka 9092; do
  sleep 0.1
done

# Create the topic
kafka-topics.sh --create --topic test-topic --bootstrap-server kafka:9092 --partitions 12 --replication-factor 1