#!/bin/bash

echo "Starting Kafka and Flink services..."

# Start services
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 30

# Create Kafka topic
echo "Creating Kafka topic..."
docker exec kafka kafka-topics --create \
  --topic song-plays \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

echo "Services are ready!"
echo "Kafka UI: http://localhost:9092"
echo "Flink UI: http://localhost:8081"