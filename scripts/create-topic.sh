#!/bin/bash

docker exec kafka kafka-topics --create \
  --topic song-plays \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo "Topic 'song-plays' created successfully!"