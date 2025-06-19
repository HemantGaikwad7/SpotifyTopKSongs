# SpotifyTopKSongs


## 🔧 Running the Application

### Step 1: Start Infrastructure

```bash
# Make scripts executable
chmod +x scripts/*.sh

# Start Kafka and Flink
./scripts/start-services.sh
```

### Step 2: Build the Application

```bash
# Build the project
mvn clean package

# This creates target/flink-spotify-topk-1.0.0.jar
```

### Step 3: Submit Job to Flink

```bash
# Submit job to Flink cluster
docker exec flink-jobmanager flink run \
  -c com.spotify.topk.SpotifyTopKSongsJob \
  /path/to/your/target/flink-spotify-topk-1.0.0.jar

# Or copy the JAR to the container first
docker cp target/flink-spotify-topk-1.0.0.jar flink-jobmanager:/opt/flink/

docker exec flink-jobmanager flink run \
  -c com.spotify.topk.SpotifyTopKSongsJob \
  /opt/flink/flink-spotify-topk-1.0.0.jar
```

### Step 4: Generate Sample Data

```bash
# Install Python dependencies
pip install kafka-python

# Run the data producer
python scripts/produce-sample-data.py
```

### Step 5: Monitor Results

1. **Flink Web UI**: Open http://localhost:8081
   - View job status and metrics
   - Check task managers and job graph

2. **Check Logs**: 
```bash
# View Flink logs
docker logs flink-taskmanager

# View Kafka logs
docker logs kafka
```

3. **Kafka Consumer** (optional):
```bash
# Consume messages from topic
docker exec -it kafka kafka-console-consumer \
  --topic song-plays \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

## 🛠️ Development Mode

For development, you can run the Flink job locally:

```bash
# Run locally (without Docker)
mvn exec:java -Dexec.mainClass="com.spotify.topk.SpotifyTopKSongsJob"
```

## 📊 Expected Output

The application will output Top K results every 5 minutes (sliding window):

```
TopK Result [2024-01-15T10:00:00Z - 2024-01-15T11:00:00Z]:
  1. Blinding Lights by The Weeknd - 1,234 plays
  2. Watermelon Sugar by Harry Styles - 1,156 plays
  3. Levitating by Dua Lipa - 1,089 plays
  4. Good 4 U by Olivia Rodrigo - 987 plays
  5. Stay by The Kid LAROI & Justin Bieber - 876 plays
  ...
```

## 🔍 Troubleshooting

### Common Issues

1. **OutOfMemoryError**:
   ```bash
   # Increase heap size
   export FLINK_TASKMANAGER_HEAP_SIZE=2048m
   export FLINK_JOBMANAGER_HEAP_SIZE=1024m
   ```

2. **Kafka Connection Issues**:
   ```bash
   # Check if Kafka is running
   docker ps | grep kafka
   
   # Check Kafka logs
   docker logs kafka
   ```

3. **Checkpointing Failures**:
   ```bash
   # Check checkpoint directory permissions
   # Ensure state backend is properly configured
   ```

### Debug Commands

```bash
# List Flink jobs
docker exec flink-jobmanager flink list

# Cancel a job
docker exec flink-jobmanager flink cancel <job-id>

# Check Kafka topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Describe Kafka topic
docker exec kafka kafka-topics --describe --topic song-plays --bootstrap-server localhost:9092
```

## 🚀 Production Considerations

### Scaling

1. **Increase Parallelism**:
   ```java
   env.setParallelism(10); // Increase parallel tasks
   ```

2. **Tune Kafka Partitions**:
   ```bash
   # Create topic with more partitions
   kafka-topics --create --topic song-plays --partitions 10 --replication-factor 3
   ```

3. **Configure Checkpointing**:
   ```java
   env.enableCheckpointing(30000); // 30 seconds
   env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
   ```

### Monitoring

- Use Prometheus metrics exporter
- Set up alerts for job failures
- Monitor memory usage and GC
- Track throughput and latency metrics

### Deployment

- Use Kubernetes with Flink Operator
- Set up proper logging aggregation
- Configure auto-scaling based on load
- Implement proper error handling and retries

This completes the comprehensive setup guide for running the Flink Top K Songs implementation!