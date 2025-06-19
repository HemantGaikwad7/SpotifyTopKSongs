// Main Flink Job Class
package com.spotify.topk;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class SpotifyTopKSongsJob {
    
    private static final int TOP_K = 100;
    private static final int WINDOW_SIZE_MINUTES = 60; // 1 hour window
    private static final int SLIDE_SIZE_MINUTES = 5;   // Slide every 5 minutes
    
    public static void main(String[] args) throws Exception {
        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(30000); // Checkpoint every 30 seconds
        
        // Configure Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("song-plays")
                .setGroupId("spotify-topk-consumer")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        
        // Create data stream from Kafka
        DataStream<String> kafkaStream = env.fromSource(source, 
            WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> {
                    try {
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode node = mapper.readTree(event);
                        return node.get("timestamp").asLong();
                    } catch (Exception e) {
                        return System.currentTimeMillis();
                    }
                }), 
            "kafka-source");
        
        // Parse JSON messages and create SongPlay objects
        DataStream<SongPlay> songPlays = kafkaStream
                .map(new JsonToSongPlayMapper())
                .name("parse-song-plays");
        
        // Process Top K songs using sliding window
        DataStream<TopKResult> topKResults = songPlays
                .keyBy(SongPlay::getSongId)
                .window(SlidingEventTimeWindows.of(
                    Time.minutes(WINDOW_SIZE_MINUTES), 
                    Time.minutes(SLIDE_SIZE_MINUTES)))
                .aggregate(new CountAggregator(), new TopKWindowFunction())
                .name("sliding-window-aggregation")
                .keyBy(result -> "global") // Global key for final top-k
                .process(new TopKProcessor())
                .name("top-k-processor");
        
        // Output results (could be to Kafka, database, etc.)
        topKResults.print().name("print-results");
        
        // Execute the job
        env.execute("Spotify Top K Songs");
    }
    
    // Data model classes
    public static class SongPlay {
        public String songId;
        public String userId;
        public long timestamp;
        public String songName;
        public String artist;
        
        public SongPlay() {}
        
        public SongPlay(String songId, String userId, long timestamp, String songName, String artist) {
            this.songId = songId;
            this.userId = userId;
            this.timestamp = timestamp;
            this.songName = songName;
            this.artist = artist;
        }
        
        public String getSongId() { return songId; }
        public String getUserId() { return userId; }
        public long getTimestamp() { return timestamp; }
        public String getSongName() { return songName; }
        public String getArtist() { return artist; }
        
        @Override
        public String toString() {
            return String.format("SongPlay{songId='%s', userId='%s', timestamp=%d, songName='%s', artist='%s'}", 
                songId, userId, timestamp, songName, artist);
        }
    }
    
    public static class SongCount {
        public String songId;
        public String songName;
        public String artist;
        public long count;
        
        public SongCount() {}
        
        public SongCount(String songId, String songName, String artist, long count) {
            this.songId = songId;
            this.songName = songName;
            this.artist = artist;
            this.count = count;
        }
        
        @Override
        public String toString() {
            return String.format("SongCount{songId='%s', songName='%s', artist='%s', count=%d}", 
                songId, songName, artist, count);
        }
    }
    
    public static class TopKResult {
        public long windowStart;
        public long windowEnd;
        public List<SongCount> topSongs;
        
        public TopKResult() {
            this.topSongs = new ArrayList<>();
        }
        
        public TopKResult(long windowStart, long windowEnd, List<SongCount> topSongs) {
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.topSongs = topSongs;
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("TopK Result [%s - %s]:\n", 
                Instant.ofEpochMilli(windowStart), 
                Instant.ofEpochMilli(windowEnd)));
            
            for (int i = 0; i < topSongs.size(); i++) {
                SongCount song = topSongs.get(i);
                sb.append(String.format("  %d. %s by %s - %d plays\n", 
                    i + 1, song.songName, song.artist, song.count));
            }
            return sb.toString();
        }
    }
    
    // Mapper to convert JSON to SongPlay
    public static class JsonToSongPlayMapper implements MapFunction<String, SongPlay> {
        private final ObjectMapper objectMapper = new ObjectMapper();
        
        @Override
        public SongPlay map(String json) throws Exception {
            JsonNode node = objectMapper.readTree(json);
            return new SongPlay(
                node.get("song_id").asText(),
                node.get("user_id").asText(),
                node.get("timestamp").asLong(),
                node.get("song_name").asText(),
                node.get("artist").asText()
            );
        }
    }
    
    // Aggregate function to count song plays
    public static class CountAggregator implements AggregateFunction<SongPlay, Tuple2<SongPlay, Long>, SongCount> {
        
        @Override
        public Tuple2<SongPlay, Long> createAccumulator() {
            return new Tuple2<>(null, 0L);
        }
        
        @Override
        public Tuple2<SongPlay, Long> add(SongPlay value, Tuple2<SongPlay, Long> accumulator) {
            return new Tuple2<>(value, accumulator.f1 + 1);
        }
        
        @Override
        public SongCount getResult(Tuple2<SongPlay, Long> accumulator) {
            SongPlay song = accumulator.f0;
            return new SongCount(song.songId, song.songName, song.artist, accumulator.f1);
        }
        
        @Override
        public Tuple2<SongPlay, Long> merge(Tuple2<SongPlay, Long> a, Tuple2<SongPlay, Long> b) {
            return new Tuple2<>(a.f0 != null ? a.f0 : b.f0, a.f1 + b.f1);
        }
    }
    
    // Window function to collect all song counts in the window
    public static class TopKWindowFunction extends ProcessWindowFunction<SongCount, TopKResult, String, TimeWindow> {
        
        @Override
        public void process(String key, Context context, Iterable<SongCount> elements, Collector<TopKResult> out) {
            List<SongCount> allSongs = new ArrayList<>();
            for (SongCount songCount : elements) {
                allSongs.add(songCount);
            }
            
            // Sort and get top K
            allSongs.sort((a, b) -> Long.compare(b.count, a.count));
            List<SongCount> topK = allSongs.subList(0, Math.min(TOP_K, allSongs.size()));
            
            TopKResult result = new TopKResult(context.window().getStart(), context.window().getEnd(), topK);
            out.collect(result);
        }
    }
    
    // Process function to maintain global top K across all windows
    public static class TopKProcessor extends KeyedProcessFunction<String, TopKResult, TopKResult> {
        
        private ListState<SongCount> topKState;
        
        @Override
        public void open(Configuration parameters) {
            ListStateDescriptor<SongCount> descriptor = new ListStateDescriptor<>(
                "top-k-songs",
                TypeInformation.of(new TypeHint<SongCount>() {})
            );
            topKState = getRuntimeContext().getListState(descriptor);
        }
        
        @Override
        public void processElement(TopKResult value, Context ctx, Collector<TopKResult> out) throws Exception {
            // Merge current top K with state
            Map<String, SongCount> songMap = new HashMap<>();
            
            // Add existing state
            for (SongCount song : topKState.get()) {
                songMap.put(song.songId, song);
            }
            
            // Add new songs from window
            for (SongCount song : value.topSongs) {
                SongCount existing = songMap.get(song.songId);
                if (existing != null) {
                    // Update with higher count
                    if (song.count > existing.count) {
                        songMap.put(song.songId, song);
                    }
                } else {
                    songMap.put(song.songId, song);
                }
            }
            
            // Sort and keep top K
            List<SongCount> globalTopK = new ArrayList<>(songMap.values());
            globalTopK.sort((a, b) -> Long.compare(b.count, a.count));
            globalTopK = globalTopK.subList(0, Math.min(TOP_K, globalTopK.size()));
            
            // Update state
            topKState.clear();
            topKState.addAll(globalTopK);
            
            // Output result
            TopKResult result = new TopKResult(value.windowStart, value.windowEnd, globalTopK);
            out.collect(result);
        }
    }
}