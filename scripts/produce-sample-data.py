#!/usr/bin/env python3

import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Sample data
SONGS = [
    {"id": "song_001", "name": "Blinding Lights", "artist": "The Weeknd"},
    {"id": "song_002", "name": "Watermelon Sugar", "artist": "Harry Styles"},
    {"id": "song_003", "name": "Levitating", "artist": "Dua Lipa"},
    {"id": "song_004", "name": "Good 4 U", "artist": "Olivia Rodrigo"},
    {"id": "song_005", "name": "Stay", "artist": "The Kid LAROI & Justin Bieber"},
    {"id": "song_006", "name": "Anti-Hero", "artist": "Taylor Swift"},
    {"id": "song_007", "name": "As It Was", "artist": "Harry Styles"},
    {"id": "song_008", "name": "Heat Waves", "artist": "Glass Animals"},
    {"id": "song_009", "name": "Unholy", "artist": "Sam Smith ft. Kim Petras"},
    {"id": "song_010", "name": "Flowers", "artist": "Miley Cyrus"},
]

USERS = [f"user_{i:03d}" for i in range(1, 101)]

def generate_song_play():
    """Generate a random song play event"""
    song = random.choice(SONGS)
    user = random.choice(USERS)
    
    # Create weighted distribution (some songs more popular)
    weights = [0.15, 0.12, 0.10, 0.08, 0.08, 0.07, 0.07, 0.06, 0.05, 0.05] + [0.02] * (len(SONGS) - 10)
    song = random.choices(SONGS, weights=weights[:len(SONGS)])[0]
    
    return {
        "song_id": song["id"],
        "user_id": user,
        "timestamp": int(time.time() * 1000),
        "song_name": song["name"],
        "artist": song["artist"]
    }

def main():
    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print("Starting to produce sample song play events...")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            # Generate and send event
            event = generate_song_play()
            producer.send('song-plays', value=event)
            
            print(f"Sent: {event['song_name']} by {event['artist']} (User: {event['user_id']})")
            
            # Random delay between 0.1 and 2 seconds
            time.sleep(random.uniform(0.1, 2.0))
            
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()