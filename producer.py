"""
Kafka producer for streaming water quality observations from CSV to Kafka.
Supports checkpointing to resume from the last sent message.
"""

import csv
import json
import time
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
TOPIC_NAME = 'water-quality-raw'
CSV_FILE_PATH = 'data/observations-2026-4-3-sorted.csv'
BATCH_SIZE = 20000
DELAY_SECONDS = 0.0
CHECKPOINT_FILE = 'data/producer_checkpoint.txt'

def get_checkpoint():
    """Read the last sent message count from the checkpoint file."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                content = f.read().strip()
                return int(content) if content else 0
        except (ValueError, IOError):
            return 0
    return 0

def save_checkpoint(count):
    """Save the current message count to the checkpoint file."""
    try:
        os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
        with open(CHECKPOINT_FILE, 'w') as f:
            f.write(str(count))
    except IOError as e:
        print(f"Warning: Could not save checkpoint: {e}")

def create_producer():
    """Create a Kafka producer with automatic retry if brokers are not ready."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                linger_ms=10,
                compression_type='gzip'
            )
            print(f"Connected to Kafka at {BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            print("Kafka brokers not available, retrying in 5 seconds...")
            time.sleep(5)

def stream_csv_to_kafka():
    """Read rows from CSV and send them to Kafka topic."""
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Error: CSV file '{CSV_FILE_PATH}' not found.")
        return

    producer = create_producer()
    print(f"Starting ingestion: {CSV_FILE_PATH} -> Kafka Topic: {TOPIC_NAME}")
    
    start_time = time.time()
    last_sent_count = get_checkpoint()
    count = 0
    
    try:
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            if last_sent_count > 0:
                print(f"Resuming from message {last_sent_count}...")
                for _ in range(last_sent_count):
                    next(reader, None)
                count = last_sent_count
            
            for row in reader:
                producer.send(TOPIC_NAME, value=row)
                
                count += 1
                if count % BATCH_SIZE == 0:
                    producer.flush()
                    save_checkpoint(count)
                    elapsed = time.time() - start_time
                    rate = count / elapsed if elapsed > 0 else 0
                    print(f"Sent {count} messages... Rate: {rate:.2f} msg/sec")
                
                if DELAY_SECONDS > 0:
                    time.sleep(DELAY_SECONDS)
                
    except KeyboardInterrupt:
        print("\nStopping producer...")
    except Exception as e:
        print(f"Error during ingestion: {e}")
    finally:
        producer.flush()
        save_checkpoint(count)
        producer.close()
        print(f"Finished. Total messages sent: {count}")

if __name__ == "__main__":
    stream_csv_to_kafka()