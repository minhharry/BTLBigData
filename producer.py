import csv
import json
import time
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# CONFIGURATION
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
TOPIC_NAME = 'water-quality-raw'
CSV_FILE_PATH = 'data/observations-2026-4-3-sorted.csv'
BATCH_SIZE = 100  # Number of messages before flushing
DELAY_SECONDS = 0.01  # Small delay between messages (100 rows/sec)

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
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Error: CSV file '{CSV_FILE_PATH}' not found.")
        return

    producer = create_producer()
    print(f"Starting ingestion: {CSV_FILE_PATH} -> Kafka Topic: {TOPIC_NAME}")
    
    start_time = time.time()
    count = 0
    
    try:
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as f:
            # Using DictReader for easier mapping to JSON
            reader = csv.DictReader(f)
            
            for row in reader:
                # Send the row as a dictionary (KafkaProducer value_serializer handles JSON)
                producer.send(TOPIC_NAME, value=row)
                
                count += 1
                if count % BATCH_SIZE == 0:
                    producer.flush()
                    elapsed = time.time() - start_time
                    rate = count / elapsed
                    print(f"Sent {count} messages... Rate: {rate:.2f} msg/sec")
                
                # Small sleep to simulate real-time stream
                if DELAY_SECONDS > 0:
                    time.sleep(DELAY_SECONDS)
                
    except KeyboardInterrupt:
        print("\nStopping producer...")
    except Exception as e:
        print(f"Error during ingestion: {e}")
    finally:
        producer.flush()
        producer.close()
        print(f"Finished. Total messages sent: {count}")

if __name__ == "__main__":
    stream_csv_to_kafka()