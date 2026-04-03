import json
from kafka import KafkaConsumer

# Initialize the consumer
consumer = KafkaConsumer(
    'water-quality-raw',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # Start from the beginning of the topic
    enable_auto_commit=True,
    group_id='my-test-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consumer started. Waiting for messages...")

try:
    for message in consumer:
        print(f"Received: {message.value}")
except KeyboardInterrupt:
    print("Closing consumer...")
finally:
    consumer.close()