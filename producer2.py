"""
Kafka producer for streaming Vietnamese aquaculture water quality data from CSV to Kafka.
Transforms wide-format DACN dataset into the same message schema expected by consumers.
Supports checkpointing to resume from the last sent message.
"""

import csv
import json
import time
import os
import re
import hashlib
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
TOPIC_NAME = 'water-quality-raw'
CSV_FILE_PATH = 'data/dataset_DACN-2024-2022.csv'
BATCH_SIZE = 5000
DELAY_SECONDS = 0.0
CHECKPOINT_FILE = 'data/producer2_checkpoint.txt'

# Mapping from Vietnamese CSV columns to determinand labels and units.
# Each entry: (csv_column_name, determinand_label, unit)
DETERMINAND_MAP = [
    ("Nhiệt độ",    "Temperature",                          "DEGREE CELSIUS"),
    ("pH",          "pH",                                    "pH UNITS"),
    ("DO",          "Oxygen, Dissolved as O2",               "MILLIGRAM PER LITRE"),
    ("Độ dẫn",      "Electrical Conductivity",               "MICROSIEMENS PER CENTIMETRE"),
    ("Độ kiềm",     "Alkalinity to pH 4.5 as CaCO3",        "MILLIGRAM PER LITRE"),
    ("N-NO2",       "Nitrogen, Nitrite as N",                "MILLIGRAM PER LITRE"),
    ("N-NH4",       "Ammoniacal Nitrogen as N",              "MILLIGRAM PER LITRE"),
    ("P-PO4",       "Phosphorus, Orthophosphate as P",       "MILLIGRAM PER LITRE"),
    ("H2S",         "Hydrogen Sulphide as S",                "MILLIGRAM PER LITRE"),
    ("TSS",         "Suspended Solids (Filterable) at 105C", "MILLIGRAM PER LITRE"),
    ("COD",         "COD as O2 : Dichromate Method",         "MILLIGRAM PER LITRE"),
    ("Coliform",    "Coliform, Total, Conf by MPN",          "MPN PER 100 MILLILITRES"),
]


def parse_vietnamese_number(value_str):
    """Parse a number that may use Vietnamese comma-as-decimal format."""
    if not value_str or not value_str.strip():
        return None
    s = value_str.strip()
    # Handle qualitative values (e.g. "Âm tính", "Dương tính")
    if any(c.isalpha() for c in s):
        return None
    # Vietnamese format uses comma as decimal separator
    s = s.replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return None


def parse_coordinates(coord_str):
    """Parse the coordinate field which contains lat and lon separated by newline.
    
    Format: "10,678892\\n 105,522353" -> (10.678892, 105.522353)
    """
    if not coord_str or not coord_str.strip():
        return None, None
    parts = coord_str.strip().split('\n')
    if len(parts) < 2:
        # Try splitting by space if newline doesn't work
        parts = coord_str.strip().split()
    
    lat, lon = None, None
    try:
        lat = float(parts[0].strip().replace(',', '.'))
    except (ValueError, IndexError):
        pass
    try:
        lon = float(parts[1].strip().replace(',', '.'))
    except (ValueError, IndexError):
        pass
    return lat, lon


def parse_date(date_str):
    """Parse Vietnamese date format (dd/mm/yyyy) to ISO format (yyyy-MM-dd HH:mm:ss)."""
    if not date_str or not date_str.strip():
        return None
    s = date_str.strip()
    # Try dd/mm/yyyy
    match = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', s)
    if match:
        day, month, year = match.groups()
        return f"{year}-{int(month):02d}-{int(day):02d} 00:00:00"
    # Try yyyy-mm-dd
    match = re.match(r'(\d{4})-(\d{1,2})-(\d{1,2})', s)
    if match:
        year, month, day = match.groups()
        return f"{year}-{int(month):02d}-{int(day):02d} 00:00:00"
    return None


def generate_station_id(station_name, province, district):
    """Generate a deterministic station notation from the station name."""
    raw = f"{province}-{district}-{station_name}"
    short_hash = hashlib.md5(raw.encode('utf-8')).hexdigest()[:8].upper()
    return f"VN-{short_hash}"


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


def row_to_messages(row, units_row):
    """Transform a wide-format CSV row into multiple Kafka messages (one per determinand).
    
    Each message matches the schema expected by region_consumer and station_consumer:
    {
        "id", "samplingPoint.notation", "samplingPoint.prefLabel",
        "samplingPoint.longitude", "samplingPoint.latitude",
        "samplingPoint.region", "samplingPoint.area", "samplingPoint.subArea",
        "samplingPoint.samplingPointStatus", "samplingPoint.samplingPointType",
        "phenomenonTime", "samplingPurpose", "sampleMaterialType",
        "determinand.notation", "determinand.prefLabel", "result", "unit"
    }
    """
    station_name = row.get("Điểm Quan Trắc", "").strip()
    province = row.get("Tỉnh", "").strip()
    district = row.get("Huyện", "").strip()
    coord_str = row.get("Tọa độ", "")
    date_str = row.get("Ngày quan trắc", "")

    if not station_name or not date_str:
        return []

    latitude, longitude = parse_coordinates(coord_str)
    phenomenon_time = parse_date(date_str)
    if not phenomenon_time:
        return []

    station_id = generate_station_id(station_name, province, district)
    # Use province as region, district as area
    region = province if province else "Unknown"
    area = district if district else "Unknown"

    messages = []
    determinand_counter = 0

    for csv_col, det_label, unit in DETERMINAND_MAP:
        raw_value = row.get(csv_col, "")
        numeric_val = parse_vietnamese_number(raw_value)

        if numeric_val is None:
            continue

        determinand_counter += 1
        # Generate a unique observation ID
        obs_id = f"vn-dacn/{station_id}/{phenomenon_time[:10]}/{determinand_counter:04d}"

        msg = {
            "id": obs_id,
            "samplingPoint.notation": station_id,
            "samplingPoint.prefLabel": f"{station_name}, {district}, {province}",
            "samplingPoint.longitude": str(longitude) if longitude is not None else "",
            "samplingPoint.latitude": str(latitude) if latitude is not None else "",
            "samplingPoint.region": region,
            "samplingPoint.area": area,
            "samplingPoint.subArea": district,
            "samplingPoint.samplingPointStatus": "OPEN",
            "samplingPoint.samplingPointType": "AQUACULTURE MONITORING",
            "phenomenonTime": phenomenon_time,
            "samplingPurpose": "AQUACULTURE WATER QUALITY MONITORING",
            "sampleMaterialType": "SURFACE WATER",
            "determinand.notation": str(determinand_counter),
            "determinand.prefLabel": det_label,
            "result": str(numeric_val),
            "unit": unit,
        }
        messages.append(msg)

    return messages


def stream_csv_to_kafka():
    """Read rows from DACN CSV, transform to consumer-compatible messages, and send to Kafka."""
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Error: CSV file '{CSV_FILE_PATH}' not found.")
        return

    producer = create_producer()
    print(f"Starting ingestion: {CSV_FILE_PATH} -> Kafka Topic: {TOPIC_NAME}")

    start_time = time.time()
    last_sent_count = get_checkpoint()
    count = 0
    rows_processed = 0

    try:
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            # Read and skip the units row (second row in the file, first data row for DictReader)
            units_row = next(reader, None)

            if last_sent_count > 0:
                print(f"Resuming from message {last_sent_count}...")

            for row in reader:
                # Skip rows without a valid Place index
                place = row.get("Place", "").strip()
                if not place or not place.isdigit():
                    continue

                messages = row_to_messages(row, units_row)
                rows_processed += 1

                for msg in messages:
                    count += 1
                    if count <= last_sent_count:
                        continue  # Skip already-sent messages

                    producer.send(TOPIC_NAME, value=msg)

                    if count % BATCH_SIZE == 0:
                        producer.flush()
                        save_checkpoint(count)
                        elapsed = time.time() - start_time
                        rate = count / elapsed if elapsed > 0 else 0
                        print(f"Sent {count} messages... ({rows_processed} CSV rows) Rate: {rate:.2f} msg/sec")

                    if DELAY_SECONDS > 0:
                        time.sleep(DELAY_SECONDS)

    except KeyboardInterrupt:
        print("\nStopping producer...")
    except Exception as e:
        print(f"Error during ingestion: {e}")
        import traceback
        traceback.print_exc()
    finally:
        producer.flush()
        save_checkpoint(count)
        producer.close()
        print(f"Finished. Total messages sent: {count} (from {rows_processed} CSV rows)")


if __name__ == "__main__":
    stream_csv_to_kafka()
