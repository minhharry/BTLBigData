"""
WQI Consumer — Water Quality Index Calculator

Consumes water quality observations from the 'water-quality-raw' Kafka topic,
calculates a Weighted Arithmetic Water Quality Index (WQI) per sampling point,
and periodically flushes results to PostgreSQL.

WQI Method: Weighted Arithmetic Index (NSF-WQI inspired)
Parameters: pH, Dissolved Oxygen (% sat), BOD, Ammoniacal Nitrogen,
            Nitrate, Orthophosphate, Temperature
"""

import json
import os
import time
import signal
import sys
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# =============================================================================
# CONFIGURATION
# =============================================================================

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
TOPIC_NAME = 'water-quality-raw'
CONSUMER_GROUP = 'wqi-calculator-group'

# PostgreSQL connection
DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
DB_NAME = os.getenv('POSTGRES_DB', 'app_database')
DB_USER = os.getenv('POSTGRES_USER', 'admin')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'your_secure_password')

# WQI Configuration
FLUSH_INTERVAL = int(os.getenv('WQI_FLUSH_INTERVAL', '60'))  # seconds
MIN_PARAMS = int(os.getenv('WQI_MIN_PARAMS', '3'))  # minimum parameters to calculate WQI

# =============================================================================
# WQI PARAMETER DEFINITIONS
# =============================================================================

# Mapping from determinand names in the dataset to our internal parameter keys
DETERMINAND_MAP = {
    'pH': 'ph',
    'Oxygen, Dissolved, % Saturation': 'dissolved_oxygen_pct',
    'BOD : 5 Day ATU': 'bod',
    'Ammoniacal Nitrogen as N': 'ammoniacal_nitrogen',
    'Nitrate as N': 'nitrate',
    'Orthophosphate, reactive as P': 'orthophosphate',
    'Temperature of Water': 'temperature',
}

# WQI parameter configurations:
#   weight: importance weight for the parameter
#   ideal: ideal value (best case)
#   standard: standard/threshold limit
#   type: 'proximity' (closer to ideal = better) or 'pollutant' (lower = better)
WQI_PARAMS = {
    'ph': {
        'weight': 4,
        'ideal': 7.0,
        'standard': 8.5,
        'type': 'proximity',
    },
    'dissolved_oxygen_pct': {
        'weight': 4,
        'ideal': 100.0,
        'standard': 80.0,  # minimum acceptable
        'type': 'proximity',
    },
    'bod': {
        'weight': 3,
        'ideal': 0.0,
        'standard': 6.0,
        'type': 'pollutant',
    },
    'ammoniacal_nitrogen': {
        'weight': 3,
        'ideal': 0.0,
        'standard': 1.5,
        'type': 'pollutant',
    },
    'nitrate': {
        'weight': 2,
        'ideal': 0.0,
        'standard': 10.0,
        'type': 'pollutant',
    },
    'orthophosphate': {
        'weight': 1,
        'ideal': 0.0,
        'standard': 0.1,
        'type': 'pollutant',
    },
    'temperature': {
        'weight': 1,
        'ideal': 15.0,
        'standard': 30.0,
        'type': 'proximity',
    },
}

# WQI classification thresholds
WQI_CATEGORIES = [
    (91, 'Excellent'),
    (71, 'Good'),
    (51, 'Moderate'),
    (26, 'Bad'),
    (0, 'Very Bad'),
]

# =============================================================================
# DATABASE SETUP
# =============================================================================

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS wqi_scores (
    id SERIAL PRIMARY KEY,
    sampling_point_notation VARCHAR(50) NOT NULL,
    sampling_point_label VARCHAR(255),
    region VARCHAR(100),
    area VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    wqi_score DOUBLE PRECISION NOT NULL,
    wqi_category VARCHAR(20) NOT NULL,
    parameters_used INTEGER NOT NULL,
    ph DOUBLE PRECISION,
    dissolved_oxygen_pct DOUBLE PRECISION,
    bod DOUBLE PRECISION,
    ammoniacal_nitrogen DOUBLE PRECISION,
    nitrate DOUBLE PRECISION,
    orthophosphate DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    sample_time TIMESTAMP,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(sampling_point_notation, sample_time)
);
"""

CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_wqi_notation ON wqi_scores(sampling_point_notation);",
    "CREATE INDEX IF NOT EXISTS idx_wqi_category ON wqi_scores(wqi_category);",
    "CREATE INDEX IF NOT EXISTS idx_wqi_region ON wqi_scores(region);",
]

UPSERT_SQL = """
INSERT INTO wqi_scores (
    sampling_point_notation, sampling_point_label, region, area,
    latitude, longitude, wqi_score, wqi_category, parameters_used,
    ph, dissolved_oxygen_pct, bod, ammoniacal_nitrogen,
    nitrate, orthophosphate, temperature, sample_time
) VALUES %s
ON CONFLICT (sampling_point_notation, sample_time)
DO UPDATE SET
    wqi_score = EXCLUDED.wqi_score,
    wqi_category = EXCLUDED.wqi_category,
    parameters_used = EXCLUDED.parameters_used,
    ph = EXCLUDED.ph,
    dissolved_oxygen_pct = EXCLUDED.dissolved_oxygen_pct,
    bod = EXCLUDED.bod,
    ammoniacal_nitrogen = EXCLUDED.ammoniacal_nitrogen,
    nitrate = EXCLUDED.nitrate,
    orthophosphate = EXCLUDED.orthophosphate,
    temperature = EXCLUDED.temperature,
    calculated_at = CURRENT_TIMESTAMP;
"""


def connect_db():
    """Connect to PostgreSQL with retry logic."""
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
            )
            conn.autocommit = False
            print(f"[DB] Connected to PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME}")
            return conn
        except psycopg2.OperationalError as e:
            print(f"[DB] Connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)


def init_db(conn):
    """Create tables and indexes if they don't exist."""
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
        for idx_sql in CREATE_INDEXES_SQL:
            cur.execute(idx_sql)
    conn.commit()
    print("[DB] Tables and indexes initialized.")


# =============================================================================
# WQI CALCULATION
# =============================================================================

def calculate_sub_index(param_key, value):
    """
    Calculate the quality sub-index (qᵢ) for a given parameter (0-100 scale).

    Each parameter uses a tailored scoring approach:
    - pH: Range-based. 6.5-8.5 = 100 (excellent), degrades outside that range.
    - DO (%): ≥100% = 100, degrades linearly to 0 at 0%.
    - Temperature: 10-20°C = 100, degrades outside that range.
    - Pollutants (BOD, NH₃, NO₃, PO₄): 0 = 100, at standard limit = 0.
    """
    config = WQI_PARAMS[param_key]
    ideal = config['ideal']
    standard = config['standard']

    # --- pH: range-based scoring ---
    if param_key == 'ph':
        if 6.5 <= value <= 8.5:
            return 100.0
        elif value < 6.5:
            # Degrades from 100 at 6.5 to 0 at 4.0
            return max(0.0, 100.0 * (1.0 - (6.5 - value) / 2.5))
        else:
            # Degrades from 100 at 8.5 to 0 at 11.0
            return max(0.0, 100.0 * (1.0 - (value - 8.5) / 2.5))

    # --- Dissolved Oxygen (%): higher is better, 100% is ideal ---
    if param_key == 'dissolved_oxygen_pct':
        if value >= 100.0:
            return 100.0
        # Linear degradation: 100% → qi=100, 0% → qi=0
        return max(0.0, min(100.0, value))

    # --- Temperature: comfortable range 10-20°C ---
    if param_key == 'temperature':
        if 10.0 <= value <= 20.0:
            return 100.0
        elif value < 10.0:
            return max(0.0, 100.0 * (1.0 - (10.0 - value) / 10.0))
        else:
            return max(0.0, 100.0 * (1.0 - (value - 20.0) / 15.0))

    # --- Pollutants: lower is better, 0 = ideal, at standard = 0 ---
    denominator = abs(standard - ideal)
    if denominator == 0:
        return 100.0

    deviation = abs(value - ideal) / denominator
    qi = max(0.0, min(100.0, 100.0 * (1.0 - deviation)))

    return qi


def calculate_wqi(params):
    """
    Calculate the Weighted Arithmetic Water Quality Index.

    Args:
        params: dict of {param_key: numeric_value}

    Returns:
        (wqi_score, category, num_params) or None if insufficient parameters
    """
    if len(params) < MIN_PARAMS:
        return None

    weighted_sum = 0.0
    weight_total = 0.0

    for key, value in params.items():
        if key not in WQI_PARAMS:
            continue
        qi = calculate_sub_index(key, value)
        wi = WQI_PARAMS[key]['weight']
        weighted_sum += qi * wi
        weight_total += wi

    if weight_total == 0:
        return None

    wqi_score = weighted_sum / weight_total

    # Classify
    category = 'Very Bad'
    for threshold, label in WQI_CATEGORIES:
        if wqi_score >= threshold:
            category = label
            break

    return wqi_score, category, len(params)


def classify_wqi(score):
    """Classify a WQI score into a category."""
    for threshold, label in WQI_CATEGORIES:
        if score >= threshold:
            return label
    return 'Very Bad'


# =============================================================================
# KAFKA CONSUMER
# =============================================================================

def create_consumer():
    """Create the Kafka consumer with retry logic."""
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=CONSUMER_GROUP,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=1000,  # poll timeout so we can check flush interval
            )
            print(f"[Kafka] Connected to {BOOTSTRAP_SERVERS}, topic: {TOPIC_NAME}")
            return consumer
        except NoBrokersAvailable:
            print("[Kafka] Brokers not available, retrying in 5 seconds...")
            time.sleep(5)


def parse_numeric(value_str):
    """Parse a numeric result, handling '<' prefix (below detection limit)."""
    if not value_str:
        return None
    value_str = str(value_str).strip()

    # Below detection limit: use half the detection limit
    if value_str.startswith('<'):
        try:
            return float(value_str[1:].strip()) / 2.0
        except ValueError:
            return None

    try:
        return float(value_str)
    except ValueError:
        return None


# =============================================================================
# MAIN LOOP
# =============================================================================

def flush_to_db(conn, buffer):
    """
    Calculate WQI for all buffered sampling points and flush to PostgreSQL.

    Args:
        conn: psycopg2 connection
        buffer: dict of {notation: {params: {...}, meta: {...}}}

    Returns:
        (total_calculated, total_skipped)
    """
    if not buffer:
        return 0, 0

    rows = []
    skipped = 0

    for notation, data in buffer.items():
        params = data['params']
        meta = data['meta']

        result = calculate_wqi(params)
        if result is None:
            skipped += 1
            continue

        wqi_score, category, num_params = result

        row = (
            notation,
            meta.get('label', ''),
            meta.get('region', ''),
            meta.get('area', ''),
            meta.get('latitude'),
            meta.get('longitude'),
            round(wqi_score, 2),
            category,
            num_params,
            params.get('ph'),
            params.get('dissolved_oxygen_pct'),
            params.get('bod'),
            params.get('ammoniacal_nitrogen'),
            params.get('nitrate'),
            params.get('orthophosphate'),
            params.get('temperature'),
            meta.get('sample_time'),
        )
        rows.append(row)

    if rows:
        try:
            with conn.cursor() as cur:
                execute_values(cur, UPSERT_SQL, rows)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"[DB] Error flushing {len(rows)} rows: {e}")
            return 0, skipped

    return len(rows), skipped


def run():
    """Main entry point for the WQI consumer."""
    print("=" * 70)
    print("WQI CONSUMER — Water Quality Index Calculator")
    print("=" * 70)
    print(f"  Kafka:          {BOOTSTRAP_SERVERS}")
    print(f"  Topic:          {TOPIC_NAME}")
    print(f"  Consumer Group: {CONSUMER_GROUP}")
    print(f"  PostgreSQL:     {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"  Flush Interval: {FLUSH_INTERVAL}s")
    print(f"  Min Parameters: {MIN_PARAMS}")
    print("=" * 70)

    # Connect to services
    conn = connect_db()
    init_db(conn)
    consumer = create_consumer()

    # In-memory buffer: {sampling_point_notation: {params: {...}, meta: {...}}}
    buffer = {}
    last_flush = time.time()
    total_messages = 0
    total_relevant = 0
    total_flushed = 0

    # Graceful shutdown
    shutdown = False

    def handle_signal(signum, frame):
        nonlocal shutdown
        print("\n[Main] Shutdown signal received...")
        shutdown = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    print("[Main] Consuming messages... Press Ctrl+C to stop.\n")

    try:
        while not shutdown:
            # Poll messages (consumer_timeout_ms=1000 ensures we don't block forever)
            for message in consumer:
                if shutdown:
                    break

                total_messages += 1
                row = message.value

                # Check if this determinand is WQI-relevant
                determinand = row.get('determinand.prefLabel', '')
                if determinand not in DETERMINAND_MAP:
                    continue

                # Parse the numeric result
                value = parse_numeric(row.get('result', ''))
                if value is None:
                    continue

                total_relevant += 1
                param_key = DETERMINAND_MAP[determinand]
                notation = row.get('samplingPoint.notation', 'UNKNOWN')

                # Initialize buffer entry if new sampling point
                if notation not in buffer:
                    buffer[notation] = {
                        'params': {},
                        'meta': {
                            'label': row.get('samplingPoint.prefLabel', ''),
                            'region': row.get('samplingPoint.region', ''),
                            'area': row.get('samplingPoint.area', ''),
                            'latitude': parse_numeric(row.get('samplingPoint.latitude')),
                            'longitude': parse_numeric(row.get('samplingPoint.longitude')),
                            'sample_time': row.get('phenomenonTime', ''),
                        },
                    }

                # Store the latest value for this parameter at this sampling point
                buffer[notation]['params'][param_key] = value

                # Update sample_time to the latest observation
                sample_time = row.get('phenomenonTime', '')
                if sample_time:
                    buffer[notation]['meta']['sample_time'] = sample_time

                # Check if it's time to flush
                elapsed = time.time() - last_flush
                if elapsed >= FLUSH_INTERVAL:
                    flushed, skipped = flush_to_db(conn, buffer)
                    total_flushed += flushed
                    print(
                        f"[Flush] {flushed} WQI scores written, {skipped} skipped "
                        f"(< {MIN_PARAMS} params) | "
                        f"Messages: {total_messages:,} | "
                        f"Relevant: {total_relevant:,} | "
                        f"Total flushed: {total_flushed:,} | "
                        f"Buffer had {len(buffer)} points"
                    )
                    buffer.clear()
                    last_flush = time.time()

            # After consumer timeout (no more messages in this poll cycle),
            # check if we should flush
            elapsed = time.time() - last_flush
            if elapsed >= FLUSH_INTERVAL and buffer:
                flushed, skipped = flush_to_db(conn, buffer)
                total_flushed += flushed
                print(
                    f"[Flush] {flushed} WQI scores written, {skipped} skipped "
                    f"(< {MIN_PARAMS} params) | "
                    f"Messages: {total_messages:,} | "
                    f"Relevant: {total_relevant:,} | "
                    f"Total flushed: {total_flushed:,} | "
                    f"Buffer had {len(buffer)} points"
                )
                buffer.clear()
                last_flush = time.time()

    except Exception as e:
        print(f"\n[Error] {e}")
    finally:
        # Final flush on shutdown
        if buffer:
            print("[Main] Final flush before shutdown...")
            flushed, skipped = flush_to_db(conn, buffer)
            total_flushed += flushed
            print(f"[Main] Final flush: {flushed} scores written, {skipped} skipped.")

        consumer.close()
        conn.close()
        print(f"\n[Main] Shutdown complete. Total WQI scores written: {total_flushed:,}")


if __name__ == "__main__":
    run()
