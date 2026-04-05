"""
WQI Consumer — Water Quality Index Calculator (PySpark Version)

Consumes water quality observations from the 'water-quality-raw' Kafka topic
using PySpark Structured Streaming, calculates a Weighted Arithmetic Water Quality 
Index (WQI) per sampling point, and periodically flushes results to PostgreSQL.
"""

import os
import time

import psycopg2
from psycopg2.extras import execute_values

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

# =============================================================================
# CONFIGURATION
# =============================================================================

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = 'water-quality-raw'

# PostgreSQL connection
DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
DB_NAME = os.getenv('POSTGRES_DB', 'app_database')
DB_USER = os.getenv('POSTGRES_USER', 'admin')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'your_secure_password')

# WQI Configuration
FLUSH_INTERVAL_ENV = os.getenv('WQI_FLUSH_INTERVAL', '60') 
if FLUSH_INTERVAL_ENV.isdigit():
    FLUSH_INTERVAL = f"{FLUSH_INTERVAL_ENV} seconds"
else:
    FLUSH_INTERVAL = FLUSH_INTERVAL_ENV

MIN_PARAMS = int(os.getenv('WQI_MIN_PARAMS', '3'))  # minimum parameters to calculate WQI

# =============================================================================
# WQI PARAMETER DEFINITIONS
# =============================================================================

DETERMINAND_MAP = {
    'pH': 'ph',
    'Oxygen, Dissolved, % Saturation': 'dissolved_oxygen_pct',
    'BOD : 5 Day ATU': 'bod',
    'Ammoniacal Nitrogen as N': 'ammoniacal_nitrogen',
    'Nitrate as N': 'nitrate',
    'Orthophosphate, reactive as P': 'orthophosphate',
    'Temperature of Water': 'temperature',
}

WQI_PARAMS = {
    'ph': {'weight': 4, 'ideal': 7.0, 'standard': 8.5, 'type': 'proximity'},
    'dissolved_oxygen_pct': {'weight': 4, 'ideal': 100.0, 'standard': 80.0, 'type': 'proximity'},
    'bod': {'weight': 3, 'ideal': 0.0, 'standard': 6.0, 'type': 'pollutant'},
    'ammoniacal_nitrogen': {'weight': 3, 'ideal': 0.0, 'standard': 1.5, 'type': 'pollutant'},
    'nitrate': {'weight': 2, 'ideal': 0.0, 'standard': 10.0, 'type': 'pollutant'},
    'orthophosphate': {'weight': 1, 'ideal': 0.0, 'standard': 0.1, 'type': 'pollutant'},
    'temperature': {'weight': 1, 'ideal': 15.0, 'standard': 30.0, 'type': 'proximity'},
}

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
            return conn
        except psycopg2.OperationalError as e:
            print(f"[DB] Connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def init_db():
    """Create tables and indexes if they don't exist."""
    print(f"[DB] Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME} to initialize tables...")
    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
        for idx_sql in CREATE_INDEXES_SQL:
            cur.execute(idx_sql)
    conn.commit()
    conn.close()
    print("[DB] Tables and indexes initialized.")


# =============================================================================
# WQI CALCULATION (Exact Python Logic)
# =============================================================================

def calculate_sub_index(param_key, value):
    config = WQI_PARAMS[param_key]
    ideal = config['ideal']
    standard = config['standard']

    if param_key == 'ph':
        if 6.5 <= value <= 8.5: return 100.0
        elif value < 6.5: return max(0.0, 100.0 * (1.0 - (6.5 - value) / 2.5))
        else: return max(0.0, 100.0 * (1.0 - (value - 8.5) / 2.5))

    if param_key == 'dissolved_oxygen_pct':
        if value >= 100.0: return 100.0
        return max(0.0, min(100.0, value))

    if param_key == 'temperature':
        if 10.0 <= value <= 20.0: return 100.0
        elif value < 10.0: return max(0.0, 100.0 * (1.0 - (10.0 - value) / 10.0))
        else: return max(0.0, 100.0 * (1.0 - (value - 20.0) / 15.0))

    denominator = abs(standard - ideal)
    if denominator == 0: return 100.0
    deviation = abs(value - ideal) / denominator
    return max(0.0, min(100.0, 100.0 * (1.0 - deviation)))


def calculate_wqi(params):
    if len(params) < MIN_PARAMS: return None

    weighted_sum = 0.0
    weight_total = 0.0
    for key, value in params.items():
        if key not in WQI_PARAMS: continue
        qi = calculate_sub_index(key, value)
        wi = WQI_PARAMS[key]['weight']
        weighted_sum += qi * wi
        weight_total += wi

    if weight_total == 0: return None
    wqi_score = weighted_sum / weight_total

    category = 'Very Bad'
    for threshold, label in WQI_CATEGORIES:
        if wqi_score >= threshold:
            category = label
            break

    return wqi_score, category, len(params)


# Spark UDF wrappers
@F.udf(returnType=FloatType())
def parse_numeric_udf(val):
    if not val: return None
    val = str(val).strip()
    if val.startswith('<'):
        try: return float(val[1:]) / 2.0
        except: return None
    try: return float(val)
    except: return None

@F.udf(returnType=StringType())
def map_param_key_udf(det):
    return DETERMINAND_MAP.get(det)

@F.udf(returnType=StructType([
    StructField("wqi_score", FloatType()),
    StructField("wqi_category", StringType()),
    StructField("parameters_used", IntegerType())
]))
def calculate_wqi_udf(ph, do, bod, nh3, no3, po4, temp):
    params = {}
    if ph is not None: params['ph'] = ph
    if do is not None: params['dissolved_oxygen_pct'] = do
    if bod is not None: params['bod'] = bod
    if nh3 is not None: params['ammoniacal_nitrogen'] = nh3
    if no3 is not None: params['nitrate'] = no3
    if po4 is not None: params['orthophosphate'] = po4
    if temp is not None: params['temperature'] = temp
    
    res = calculate_wqi(params)
    if not res: return None
    score, category, count = res
    return (float(score), category, count)

# =============================================================================
# WRITING TO POSTGRES
# =============================================================================

def write_partition_to_postgres(iter):
    rows = []
    for row in iter:
        try:
            rows.append((
                row['notation'],
                row['label'],
                row['region'],
                row['area'],
                row['latitude'],
                row['longitude'],
                round(row['wqi_score'], 2) if row['wqi_score'] is not None else 0.0,
                row['wqi_category'] or 'Very Bad',
                row['parameters_used'] or 0,
                row['ph'],
                row['dissolved_oxygen_pct'],
                row['bod'],
                row['ammoniacal_nitrogen'],
                row['nitrate'],
                row['orthophosphate'],
                row['temperature'],
                row['sample_time'],
            ))
        except Exception as e:
            print(f"[Worker] Error formatting row: {e}")
            continue

    if not rows:
        return

    conn = connect_db()
    try:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, rows)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[DB] Error upserting {len(rows)} rows: {e}")
    finally:
        conn.close()

def process_batch(batch_df, batch_id):
    # This runs on the driver for each micro-batch (FLUSH_INTERVAL)
    # The dataframe 'batch_df' has the raw parsed rows.
    
    valid_determinands = list(DETERMINAND_MAP.keys())
    df_filtered = batch_df.filter(F.col("determinand").isin(valid_determinands))
    
    df_clean = df_filtered.withColumn("param_key", map_param_key_udf("determinand")) \
                          .withColumn("numeric_value", parse_numeric_udf("result")) \
                          .withColumn("latitude_val", parse_numeric_udf("latitude")) \
                          .withColumn("longitude_val", parse_numeric_udf("longitude")) \
                          .filter(F.col("numeric_value").isNotNull() & F.col("notation").isNotNull())

    agg_exprs = [
        F.last("prefLabel", ignorenulls=True).alias("label"),
        F.last("region", ignorenulls=True).alias("region"),
        F.last("area", ignorenulls=True).alias("area"),
        F.last("latitude_val", ignorenulls=True).alias("latitude"),
        F.last("longitude_val", ignorenulls=True).alias("longitude"),
        F.max("sample_time").alias("sample_time")
    ]
    
    for param in WQI_PARAMS.keys():
        agg_exprs.append(
            F.last(F.when(F.col("param_key") == param, F.col("numeric_value")), ignorenulls=True).alias(param)
        )
        
    df_agg = df_clean.groupBy("notation").agg(*agg_exprs)
    
    df_wqi = df_agg.withColumn(
        "wqi_res",
        calculate_wqi_udf("ph", "dissolved_oxygen_pct", "bod", "ammoniacal_nitrogen", "nitrate", "orthophosphate", "temperature")
    ).filter(F.col("wqi_res").isNotNull()) \
     .withColumn("wqi_score", F.col("wqi_res.wqi_score")) \
     .withColumn("wqi_category", F.col("wqi_res.wqi_category")) \
     .withColumn("parameters_used", F.col("wqi_res.parameters_used"))

    # Execute the DAG and send partition data to PostgreSQL
    df_wqi.rdd.foreachPartition(write_partition_to_postgres)

# =============================================================================
# MAIN LOOP
# =============================================================================

def run():
    print("=" * 70)
    print("WQI CONSUMER — PySpark Structured Streaming")
    print("=" * 70)
    print(f"  Kafka:          {BOOTSTRAP_SERVERS}")
    print(f"  Topic:          {TOPIC_NAME}")
    print(f"  PostgreSQL:     {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"  Flush Interval: {FLUSH_INTERVAL}")
    print(f"  Min Parameters: {MIN_PARAMS}")
    print("=" * 70)

    # Initialize PostgreSQL Tables
    init_db()

    # Need the spark-sql-kafka driver so we include it as package
    spark = SparkSession.builder \
        .appName("WQIConsumerSpark") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
        
    parsed_stream = raw_stream.withColumn("json_str", F.col("value").cast("string")) \
        .withColumn("json", F.expr("from_json(json_str, 'map<string,string>')")) \
        .selectExpr(
            "json['determinand.prefLabel'] as determinand",
            "json['result'] as result",
            "json['samplingPoint.notation'] as notation",
            "json['samplingPoint.prefLabel'] as prefLabel",
            "json['samplingPoint.region'] as region",
            "json['samplingPoint.area'] as area",
            "json['samplingPoint.latitude'] as latitude",
            "json['samplingPoint.longitude'] as longitude",
            "json['phenomenonTime'] as sample_time"
        )
        
    query = parsed_stream.writeStream \
        .trigger(processingTime=FLUSH_INTERVAL) \
        .foreachBatch(process_batch) \
        .start()

    print("[Main] Streaming job started! Press Ctrl+C to shut down.")
    query.awaitTermination()

if __name__ == "__main__":
    run()
