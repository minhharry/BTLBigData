import os
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, when, avg, expr, abs, first
)
from pyspark.sql.types import StructType, StructField, StringType
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "water-quality-raw"
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "app_database")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "your_secure_password")
WQI_FLUSH_INTERVAL = os.getenv("WQI_FLUSH_INTERVAL", "10 minutes")

def get_spark_session():
    # Updated to Spark 4.1.1 as per your configuration
    return SparkSession.builder \
        .appName("WaterQualityIndexCalculator") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .getOrCreate()

def init_db():
    """
    Initializes the PostgreSQL table if it doesn't exist.
    The Primary Key (sampling_point, window_start) remains valid for sliding windows.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=POSTGRES_DB, 
            user=POSTGRES_USER, password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        create_table_query = """
            CREATE TABLE IF NOT EXISTS water_quality_index (
                sampling_point VARCHAR(255),
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                avg_bod DOUBLE PRECISION,
                avg_cod DOUBLE PRECISION,
                avg_phosphorus DOUBLE PRECISION,
                avg_ammonia DOUBLE PRECISION,
                avg_ph DOUBLE PRECISION,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                wqi_score DOUBLE PRECISION,
                wqi_status VARCHAR(50),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (sampling_point, window_start)
            );
        """
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        print("PostgreSQL table initialized successfully.")
    except Exception as e:
        print(f"Failed to initialize PostgreSQL table: {e}")
    finally:
        if conn is not None:
            conn.close()

def process_batch(df, epoch_id):
    """
    Executes an upsert operation into PostgreSQL for each micro-batch.
    """
    flat_df = df.select(
        col("samplingPoint_notation"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_bod"),
        col("avg_cod"),
        col("avg_phosphorus"),
        col("avg_ammonia"),
        col("avg_ph"),
        col("latitude"),
        col("longitude"),
        col("wqi_score"),
        col("wqi_status")
    )

    rows = flat_df.collect()
    if not rows:
        return

    records = [
        (
            row.samplingPoint_notation,
            row.window_start,
            row.window_end,
            row.avg_bod,
            row.avg_cod,
            row.avg_phosphorus,
            row.avg_ammonia,
            row.avg_ph,
            row.latitude,
            row.longitude,
            row.wqi_score,
            row.wqi_status
        )
        for row in rows
    ]

    upsert_query = """
        INSERT INTO water_quality_index (
            sampling_point, window_start, window_end, 
            avg_bod, avg_cod, avg_phosphorus, 
            avg_ammonia, avg_ph, latitude, longitude, wqi_score, wqi_status
        ) VALUES %s
        ON CONFLICT (sampling_point, window_start) 
        DO UPDATE SET 
            window_end = EXCLUDED.window_end,
            avg_bod = EXCLUDED.avg_bod,
            avg_cod = EXCLUDED.avg_cod,
            avg_phosphorus = EXCLUDED.avg_phosphorus,
            avg_ammonia = EXCLUDED.avg_ammonia,
            avg_ph = EXCLUDED.avg_ph,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            wqi_score = EXCLUDED.wqi_score,
            wqi_status = EXCLUDED.wqi_status,
            updated_at = CURRENT_TIMESTAMP;
    """

    conn = None
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=POSTGRES_DB, 
            user=POSTGRES_USER, password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        execute_values(cursor, upsert_query, records)
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"Failed to write batch to PostgreSQL: {e}")
    finally:
        if conn is not None:
            conn.close()

def main():
    init_db()

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("samplingPoint.notation", StringType(), True),
        StructField("samplingPoint.prefLabel", StringType(), True),
        StructField("samplingPoint.longitude", StringType(), True),
        StructField("samplingPoint.latitude", StringType(), True),
        StructField("samplingPoint.region", StringType(), True),
        StructField("samplingPoint.area", StringType(), True),
        StructField("samplingPoint.subArea", StringType(), True),
        StructField("samplingPoint.samplingPointStatus", StringType(), True),
        StructField("samplingPoint.samplingPointType", StringType(), True),
        StructField("phenomenonTime", StringType(), True),
        StructField("samplingPurpose", StringType(), True),
        StructField("sampleMaterialType", StringType(), True),
        StructField("determinand.notation", StringType(), True),
        StructField("determinand.prefLabel", StringType(), True),
        StructField("result", StringType(), True),
        StructField("unit", StringType(), True)
    ])

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    parsed_stream = raw_stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    cleaned_columns = [col(f"`{c}`").alias(c.replace(".", "_")) for c in parsed_stream.columns]
    df = parsed_stream.select(*cleaned_columns)

    df = df.withColumn("numeric_result", expr("TRY_CAST(regexp_replace(result, '[^0-9.]', '') AS DOUBLE)"))
    df = df.withColumn("timestamp", to_timestamp(col("phenomenonTime"), "yyyy-MM-dd HH:mm:ss"))

    # --- THE KEY CHANGE: SLIDING WINDOW ---
    # We use a 3-hour window that slides every 1 hour.
    # This captures measurements across boundary lines.
    windowed_df = df \
        .withWatermark("timestamp", "3 hours") \
        .groupBy(
            col("samplingPoint_notation"),
            window(col("timestamp"), "3 hours", "1 hour")
        ) \
        .agg(
            avg(when(col("determinand_prefLabel") == "BOD : 5 Day ATU", col("numeric_result"))).alias("avg_bod"),
            avg(when(col("determinand_prefLabel") == "Chemical Oxygen Demand :- {COD}", col("numeric_result"))).alias("avg_cod"),
            avg(when(col("determinand_prefLabel") == "Phosphorus, Total as P", col("numeric_result"))).alias("avg_phosphorus"),
            avg(when(col("determinand_prefLabel") == "Ammoniacal Nitrogen as N", col("numeric_result"))).alias("avg_ammonia"),
            avg(when(col("determinand_prefLabel") == "pH", col("numeric_result"))).alias("avg_ph"),
            first(col("samplingPoint_latitude").cast("double")).alias("latitude"),
            first(col("samplingPoint_longitude").cast("double")).alias("longitude")
        )

    robust_df = windowed_df.withColumn(
        "param_count",
        (when(col("avg_bod").isNotNull(), 1).otherwise(0) +
         when(col("avg_cod").isNotNull(), 1).otherwise(0) +
         when(col("avg_phosphorus").isNotNull(), 1).otherwise(0) +
         when(col("avg_ammonia").isNotNull(), 1).otherwise(0) +
         when(col("avg_ph").isNotNull(), 1).otherwise(0))
    ).filter(col("param_count") >= 3)

    scored_df = robust_df.withColumn(
        "wqi_score",
        expr("""
            GREATEST(0, LEAST(100, 
                100 - (COALESCE(avg_bod, 0) * 0.5) 
                    - (COALESCE(avg_cod, 0) * 0.1) 
                    - (COALESCE(avg_phosphorus, 0) * 5)
                    - (COALESCE(avg_ammonia, 0) * 2)
                    - (ABS(COALESCE(avg_ph, 7.0) - 7.0) * 5)
            ))
        """)
    )

    scored_df = scored_df.withColumn(
        "wqi_status",
        when(col("wqi_score") >= 90, "Excellent")
        .when(col("wqi_score") >= 70, "Good")
        .when(col("wqi_score") >= 50, "Fair")
        .when(col("wqi_score") >= 25, "Poor")
        .otherwise("Very Poor")
    )

    query = scored_df.writeStream \
        .outputMode("update") \
        .foreachBatch(process_batch) \
        .trigger(processingTime=WQI_FLUSH_INTERVAL) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()