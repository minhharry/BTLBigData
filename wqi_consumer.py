import os
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, when, avg, expr
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
    return SparkSession.builder \
        .appName("WaterQualityIndexCalculator") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .getOrCreate()

def process_batch(df, epoch_id):
    """
    Executes an upsert operation into PostgreSQL for each micro-batch.
    """
    # 1. Flatten the nested 'window' struct into standard columns to avoid conversion crashes
    flat_df = df.select(
        col("samplingPoint_notation"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_bod"),
        col("avg_cod"),
        col("avg_phosphorus"),
        col("wqi_score")
    )

    # 2. Use Spark's native collect() instead of toPandas()
    # This returns a list of Spark Row objects, which are completely safe to iterate over
    rows = flat_df.collect()
    if not rows:
        return

    # 3. Prepare data tuples for the upsert
    records = [
        (
            row.samplingPoint_notation,
            row.window_start,
            row.window_end,
            row.avg_bod,
            row.avg_cod,
            row.avg_phosphorus,
            row.wqi_score
        )
        for row in rows
    ]

    upsert_query = """
        INSERT INTO water_quality_index (
            sampling_point, window_start, window_end, 
            avg_bod, avg_cod, avg_phosphorus, wqi_score
        ) VALUES %s
        ON CONFLICT (sampling_point, window_start) 
        DO UPDATE SET 
            window_end = EXCLUDED.window_end,
            avg_bod = EXCLUDED.avg_bod,
            avg_cod = EXCLUDED.avg_cod,
            avg_phosphorus = EXCLUDED.avg_phosphorus,
            wqi_score = EXCLUDED.wqi_score,
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
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Define the exact schema from the CSV producer
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

    # 2. Read Stream from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 3. Parse JSON and clean column names (replacing dots with underscores for Spark SQL safety)
    parsed_stream = raw_stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    cleaned_columns = [col(f"`{c}`").alias(c.replace(".", "_")) for c in parsed_stream.columns]
    df = parsed_stream.select(*cleaned_columns)

    # 4. Clean Numeric Data and Cast Timestamps
    # Extract numbers and use TRY_CAST to safely return null for empty or invalid strings
    df = df.withColumn("numeric_result", expr("TRY_CAST(regexp_replace(result, '[^0-9.]', '') AS DOUBLE)"))
    df = df.withColumn("timestamp", to_timestamp(col("phenomenonTime"), "yyyy-MM-dd HH:mm:ss"))

    # 5. Apply Watermarking and 3-Hour Time Window
    # The watermark ensures we keep state for late data up to 3 hours past the max observed event time
    windowed_df = df \
        .withWatermark("timestamp", "3 hours") \
        .groupBy(
            col("samplingPoint_notation"),
            window(col("timestamp"), "3 hours")
        ) \
        .agg(
            avg(when(col("determinand_prefLabel") == "BOD : 5 Day ATU", col("numeric_result"))).alias("avg_bod"),
            avg(when(col("determinand_prefLabel") == "Chemical Oxygen Demand :- {COD}", col("numeric_result"))).alias("avg_cod"),
            avg(when(col("determinand_prefLabel") == "Phosphorus, Total as P", col("numeric_result"))).alias("avg_phosphorus")
        )

    # 6. Calculate a Generalized Water Quality Index (WQI)
    # Note: Replace this formula with your specific regulatory calculation
    # This sample uses a 100-point scale, deducting points for high contamination levels
    scored_df = windowed_df.withColumn(
        "wqi_score",
        expr("""
            GREATEST(0, LEAST(100, 
                100 - (COALESCE(avg_bod, 0) * 0.5) 
                    - (COALESCE(avg_cod, 0) * 0.1) 
                    - (COALESCE(avg_phosphorus, 0) * 5)
            ))
        """)
    )

    # 7. Write Stream via ForeachBatch
    query = scored_df.writeStream \
        .outputMode("update") \
        .foreachBatch(process_batch) \
        .trigger(processingTime=WQI_FLUSH_INTERVAL) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()