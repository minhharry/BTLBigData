import os
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, when, avg, expr, stddev, count
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
FLUSH_INTERVAL = os.getenv("REGION_FLUSH_INTERVAL", "10 minutes")
print("FLUSH INTERVAL: ", FLUSH_INTERVAL)

def get_spark_session():
    return SparkSession.builder \
        .appName("RegionDailyAveragesCalculator") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .getOrCreate()

def init_db():
    conn = None
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=POSTGRES_DB, 
            user=POSTGRES_USER, password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        create_table_query = """
            CREATE TABLE IF NOT EXISTS region_daily_averages (
                region VARCHAR(255),
                sample_material_type VARCHAR(255),
                determinand_label VARCHAR(255),
                unit VARCHAR(255),
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                avg_result DOUBLE PRECISION,
                std_result DOUBLE PRECISION,
                num_samples INTEGER,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (region, sample_material_type, determinand_label, window_start)
            );
        """
        create_predictions_table_query = """
            CREATE TABLE IF NOT EXISTS daily_predictions (
                region VARCHAR(255),
                sample_material_type VARCHAR(255),
                determinand_label VARCHAR(255),
                unit VARCHAR(255),
                model_name VARCHAR(255),
                prediction_date TIMESTAMP,
                target_date TIMESTAMP,
                predicted_value DOUBLE PRECISION,
                PRIMARY KEY (region, sample_material_type, determinand_label, model_name, prediction_date, target_date)
            );
        """
        cursor.execute(create_table_query)
        cursor.execute(create_predictions_table_query)
        conn.commit()
        cursor.close()
        print("PostgreSQL table initialized successfully.")
    except Exception as e:
        print(f"Failed to initialize PostgreSQL table: {e}")
    finally:
        if conn is not None:
            conn.close()

def process_batch(df, epoch_id):
    flat_df = df.select(
        col("samplingPoint_region").alias("region"),
        col("sampleMaterialType").alias("sample_material_type"),
        col("determinand_prefLabel").alias("determinand_label"),
        col("unit"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_result"),
        col("std_result"),
        col("num_samples")
    )

    rows = flat_df.collect()
    if not rows:
        print(f"Batch {epoch_id} is empty. No data to process.")
        return

    records = [
        (
            row.region,
            row.sample_material_type,
            row.determinand_label,
            row.unit,
            row.window_start,
            row.window_end,
            row.avg_result,
            row.std_result,
            row.num_samples
        )
        for row in rows
    ]

    upsert_query = """
        INSERT INTO region_daily_averages (
            region, sample_material_type, determinand_label, unit,
            window_start, window_end, avg_result, std_result, num_samples
        ) VALUES %s
        ON CONFLICT (region, sample_material_type, determinand_label, window_start) 
        DO UPDATE SET 
            unit = EXCLUDED.unit,
            window_end = EXCLUDED.window_end,
            avg_result = EXCLUDED.avg_result,
            std_result = EXCLUDED.std_result,
            num_samples = EXCLUDED.num_samples,
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
    print("Upserted batch")
    
    # Run predictions for groups that have num_samples >= 10 in this batch
    try:
        from models.predictor import WaterQualityPredictor
        predictor = WaterQualityPredictor()
        
        predictable_groups = [row for row in rows if row.num_samples >= 10]
        
        if predictable_groups:
            predictor.train_and_predict_batch(
                predictable_groups, PG_HOST, PG_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
            )
    except ImportError as ie:
        print(f"Could not load predictor: {ie}")
    except Exception as e:
        print(f"Error during prediction phase: {e}")

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
        .option("startingOffsets", "earliest") \
        .load()

    parsed_stream = raw_stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    cleaned_columns = [col(f"`{c}`").alias(c.replace(".", "_")) for c in parsed_stream.columns]
    df = parsed_stream.select(*cleaned_columns)
    
    # Extract raw numeric value
    df = df.withColumn("raw_numeric", expr("TRY_CAST(regexp_replace(result, '[^0-9.]', '') AS DOUBLE)"))
    
    # Check if result starts with '<'
    df = df.withColumn("is_less_than", col("result").startswith("<"))
    
    # Calculate processed numeric result by halving it if it started with '<'
    df = df.withColumn("numeric_result", 
                       when(col("is_less_than") & col("raw_numeric").isNotNull(), col("raw_numeric") / 2.0)
                       .otherwise(col("raw_numeric")))
    
    # Parse timestamp
    df = df.withColumn("timestamp", to_timestamp(col("phenomenonTime"), "yyyy-MM-dd HH:mm:ss"))

    # Daily window aggregation
    # Using 1 day window tumbling
    windowed_df = df \
        .withWatermark("timestamp", "1 day") \
        .groupBy(
            col("samplingPoint_region"),
            col("sampleMaterialType"),
            col("determinand_prefLabel"),
            col("unit"),
            window(col("timestamp"), "1 day")
        ) \
        .agg(
            avg(col("numeric_result")).alias("avg_result"),
            stddev(col("numeric_result")).alias("std_result"),
            count(col("numeric_result")).alias("num_samples")
        )
        
    # Drop rows with null key attributes that would fail DB insert
    windowed_df = windowed_df.dropna(subset=["samplingPoint_region", "sampleMaterialType", "determinand_prefLabel", "unit", "window"])

    query = windowed_df.writeStream \
        .outputMode("update") \
        .foreachBatch(process_batch) \
        .trigger(processingTime=FLUSH_INTERVAL) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
