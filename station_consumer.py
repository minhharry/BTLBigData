"""
Spark Structured Streaming consumer for calculating station-level daily averages and detecting anomalies.
Aggregates data by station, material type, and determinand on a daily window.
Calculates Z-scores for cross-sectional anomaly detection within each daily window.
"""

import os
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, when, avg, expr, stddev, count
)
from pyspark.sql.types import StructType, StructField, StringType
from dotenv import load_dotenv
import pandas as pd
import numpy as np

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "water-quality-raw"
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "app_database")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "your_secure_password")
FLUSH_INTERVAL = os.getenv("STATION_FLUSH_INTERVAL", "10 minutes")

print("STATION FLUSH INTERVAL: ", FLUSH_INTERVAL)

def get_spark_session():
    """Create and return a Spark session configured for Kafka and PostgreSQL."""
    return SparkSession.builder \
        .appName("StationDailyAveragesCalculator") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .getOrCreate()

def init_db():
    """Initialize PostgreSQL tables for station daily averages and anomalies."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=POSTGRES_DB, 
            user=POSTGRES_USER, password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        create_table_query = """
            CREATE TABLE IF NOT EXISTS station_daily_averages (
                station_id VARCHAR(255),
                station_name VARCHAR(255),
                longitude DOUBLE PRECISION,
                latitude DOUBLE PRECISION,
                sample_material_type VARCHAR(255),
                determinand_label VARCHAR(255),
                unit VARCHAR(255),
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                avg_result DOUBLE PRECISION,
                std_result DOUBLE PRECISION,
                num_samples INTEGER,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (station_id, sample_material_type, determinand_label, window_start)
            );
        """
        create_anomalies_table_query = """
            CREATE TABLE IF NOT EXISTS station_anomalies (
                station_id VARCHAR(255),
                station_name VARCHAR(255),
                longitude DOUBLE PRECISION,
                latitude DOUBLE PRECISION,
                sample_material_type VARCHAR(255),
                determinand_label VARCHAR(255),
                unit VARCHAR(255),
                window_start TIMESTAMP,
                avg_result DOUBLE PRECISION,
                z_score DOUBLE PRECISION,
                is_anomaly BOOLEAN,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (station_id, sample_material_type, determinand_label, window_start)
            );
        """
        cursor.execute(create_table_query)
        cursor.execute(create_anomalies_table_query)
        conn.commit()
        cursor.close()
        print("PostgreSQL tables 'station_daily_averages' and 'station_anomalies' initialized successfully.")
    except Exception as e:
        print(f"Failed to initialize PostgreSQL table: {e}")
    finally:
        if conn is not None:
            conn.close()

def process_batch(df, epoch_id):
    """Process a batch of Spark streaming data, upserting to DB and detecting anomalies."""
    flat_df = df.select(
        col("samplingPoint_notation").alias("station_id"),
        col("samplingPoint_prefLabel").alias("station_name"),
        col("samplingPoint_longitude").cast("double").alias("longitude"),
        col("samplingPoint_latitude").cast("double").alias("latitude"),
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
            row.station_id,
            row.station_name,
            row.longitude,
            row.latitude,
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
        INSERT INTO station_daily_averages (
            station_id, station_name, longitude, latitude, sample_material_type, determinand_label, unit,
            window_start, window_end, avg_result, std_result, num_samples
        ) VALUES %s
        ON CONFLICT (station_id, sample_material_type, determinand_label, window_start) 
        DO UPDATE SET 
            station_name = EXCLUDED.station_name,
            longitude = EXCLUDED.longitude,
            latitude = EXCLUDED.latitude,
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

    batch_df = pd.DataFrame(records, columns=[
        "station_id", "station_name", "longitude", "latitude", "sample_material_type",
        "determinand_label", "unit", "window_start", "window_end", "avg_result", "std_result", "num_samples"
    ])

    if not batch_df.empty:
        group_cols = ['sample_material_type', 'determinand_label', 'window_start']
        
        batch_df['group_mean'] = batch_df.groupby(group_cols)['avg_result'].transform('mean')
        batch_df['group_std'] = batch_df.groupby(group_cols)['avg_result'].transform('std')
        
        batch_df['z_score'] = 0.0
        mask = (batch_df['group_std'] > 0) & (batch_df['group_std'].notna())
        batch_df.loc[mask, 'z_score'] = (batch_df.loc[mask, 'avg_result'] - batch_df.loc[mask, 'group_mean']) / batch_df.loc[mask, 'group_std']
        
        batch_df['is_anomaly'] = batch_df['z_score'].abs() > 3
        
        anomaly_df = batch_df[batch_df['is_anomaly']]
        
        if not anomaly_df.empty:
            anomaly_records = []
            for _, row in anomaly_df.iterrows():
                anomaly_records.append((
                    str(row['station_id']),
                    str(row['station_name']),
                    float(row['longitude']),
                    float(row['latitude']),
                    str(row['sample_material_type']),
                    str(row['determinand_label']),
                    str(row['unit']),
                    row['window_start'],
                    float(row['avg_result']),
                    float(row['z_score']),
                    bool(row['is_anomaly'])
                ))

            upsert_anomaly_query = """
                INSERT INTO station_anomalies (
                    station_id, station_name, longitude, latitude, sample_material_type,
                    determinand_label, unit, window_start, avg_result, z_score, is_anomaly
                ) VALUES %s
                ON CONFLICT (station_id, sample_material_type, determinand_label, window_start)
                DO UPDATE SET
                    station_name = EXCLUDED.station_name,
                    longitude = EXCLUDED.longitude,
                    latitude = EXCLUDED.latitude,
                    unit = EXCLUDED.unit,
                    avg_result = EXCLUDED.avg_result,
                    z_score = EXCLUDED.z_score,
                    is_anomaly = EXCLUDED.is_anomaly,
                    updated_at = CURRENT_TIMESTAMP;
            """

            try:
                conn = psycopg2.connect(
                    host=PG_HOST, port=PG_PORT, dbname=POSTGRES_DB, 
                    user=POSTGRES_USER, password=POSTGRES_PASSWORD
                )
                cursor = conn.cursor()
                execute_values(cursor, upsert_anomaly_query, anomaly_records)
                conn.commit()
                cursor.close()
                print(f"Upserted {len(anomaly_records)} anomalies to station_anomalies")
            except Exception as e:
                print(f"Failed to write anomalies to PostgreSQL: {e}")
            finally:
                if conn is not None:
                    conn.close()

    print(f"Upserted batch {epoch_id} with {len(records)} records to station_daily_averages")

def main():
    """Main execution flow for station daily averages and anomaly detection consumer."""
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
    
    df = df.withColumn("raw_numeric", expr("TRY_CAST(regexp_replace(result, '[^0-9.]', '') AS DOUBLE)"))
    
    df = df.withColumn("is_less_than", col("result").startswith("<"))
    df = df.withColumn("is_greater_than", col("result").startswith(">"))
    
    df = df.withColumn("numeric_result", 
                       when(col("is_less_than") & col("raw_numeric").isNotNull(), col("raw_numeric") / 2.0)
                       .when(col("is_greater_than") & col("raw_numeric").isNotNull(), col("raw_numeric"))
                       .otherwise(col("raw_numeric")))
    
    df = df.withColumn("timestamp", to_timestamp(col("phenomenonTime"), "yyyy-MM-dd HH:mm:ss"))

    windowed_df = df \
        .withWatermark("timestamp", "1 day") \
        .groupBy(
            col("samplingPoint_notation"),
            col("samplingPoint_prefLabel"),
            col("samplingPoint_longitude"),
            col("samplingPoint_latitude"),
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
        
    windowed_df = windowed_df.dropna(subset=["samplingPoint_notation", "sampleMaterialType", "determinand_prefLabel", "unit", "window", "avg_result"])

    query = windowed_df.writeStream \
        .outputMode("update") \
        .foreachBatch(process_batch) \
        .trigger(processingTime=FLUSH_INTERVAL) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
