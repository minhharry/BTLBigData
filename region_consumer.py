"""
Spark Structured Streaming consumer for calculating regional daily averages of water quality.
Aggregates data by region, material type, and determinand on a daily window.
Triggers predictive models for regions with sufficient data.
"""

import os
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, when, avg, expr, stddev, count
)
from pyspark.sql.types import StructType, StructField, StringType
from dotenv import load_dotenv

load_dotenv()

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
    """Create and return a Spark session configured for Kafka and PostgreSQL."""
    return SparkSession.builder \
        .appName("RegionDailyAveragesCalculator") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .getOrCreate()

def init_db():
    """Initialize PostgreSQL tables for regional averages and predictions."""
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
        create_gqa_table_query = """
            CREATE TABLE IF NOT EXISTS region_daily_gqa (
                region VARCHAR(255),
                sample_material_type VARCHAR(255),
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                gqa_grade VARCHAR(2),
                do_value DOUBLE PRECISION,
                bod_value DOUBLE PRECISION,
                ammonia_value DOUBLE PRECISION,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (region, sample_material_type, window_start)
            );
        """
        create_index_perf_query = """
            CREATE INDEX IF NOT EXISTS idx_daily_predictions_model_performance 
            ON daily_predictions (model_name, region, sample_material_type, determinand_label, target_date, prediction_date DESC);
        """
        create_index_scope_query = """
            CREATE INDEX IF NOT EXISTS idx_daily_predictions_scope 
            ON daily_predictions (region, sample_material_type, determinand_label, target_date);
        """
        cursor.execute(create_table_query)
        cursor.execute(create_predictions_table_query)
        cursor.execute(create_gqa_table_query)
        cursor.execute(create_index_perf_query)
        cursor.execute(create_index_scope_query)
        conn.commit()
        cursor.close()
        print("PostgreSQL table initialized successfully.")
    except Exception as e:
        print(f"Failed to initialize PostgreSQL table: {e}")
    finally:
        if conn is not None:
            conn.close()

def process_batch(df, epoch_id):
    """Process a batch of Spark streaming data, removing outliers before calculating averages."""
    batch_df = df.dropna(subset=["samplingPoint_region", "sampleMaterialType", "determinand_prefLabel", "unit", "numeric_result"])
    batch_df = batch_df.withColumn("window", window(col("timestamp"), "1 day"))
    
    window_spec = Window.partitionBy(
        "samplingPoint_region",
        "sampleMaterialType",
        "determinand_prefLabel",
        "unit",
        "window"
    )
    
    batch_with_stats = batch_df \
        .withColumn("group_mean", avg(col("numeric_result")).over(window_spec)) \
        .withColumn("group_std", stddev(col("numeric_result")).over(window_spec))
        
    cleaned_batch = batch_with_stats.filter(
        (col("group_std").isNull()) |
        (col("group_std") == 0.0) |
        (expr("abs((numeric_result - group_mean) / group_std) <= 3.0"))
    )
    
    windowed_df = cleaned_batch.groupBy(
        col("samplingPoint_region"),
        col("sampleMaterialType"),
        col("determinand_prefLabel"),
        col("unit"),
        col("window")
    ).agg(
        avg(col("numeric_result")).alias("avg_result"),
        stddev(col("numeric_result")).alias("std_result"),
        count(col("numeric_result")).alias("num_samples"),
        expr("percentile_approx(numeric_result, 0.1)").alias("p10_result"),
        expr("percentile_approx(numeric_result, 0.9)").alias("p90_result"),
        avg(col("samplingPoint_latitude").cast("double")).alias("avg_lat"),
        avg(col("samplingPoint_longitude").cast("double")).alias("avg_long")
    )
    
    windowed_df = windowed_df.dropna(subset=["samplingPoint_region", "sampleMaterialType", "determinand_prefLabel", "unit", "window", "avg_result"])
    
    flat_df = windowed_df.select(
        col("samplingPoint_region").alias("region"),
        col("sampleMaterialType").alias("sample_material_type"),
        col("determinand_prefLabel").alias("determinand_label"),
        col("unit"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_result"),
        col("std_result"),
        col("num_samples"),
        col("p10_result"),
        col("p90_result"),
        col("avg_lat"),
        col("avg_long")
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

    deduped_records = {}
    for r in records:
        key = (r[0], r[1], r[2], r[4])
        if key not in deduped_records:
            deduped_records[key] = r
        else:
            if r[8] > deduped_records[key][8]:
                deduped_records[key] = r
    records = list(deduped_records.values())

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
    
    # Calculate GQA
    from collections import defaultdict
    gqa_groups = defaultdict(list)
    for row in rows:
        gqa_groups[(row.region, row.sample_material_type, row.window_start, row.window_end)].append(row)
        
    gqa_records = []
    for (region, material_type, w_start, w_end), group_rows in gqa_groups.items():
        do_val = None
        bod_val = None
        amm_val = None
        lats = []
        longs = []
        
        for r in group_rows:
            lats.append(r.avg_lat)
            longs.append(r.avg_long)
            label = str(r.determinand_label)
            if label == "Oxygen, Dissolved, % Saturation":
                do_val = r.p10_result
            elif label == "BOD : 5 Day ATU":
                bod_val = r.p90_result
            elif label == "Ammoniacal Nitrogen as N":
                amm_val = r.p90_result
                
        # Only calculate if all 3 determinands exist
        if do_val is not None and bod_val is not None and amm_val is not None:
            grades = []
            
            # Dissolved Oxygen grade
            if do_val >= 80: grades.append(('A', 1))
            elif do_val >= 70: grades.append(('B', 2))
            elif do_val >= 60: grades.append(('C', 3))
            elif do_val >= 50: grades.append(('D', 4))
            elif do_val >= 20: grades.append(('E', 5))
            else: grades.append(('F', 6))
            
            # BOD grade
            if bod_val <= 2.5: grades.append(('A', 1))
            elif bod_val <= 4: grades.append(('B', 2))
            elif bod_val <= 6: grades.append(('C', 3))
            elif bod_val <= 8: grades.append(('D', 4))
            elif bod_val <= 15: grades.append(('E', 5))
            else: grades.append(('F', 6))
            
            # Ammonia grade
            if amm_val <= 0.25: grades.append(('A', 1))
            elif amm_val <= 0.6: grades.append(('B', 2))
            elif amm_val <= 1.3: grades.append(('C', 3))
            elif amm_val <= 2.5: grades.append(('D', 4))
            elif amm_val <= 9.0: grades.append(('E', 5))
            else: grades.append(('F', 6))
            
            if grades:
                worst_grade = max(grades, key=lambda x: x[1])[0]
                avg_lat = sum(lats) / len(lats) if lats else None
                avg_long = sum(longs) / len(longs) if longs else None
                gqa_records.append((region, material_type, w_start, w_end, worst_grade, do_val, bod_val, amm_val, avg_lat, avg_long))
            
    deduped_gqa = {}
    for r in gqa_records:
        key = (r[0], r[1], r[2])
        if key not in deduped_gqa:
            deduped_gqa[key] = r
        else:
            if r[4] > deduped_gqa[key][4]:
                deduped_gqa[key] = r
    gqa_records = list(deduped_gqa.values())

    if gqa_records:
        gqa_upsert_query = """
            INSERT INTO region_daily_gqa (
                region, sample_material_type, window_start, window_end, gqa_grade, do_value, bod_value, ammonia_value, latitude, longitude
            ) VALUES %s
            ON CONFLICT (region, sample_material_type, window_start) 
            DO UPDATE SET 
                window_end = EXCLUDED.window_end,
                gqa_grade = EXCLUDED.gqa_grade,
                do_value = EXCLUDED.do_value,
                bod_value = EXCLUDED.bod_value,
                ammonia_value = EXCLUDED.ammonia_value,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                updated_at = CURRENT_TIMESTAMP;
        """
        conn = None
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=POSTGRES_DB, 
                user=POSTGRES_USER, password=POSTGRES_PASSWORD
            )
            cursor = conn.cursor()
            execute_values(cursor, gqa_upsert_query, gqa_records)
            conn.commit()
            cursor.close()
            print("Upserted GQA batch")
        except Exception as e:
            print(f"Failed to write GQA batch to PostgreSQL: {e}")
        finally:
            if conn is not None:
                conn.close()

    try:
        from models.predictor import WaterQualityPredictor
        
        predictable_groups = [row for row in rows if row.num_samples >= 10]
        
        if predictable_groups:
            for model_name in ["LinearRegression", "XGBoost", "ARIMA", "ETS"]:
                predictor = WaterQualityPredictor(model_type=model_name)
                predictor.train_and_predict_batch(
                    predictable_groups, PG_HOST, PG_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
                )
    except ImportError as ie:
        print(f"Could not load predictor: {ie}")
    except Exception as e:
        print(f"Error during prediction phase: {e}")

def main():
    """Main execution flow for regional daily averages consumer."""
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
        .option("maxOffsetsPerTrigger", 50000) \
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

    query = df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch) \
        .trigger(processingTime=FLUSH_INTERVAL) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
