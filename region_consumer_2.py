"""
Spark Structured Streaming consumer for calculating regional daily averages of water quality.
Adapted for Vietnamese aquaculture dataset (DACN) - computes WQI instead of GQA.
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

# os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"

# Vietnamese QCVN 08-MT:2023 thresholds for aquaculture surface water (Column B1)
# Each parameter: (qi_good, qi_bad, BP_i_lower, BP_i_upper, q_i_lower, q_i_upper)
# WQI sub-index breakpoints for Vietnamese aquaculture standards
WQI_PARAMS = {
    "DO": {
        # DO in mg/L - higher is better
        "breakpoints": [
            (0.0, 2.0, 1, 25),
            (2.0, 4.0, 25, 50),
            (4.0, 6.0, 50, 75),
            (6.0, 8.0, 75, 100),
        ],
        "higher_is_better": True,
    },
    "pH": {
        # pH is optimal in range 6.5-8.5
        "breakpoints": [
            (0.0, 5.5, 1, 25),
            (5.5, 6.0, 25, 50),
            (6.0, 6.5, 50, 75),
            (6.5, 8.5, 75, 100),
            (8.5, 9.0, 75, 50),
            (9.0, 9.5, 50, 25),
            (9.5, 14.0, 25, 1),
        ],
        "higher_is_better": None,  # range-based
    },
    "N-NH4": {
        # Ammoniacal Nitrogen mg/L - lower is better
        "breakpoints": [
            (0.0, 0.1, 100, 75),
            (0.1, 0.2, 75, 50),
            (0.2, 0.5, 50, 25),
            (0.5, 1.0, 25, 1),
        ],
        "higher_is_better": False,
    },
    "N-NO2": {
        # Nitrite as N mg/L - lower is better
        "breakpoints": [
            (0.0, 0.01, 100, 75),
            (0.01, 0.02, 75, 50),
            (0.02, 0.04, 50, 25),
            (0.04, 0.05, 25, 1),
        ],
        "higher_is_better": False,
    },
    "P-PO4": {
        # Orthophosphate as P mg/L - lower is better
        "breakpoints": [
            (0.0, 0.1, 100, 75),
            (0.1, 0.2, 75, 50),
            (0.2, 0.3, 50, 25),
            (0.3, 0.5, 25, 1),
        ],
        "higher_is_better": False,
    },
    "TSS": {
        # Total Suspended Solids mg/L - lower is better
        "breakpoints": [
            (0.0, 20.0, 100, 75),
            (20.0, 30.0, 75, 50),
            (30.0, 50.0, 50, 25),
            (50.0, 100.0, 25, 1),
        ],
        "higher_is_better": False,
    },
    "COD": {
        # COD mg/L - lower is better
        "breakpoints": [
            (0.0, 10.0, 100, 75),
            (10.0, 15.0, 75, 50),
            (15.0, 30.0, 50, 25),
            (30.0, 50.0, 25, 1),
        ],
        "higher_is_better": False,
    },
    "Coliform": {
        # Total Coliform MPN/100mL - lower is better
        "breakpoints": [
            (0.0, 2500.0, 100, 75),
            (2500.0, 5000.0, 75, 50),
            (5000.0, 7500.0, 50, 25),
            (7500.0, 10000.0, 25, 1),
        ],
        "higher_is_better": False,
    },
    "Temperature": {
        # Temperature °C - optimal range
        "breakpoints": [
            (0.0, 15.0, 25, 50),
            (15.0, 20.0, 50, 75),
            (20.0, 30.0, 75, 100),
            (30.0, 35.0, 100, 75),
            (35.0, 40.0, 75, 50),
            (40.0, 50.0, 50, 25),
        ],
        "higher_is_better": None,  # range-based
    },
}

# Map from producer2 determinand labels to WQI parameter keys
DETERMINAND_TO_WQI_KEY = {
    "Oxygen, Dissolved as O2": "DO",
    "pH": "pH",
    "Ammoniacal Nitrogen as N": "N-NH4",
    "Nitrogen, Nitrite as N": "N-NO2",
    "Phosphorus, Orthophosphate as P": "P-PO4",
    "Suspended Solids (Filterable) at 105C": "TSS",
    "COD as O2 : Dichromate Method": "COD",
    "Coliform, Total, Conf by MPN": "Coliform",
    "Temperature": "Temperature",
}


def calculate_sub_index(value, param_key):
    """Calculate the WQI sub-index for a single parameter using linear interpolation.
    
    Uses Vietnamese standard breakpoint tables to convert raw measurement values
    to a 1-100 quality score via piecewise linear interpolation.
    """
    if param_key not in WQI_PARAMS:
        return None
    
    config = WQI_PARAMS[param_key]
    breakpoints = config["breakpoints"]
    
    for bp_low, bp_high, q_low, q_high in breakpoints:
        if bp_low <= value <= bp_high:
            # Linear interpolation within the breakpoint range
            if bp_high == bp_low:
                return q_low
            ratio = (value - bp_low) / (bp_high - bp_low)
            return q_low + ratio * (q_high - q_low)
    
    # Value outside all breakpoint ranges
    # If below lowest breakpoint
    if value < breakpoints[0][0]:
        return breakpoints[0][2]  # Return q_low of first range
    # If above highest breakpoint
    if value > breakpoints[-1][1]:
        return breakpoints[-1][3]  # Return q_high of last range
    
    return None


def calculate_wqi(sub_indices):
    """Calculate overall WQI from sub-indices using the Vietnamese standard formula.
    
    WQI = (1/n) * sum(qi) * 100 / 100
    where qi are the individual sub-index scores (1-100 scale).
    
    The final WQI is the weighted average of available sub-indices.
    """
    if not sub_indices:
        return None, None
    
    # Separate pH sub-index (handled differently in Vietnamese WQI)
    ph_qi = sub_indices.pop("pH", None)
    
    # Group remaining into categories
    # Group 1: DO (dissolved oxygen) - treated specially
    do_qi = sub_indices.pop("DO", None)
    
    # Group 2: All other parameters
    other_qis = list(sub_indices.values())
    
    if not other_qis:
        return None, None
    
    # Vietnamese WQI formula:
    # WQI = WQI_pH * (1/n * sum(qi_other))^(1/1) * WQI_DO^(1/1) / 100
    # Simplified: WQI = (1/n) * sum(all qi) for practical purposes
    
    all_qis = other_qis.copy()
    if do_qi is not None:
        all_qis.append(do_qi)
    if ph_qi is not None:
        all_qis.append(ph_qi)
    
    n = len(all_qis)
    wqi = sum(all_qis) / n
    
    # Classify WQI into quality level
    if wqi >= 91:
        quality = "Rất tốt"       # Very Good
    elif wqi >= 76:
        quality = "Tốt"           # Good
    elif wqi >= 51:
        quality = "Trung bình"    # Average
    elif wqi >= 26:
        quality = "Xấu"          # Bad
    else:
        quality = "Rất xấu"      # Very Bad
    
    return round(wqi, 2), quality


def get_spark_session():
    """Create and return a Spark session configured for Kafka and PostgreSQL."""
    return SparkSession.builder \
        .appName("RegionDailyAveragesCalculator_WQI") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .getOrCreate()

def init_db():
    """Initialize PostgreSQL tables for regional averages and WQI."""
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
        create_wqi_table_query = """
            CREATE TABLE IF NOT EXISTS region_daily_wqi (
                region VARCHAR(255),
                sample_material_type VARCHAR(255),
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                wqi_value DOUBLE PRECISION,
                wqi_quality VARCHAR(50),
                do_value DOUBLE PRECISION,
                ph_value DOUBLE PRECISION,
                nh4_value DOUBLE PRECISION,
                no2_value DOUBLE PRECISION,
                po4_value DOUBLE PRECISION,
                tss_value DOUBLE PRECISION,
                cod_value DOUBLE PRECISION,
                coliform_value DOUBLE PRECISION,
                temperature_value DOUBLE PRECISION,
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
        cursor.execute(create_wqi_table_query)
        cursor.execute(create_index_perf_query)
        cursor.execute(create_index_scope_query)
        conn.commit()
        cursor.close()
        print("PostgreSQL tables initialized successfully (with region_daily_wqi).")
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
    
    # Calculate WQI (instead of GQA)
    from collections import defaultdict
    wqi_groups = defaultdict(list)
    for row in rows:
        wqi_groups[(row.region, row.sample_material_type, row.window_start, row.window_end)].append(row)
        
    wqi_records = []
    for (region, material_type, w_start, w_end), group_rows in wqi_groups.items():
        # Collect parameter values from determinand labels
        param_values = {}
        lats = []
        longs = []
        
        for r in group_rows:
            lats.append(r.avg_lat)
            longs.append(r.avg_long)
            label = str(r.determinand_label)
            
            if label in DETERMINAND_TO_WQI_KEY:
                wqi_key = DETERMINAND_TO_WQI_KEY[label]
                param_values[wqi_key] = r.avg_result
        
        # Calculate sub-indices for all available parameters
        sub_indices = {}
        for param_key, value in param_values.items():
            if value is not None:
                qi = calculate_sub_index(value, param_key)
                if qi is not None:
                    sub_indices[param_key] = qi
        
        # Need at least 3 parameters to calculate a meaningful WQI
        if len(sub_indices) >= 3:
            wqi_value, wqi_quality = calculate_wqi(sub_indices.copy())
            
            if wqi_value is not None:
                avg_lat = sum(lats) / len(lats) if lats else None
                avg_long = sum(longs) / len(longs) if longs else None
                
                wqi_records.append((
                    region,
                    material_type,
                    w_start,
                    w_end,
                    wqi_value,
                    wqi_quality,
                    param_values.get("DO"),
                    param_values.get("pH"),
                    param_values.get("N-NH4"),
                    param_values.get("N-NO2"),
                    param_values.get("P-PO4"),
                    param_values.get("TSS"),
                    param_values.get("COD"),
                    param_values.get("Coliform"),
                    param_values.get("Temperature"),
                    avg_lat,
                    avg_long,
                ))
            
    deduped_wqi = {}
    for r in wqi_records:
        key = (r[0], r[1], r[2])
        if key not in deduped_wqi:
            deduped_wqi[key] = r
        else:
            # Keep the one with higher WQI (better quality)
            if r[4] > deduped_wqi[key][4]:
                deduped_wqi[key] = r
    wqi_records = list(deduped_wqi.values())

    if wqi_records:
        wqi_upsert_query = """
            INSERT INTO region_daily_wqi (
                region, sample_material_type, window_start, window_end, 
                wqi_value, wqi_quality,
                do_value, ph_value, nh4_value, no2_value, po4_value,
                tss_value, cod_value, coliform_value, temperature_value,
                latitude, longitude
            ) VALUES %s
            ON CONFLICT (region, sample_material_type, window_start) 
            DO UPDATE SET 
                window_end = EXCLUDED.window_end,
                wqi_value = EXCLUDED.wqi_value,
                wqi_quality = EXCLUDED.wqi_quality,
                do_value = EXCLUDED.do_value,
                ph_value = EXCLUDED.ph_value,
                nh4_value = EXCLUDED.nh4_value,
                no2_value = EXCLUDED.no2_value,
                po4_value = EXCLUDED.po4_value,
                tss_value = EXCLUDED.tss_value,
                cod_value = EXCLUDED.cod_value,
                coliform_value = EXCLUDED.coliform_value,
                temperature_value = EXCLUDED.temperature_value,
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
            execute_values(cursor, wqi_upsert_query, wqi_records)
            conn.commit()
            cursor.close()
            print(f"Upserted {len(wqi_records)} WQI records")
        except Exception as e:
            print(f"Failed to write WQI batch to PostgreSQL: {e}")
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
    """Main execution flow for regional daily averages consumer with WQI."""
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
        .option("maxOffsetsPerTrigger", 500000) \
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
