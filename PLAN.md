# Big Data Project Plan: UK Water Quality Intelligence Platform

## Project Title
**Real-time Water Quality Monitoring & Pollution Intelligence System**
Using UK Environment Agency Open Data (1.8M observations, 741MB)

---

## 1. Problem Statement

The UK Environment Agency collects millions of water quality observations annually across 16,799 sampling points. The raw data is in a **long/narrow format** (one row per measurement), making it difficult to:
- Detect **multi-parameter pollution events** (e.g., high BOD + low dissolved oxygen happening together)
- Identify **sewage treatment works that consistently fail** permit limits
- Track how **PFAS "forever chemicals"** are spreading across regions
- Build a **Water Quality Index (WQI)** that combines multiple parameters into a single health score

This project builds an **end-to-end Big Data pipeline** that ingests, transforms, enriches, and analyzes this data using Kafka, PySpark, PostgreSQL, and Apache Superset.

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCE                              │
│  observations-2026-4-3.csv (741MB, 1.8M rows)                  │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                               │
│  producer.py → Kafka Topic: water-quality-raw                   │
│  (Streams CSV rows as JSON messages, simulates real-time feed)  │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                SPARK PROCESSING LAYER                           │
│                                                                 │
│  Stage 1: Data Cleaning & Type Parsing                          │
│    - Parse '<' detection limits → numeric (half-limit)          │
│    - Parse timestamps, cast types, handle nulls                 │
│    - Filter coded/text results                                  │
│                                                                 │
│  Stage 2: Pivot Wide — Multi-Parameter Site Profiles            │
│    - Pivot long format → wide format per (site, timestamp)      │
│    - One row = one sampling event with ALL parameters as cols   │
│    - Creates feature matrix for ML                              │
│                                                                 │
│  Stage 3: Water Quality Index (WQI) Computation                 │
│    - Sub-index calculation for each of 9 parameters             │
│    - Weighted geometric mean → single WQI score (0-100)         │
│    - Classify: Excellent/Good/Fair/Marginal/Poor                │
│                                                                 │
│  Stage 4: Anomaly Detection (Isolation Forest / Z-Score)        │
│    - Per-site statistical baselines (rolling mean/std)          │
│    - Multi-variate anomaly detection on pivoted data            │
│    - Flag pollution events with severity score                  │
│                                                                 │
│  Stage 5: Sewage Treatment Works (STW) Benchmarking             │
│    - Filter sewage effluent sampling points                     │
│    - Compare BOD/Ammonia/Phosphorus against permit thresholds   │
│    - Rank STWs by compliance rate, compute exceedance stats     │
│    - Identify worst-performing treatment works                  │
│                                                                 │
│  Stage 6: PFAS Spatial Contamination Analysis                   │
│    - Filter 60 PFAS compounds                                  │
│    - Aggregate by GPS grid cells (spatial binning)              │
│    - Compute total PFAS load per grid cell                      │
│    - Identify contamination hotspots by region                  │
│                                                                 │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                   STORAGE LAYER — PostgreSQL                    │
│                                                                 │
│  Spark writes results via JDBC to PostgreSQL tables:            │
│    • wqi_scores        — WQI per site/month + classification    │
│    • anomalies         — Detected pollution events + severity   │
│    • stw_ranking       — Sewage works compliance ranking        │
│    • pfas_hotspots     — PFAS contamination by grid cell        │
│    • monthly_summary   — Regional monthly aggregates            │
│                                                                 │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│              VISUALIZATION LAYER — Apache Superset              │
│                                                                 │
│  Connects to PostgreSQL via SQLAlchemy                          │
│  Dashboards:                                                    │
│    • WQI Regional Heatmap (map + time series)                   │
│    • Anomaly / Pollution Events Timeline                        │
│    • STW Performance Leaderboard (bar chart + table)            │
│    • PFAS Contamination Map (geographic scatter)                │
│    • Monthly Trend Dashboard (multi-line charts)                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Technology Stack Summary

| Layer | Technology | Role |
|---|---|---|
| Ingestion | **Apache Kafka** | Stream CSV data as messages |
| Processing | **Apache Spark (PySpark)** | Clean, pivot, compute WQI, anomaly detection, STW ranking, PFAS analysis |
| Storage | **PostgreSQL** | Persist processed results for querying |
| Visualization | **Apache Superset** | Interactive dashboards, charts, maps |
| Infrastructure | **Docker Compose** | Container orchestration for all services |
| Package Management | **uv** | Python dependency management |

---

## 3. What Makes the Spark Processing Complex

The key challenge is that the **raw data is in long format** — each row is ONE measurement of ONE parameter at ONE site. But meaningful analysis requires the **wide format** — one row per sampling event with ALL parameters as columns.

### The Pivot Problem (why this is a Big Data problem)

```
LONG FORMAT (raw):                 WIDE FORMAT (needed):
site  | time  | param    | value   site  | time  | pH  | BOD  | DO   | NH3  | ...
A     | 10:00 | pH       | 7.8     A     | 10:00 | 7.8 | 2.1  | 95.3 | 0.05 | ...
A     | 10:00 | BOD      | 2.1     B     | 11:00 | 8.1 | 45.0 | 12.5 | 15.3 | ...
A     | 10:00 | DO       | 95.3
A     | 10:00 | NH3      | 0.05
B     | 11:00 | pH       | 8.1
B     | 11:00 | BOD      | 45.0
...
```

This pivot across **1.8M rows × 996 determinands × 16,799 sites** is:
- A **massive shuffle** operation in Spark (data redistribution across partitions)
- Requires **groupBy + pivot** — one of the most expensive Spark operations
- Produces a **sparse matrix** (most sites don't measure all 996 params)
- Needs careful **memory management** and partitioning strategy

### Additional Complex Operations

| Operation | Spark Feature Used | Why It's Complex |
|---|---|---|
| Detection limit parsing (`<X` → `X/2`) | UDF + regex | Mixed-type column, 29% of data affected |
| Pivot long→wide | `groupBy().pivot()` | Massive shuffle, 996 possible columns |
| WQI computation | Custom UDF with sub-index math | Non-linear scaling functions per parameter |
| Anomaly detection | Window functions + MLlib | Per-site rolling statistics, multi-variate |
| STW benchmarking | Multi-level aggregation + ranking | GroupBy site → compare thresholds → rank |
| PFAS spatial binning | Math on lat/lon + aggregation | Grid-cell creation, spatial joins |

---

## 4. Detailed Processing Stages

### Stage 1: Data Cleaning & Type Parsing

**Input:** Raw JSON messages from Kafka (or CSV direct load)
**Output:** Cleaned DataFrame with proper types

Tasks:
- Parse `phenomenonTime` → TimestampType
- Parse `result` column:
  - Numeric (`"7.8"`) → float
  - Below detection limit (`"<0.03"`) → float (half the limit: `0.015`)
  - Text/coded → null (flagged separately)
- Cast `latitude`, `longitude` → DoubleType
- Extract `year`, `month`, `day`, `hour` from timestamp
- Drop unnecessary columns (`id`, `samplingPoint.samplingPointStatus`)

### Stage 2: Pivot to Wide Format

**Input:** Cleaned long-format DataFrame
**Output:** Wide DataFrame — one row per (samplingPoint, phenomenonTime)

Tasks:
- Select top N determinands for pivot (e.g., top 30 by frequency)
- `groupBy('samplingPoint.notation', 'phenomenonTime', 'samplingPoint.region', ...)` 
- `.pivot('determinand.prefLabel')`
- `.agg(first('result_numeric'))`
- Handle column name sanitization (spaces, commas in determinand names)
- Result: ~100K+ rows × 30+ parameter columns

### Stage 3: Water Quality Index (WQI)

**Input:** Wide-format DataFrame with key parameters
**Output:** DataFrame with WQI score + classification per row

WQI Formula (Canadian Council of Ministers of the Environment method):
```
Parameters used (9 total):
  1. Dissolved Oxygen (% saturation)
  2. BOD (5-day ATU)
  3. pH
  4. Temperature
  5. Ammoniacal Nitrogen
  6. Nitrate as N
  7. Orthophosphate as P
  8. Conductivity
  9. Suspended Solids

Each parameter → sub-index score (0-100) via non-linear scaling
WQI = weighted geometric mean of available sub-indices

Classification:
  90-100: Excellent
  70-89:  Good
  50-69:  Fair
  25-49:  Marginal
  0-24:   Poor
```

### Stage 4: Anomaly Detection

**Input:** Wide-format DataFrame + WQI scores
**Output:** Flagged anomalies with severity scores

Approach A — Statistical (Z-Score per site):
- Window function: partition by `samplingPoint.notation`, order by time
- Compute rolling mean/std for each parameter (window of last 6 measurements)  
- Z-score = (current - rolling_mean) / rolling_std
- Multi-parameter anomaly = sum of squared Z-scores > threshold

Approach B — Spark MLlib Isolation Forest (if time permits):
- Use the pivoted wide-format as feature matrix
- Train Isolation Forest model per region
- Score each observation with anomaly probability
- Advantage: captures multi-variate correlations automatically

### Stage 5: Sewage Treatment Works Benchmarking

**Input:** Cleaned DataFrame filtered to sewage effluent types
**Output:** STW performance ranking table

UK Permit Thresholds (typical):
```
BOD (5-day ATU):           ≤ 25 mg/L  (final effluent)
Ammoniacal Nitrogen as N:  ≤ 5 mg/L   (final effluent, most permits)
Phosphorus, Total as P:    ≤ 1 mg/L   (in sensitive areas)
Suspended Solids:          ≤ 30 mg/L
```

Tasks:
- Filter to `samplingPointType LIKE '%SEWAGE%FINAL%TREATED%'`
- Filter to `sampleMaterialType = 'FINAL SEWAGE EFFLUENT'`
- Compare each measurement vs threshold
- Per-STW aggregation:
  - Total samples, exceedance count, exceedance rate
  - Mean/max/p95 for each parameter
  - Months with worst performance
- Rank by composite compliance score
- Identify Top 20 worst-performing STWs

### Stage 6: PFAS Spatial Analysis

**Input:** Cleaned DataFrame filtered to PFAS determinands
**Output:** Spatial contamination grid + hotspot list

Tasks:
- Filter determinands containing `perfluoro|fluorotelomer|sulfonamid`
- Parse detection limits (many PFAS results are `<X`)
- Create spatial grid: round lat/lon to 0.1° cells (~8km resolution)
- Per grid cell:
  - Count of PFAS detections (above limit)
  - Total PFAS concentration (sum of all compounds)
  - Number of distinct PFAS compounds detected
  - Detection rate (detected / total sampled)
- Rank grid cells by PFAS load
- Aggregate by region for regional comparison

---

## 5. Project File Structure

```
btlbigdata/
├── docker-compose.yml          # Kafka + Kafka UI + PostgreSQL + Superset
├── pyproject.toml              # Dependencies (uv)
├── producer.py                 # Kafka producer — streams CSV to topic
├── consumer.py                 # Kafka consumer — basic message reader
├── spark_processing.py         # Main Spark job (all 6 stages)
├── spark_utils.py              # Helper functions (UDFs, WQI calc, etc.)
├── init_db.sql                 # PostgreSQL schema initialization
├── main.ipynb                  # Jupyter notebook for exploration
├── observations-2026-4-3.csv   # Raw data (741MB)
├── PLAN.md                     # This file
└── README.md                   # Project documentation
```

---

## 6. PostgreSQL Schema

### Tables

```sql
-- WQI scores per sampling event
CREATE TABLE wqi_scores (
    id SERIAL PRIMARY KEY,
    sampling_point TEXT NOT NULL,
    site_name TEXT,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    region TEXT,
    area TEXT,
    phenomenon_time TIMESTAMP,
    month TEXT,
    ph DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    bod DOUBLE PRECISION,
    ammoniacal_nitrogen DOUBLE PRECISION,
    dissolved_oxygen_pct DOUBLE PRECISION,
    dissolved_oxygen_mg DOUBLE PRECISION,
    nitrate DOUBLE PRECISION,
    orthophosphate DOUBLE PRECISION,
    conductivity DOUBLE PRECISION,
    suspended_solids DOUBLE PRECISION,
    wqi_score DOUBLE PRECISION,
    wqi_class TEXT
);

-- Detected anomalies / pollution events
CREATE TABLE anomalies (
    id SERIAL PRIMARY KEY,
    sampling_point TEXT NOT NULL,
    site_name TEXT,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    region TEXT,
    phenomenon_time TIMESTAMP,
    parameter TEXT,
    value DOUBLE PRECISION,
    rolling_mean DOUBLE PRECISION,
    rolling_std DOUBLE PRECISION,
    z_score DOUBLE PRECISION,
    severity TEXT
);

-- Sewage Treatment Works performance
CREATE TABLE stw_ranking (
    id SERIAL PRIMARY KEY,
    sampling_point TEXT NOT NULL,
    stw_name TEXT,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    region TEXT,
    area TEXT,
    total_samples INTEGER,
    bod_mean DOUBLE PRECISION,
    bod_max DOUBLE PRECISION,
    bod_exceedance_rate DOUBLE PRECISION,
    ammonia_mean DOUBLE PRECISION,
    ammonia_max DOUBLE PRECISION,
    ammonia_exceedance_rate DOUBLE PRECISION,
    phosphorus_mean DOUBLE PRECISION,
    phosphorus_exceedance_rate DOUBLE PRECISION,
    overall_compliance_rate DOUBLE PRECISION,
    rank INTEGER
);

-- PFAS contamination by spatial grid cell
CREATE TABLE pfas_hotspots (
    id SERIAL PRIMARY KEY,
    grid_lat DOUBLE PRECISION,
    grid_lon DOUBLE PRECISION,
    region TEXT,
    total_observations INTEGER,
    detections_above_limit INTEGER,
    detection_rate DOUBLE PRECISION,
    distinct_compounds INTEGER,
    total_pfas_concentration DOUBLE PRECISION,
    max_single_compound_value DOUBLE PRECISION,
    max_compound_name TEXT
);

-- Monthly regional summary
CREATE TABLE monthly_summary (
    id SERIAL PRIMARY KEY,
    region TEXT,
    month TEXT,
    total_observations INTEGER,
    avg_wqi DOUBLE PRECISION,
    pct_excellent DOUBLE PRECISION,
    pct_good DOUBLE PRECISION,
    pct_fair DOUBLE PRECISION,
    pct_marginal DOUBLE PRECISION,
    pct_poor DOUBLE PRECISION,
    anomaly_count INTEGER,
    worst_site TEXT,
    worst_site_wqi DOUBLE PRECISION
);
```

### Spark → PostgreSQL Write Pattern

```python
# Spark writes results to PostgreSQL via JDBC
df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/waterquality") \
    .option("dbtable", "wqi_scores") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()
```

Note: PySpark needs the PostgreSQL JDBC driver JAR. It will be passed via `--jars` or `spark.jars.packages` config.

---

## 7. Apache Superset Dashboards

### Dashboard 1: Water Quality Overview
- **Map chart**: WQI scores plotted on map using lat/lon, color-coded by classification
- **Time series**: Average WQI by region over months
- **Pie chart**: Distribution of WQI classes (Excellent/Good/Fair/Marginal/Poor)
- **Filter bar**: Region, month, WQI class

### Dashboard 2: Pollution Events
- **Table**: Anomalies sorted by severity, with site name, parameter, value, z-score
- **Bar chart**: Anomaly count by region
- **Time series**: Anomaly frequency over months
- **Map**: Anomaly locations with severity color-coding

### Dashboard 3: Sewage Works Performance
- **Horizontal bar**: Top 20 worst STWs by compliance rate
- **Table**: Full STW ranking with BOD/ammonia/phosphorus stats
- **Big number**: Total exceedances, worst STW name
- **Map**: STW locations colored by compliance rate

### Dashboard 4: PFAS Contamination
- **Scatter map**: PFAS hotspots on map, sized by concentration
- **Bar chart**: PFAS detection rate by region
- **Table**: Top 20 most contaminated grid cells
- **Big number**: Total PFAS detections, number of compounds found

### Dashboard 5: Monthly Trends
- **Multi-line chart**: Key parameters (BOD, ammonia, nitrate, DO) averaged by month
- **Grouped bar**: Regional comparison per month
- **Heatmap table**: Region × Month with color-coded WQI

---

## 8. Docker Compose Services

The `docker-compose.yml` will include these services:

| Service | Image | Port | Purpose |
|---|---|---|---|
| **kafka** | `apache/kafka:latest` | 9092 | Message broker |
| **kafka-ui** | `provectuslabs/kafka-ui:latest` | 8080 | Kafka monitoring UI |
| **postgres** | `postgres:16` | 5432 | Storage for processed results |
| **superset** | `apache/superset:latest` | 8088 | Visualization dashboards |

PostgreSQL config:
- Database: `waterquality`
- User: `postgres` / Password: `postgres`
- Volume: `postgres_data` for persistence
- Init script: `init_db.sql` mounted to `/docker-entrypoint-initdb.d/`

Superset config:
- Admin: `admin` / `admin`
- Metadata DB: SQLite (embedded, simplest setup)
- Data source: PostgreSQL connection configured on first launch

---

## 9. Implementation Order & Milestones

### Phase 1: Infrastructure ✅ (Done)
- [x] Docker Compose with Kafka + Kafka UI
- [x] Basic producer/consumer
- [x] Project setup with uv

### Phase 2: Infrastructure — Storage & Visualization
- [ ] Add PostgreSQL to `docker-compose.yml`
- [ ] Create `init_db.sql` with table schemas
- [ ] Add Apache Superset to `docker-compose.yml`
- [ ] Verify all services start together: `docker compose up -d`

### Phase 3: Producer Enhancement
- [ ] Modify `producer.py` to read CSV and stream rows as JSON to `water-quality-raw` topic
- [ ] Add batch control (configurable rows/second, batch size)
- [ ] Add topic partitioning by region (7 partitions)

### Phase 4: Spark Processing — Core
- [ ] `spark_processing.py` — Stage 1: Data cleaning + type parsing
- [ ] `spark_processing.py` — Stage 2: Pivot long→wide format
- [ ] `spark_utils.py` — UDFs for detection limit parsing
- [ ] Test with subset of data first, then full 1.8M rows

### Phase 5: Spark Processing — Analytics
- [ ] Stage 3: WQI computation + classification
- [ ] Stage 4: Anomaly detection (Z-score approach)
- [ ] Stage 5: STW benchmarking + ranking
- [ ] Stage 6: PFAS spatial analysis

### Phase 6: Spark → PostgreSQL Integration
- [ ] Download PostgreSQL JDBC driver JAR
- [ ] Configure Spark to write to PostgreSQL via JDBC
- [ ] Write all 5 result tables to PostgreSQL
- [ ] Verify data in PostgreSQL: `psql` or pgAdmin

### Phase 7: Kafka → Spark Integration
- [ ] Connect Spark Structured Streaming to Kafka consumer
- [ ] Or: Spark batch reads from Kafka topic
- [ ] End-to-end pipeline: CSV → Kafka → Spark → PostgreSQL

### Phase 8: Superset Dashboards
- [ ] Configure PostgreSQL as data source in Superset
- [ ] Create datasets from each table
- [ ] Build Dashboard 1: Water Quality Overview
- [ ] Build Dashboard 2: Pollution Events
- [ ] Build Dashboard 3: Sewage Works Performance
- [ ] Build Dashboard 4: PFAS Contamination
- [ ] Build Dashboard 5: Monthly Trends

### Phase 9: Documentation & Polish
- [ ] Update README.md with full setup instructions
- [ ] Record demo / take screenshots
- [ ] Prepare presentation materials

---

## 10. Dependencies

### Python (managed by uv)
```toml
[project]
dependencies = [
    "kafka-python>=2.3.0",
    "pyspark>=4.1.1",
    "pandas>=3.0.2",
    "ipykernel>=7.2.0",
]
```

### External
- **PostgreSQL JDBC Driver**: `postgresql-42.7.x.jar` — needed for Spark JDBC writes
  - Download from https://jdbc.postgresql.org/ or use via `spark.jars.packages=org.postgresql:postgresql:42.7.5`
- **Docker**: All infrastructure services (Kafka, PostgreSQL, Superset) run in containers

---

## 11. Key Technical Challenges

| Challenge | Solution |
|---|---|
| Mixed-type `result` column (numeric, `<X`, text) | Custom UDF with regex parsing, returns float or null |
| Pivot with 996 possible columns | Limit to top 30 determinands, use `.pivot()` with explicit values list |
| Sparse data after pivot | Use `coalesce()` to fill nulls, filter rows with minimum parameter coverage |
| WQI requires all 9 parameters | Compute WQI only for rows with ≥5 parameters; use adjusted weights |
| Memory pressure on 741MB CSV | Use Spark partitioning (by region or month), cache wisely |
| PFAS results mostly below detection | Count detection rates, not just concentrations; use binary detected/not-detected |
| Spark → PostgreSQL JDBC driver | Download JAR manually or use `spark.jars.packages` config |
| Superset initial setup complexity | Use simplified single-container setup with `superset init` entrypoint |
| Superset map charts need lat/lon | Ensure all tables include `latitude`/`longitude` or `grid_lat`/`grid_lon` columns |

---

## 12. Expected Outputs & Deliverables

1. **5 Superset Dashboards** — Interactive visualizations connected to PostgreSQL
2. **WQI Map** — All 16,799 sampling points color-coded by water quality class
3. **Anomaly Report** — Top 50 most severe pollution events with map locations
4. **STW Leaderboard** — Sewage works ranked by compliance, worst highlighted
5. **PFAS Hotspot Map** — Geographic scatter of PFAS contamination intensity
6. **Working Pipeline** — End-to-end: CSV → Kafka → Spark → PostgreSQL → Superset
7. **Technical Report** — Architecture, methodology, results (README.md)

---

## 13. How to Run (will be finalized)

```bash
# 1. Start all services (Kafka, PostgreSQL, Superset)
docker compose up -d

# 2. Wait for services to be ready (~30s)
# Kafka UI:  http://localhost:8080
# Superset:  http://localhost:8088 (admin/admin)
# Postgres:  localhost:5432 (postgres/postgres, db: waterquality)

# 3. Activate virtualenv
.venv\Scripts\activate

# 4. Run producer (streams CSV data to Kafka)
python producer.py

# 5. Run Spark processing (reads from Kafka/CSV → processes → writes to PostgreSQL)
python spark_processing.py

# 6. Open Superset dashboards
# Navigate to http://localhost:8088
# Dashboards will be populated with data from PostgreSQL
```
