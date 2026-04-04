# UK Water Quality Intelligence Platform

A real-time Big Data pipeline for monitoring and analyzing water quality across the UK, using Environment Agency Open Data.

## Phase 1 & 2: Ingestion Layer

The ingestion layer simulates real-time data flow by streaming row-level observations from a 741MB CSV file into a Kafka topic.

### Infrastructure
- **Apache Kafka**: Message broker for the raw data stream.
- **Kafka UI**: Monitoring interface available at http://localhost:8080.
- **PostgreSQL**: Stores computed Water Quality Index (WQI) scores.
- **pgAdmin**: Database management UI at http://localhost:5050.
- **Docker Compose**: Orchestrates all services.

### Components
- `producer.py`: Streams `observations-2026-4-3-sorted.csv` to the `water-quality-raw` Kafka topic.
  - Efficiently uses `csv.DictReader` for low memory footprint.
  - Simulates real-time ingestion with a configurable delay (default ~100 rows/sec).
  - Handles 1.8M rows of historical data.

## Phase 3: WQI Processing Layer

The WQI consumer calculates a **Weighted Arithmetic Water Quality Index** per sampling point from incoming Kafka messages, then periodically batch-upserts results to PostgreSQL.

### WQI Calculation Method

Uses the **Weighted Arithmetic Index** (NSF-WQI inspired) with 7 parameters:

| Parameter | Weight | Ideal | Standard Limit |
|---|---|---|---|
| pH | 4 | 7.0 | 8.5 |
| Dissolved Oxygen (% sat) | 4 | 100% | 80% min |
| BOD (5-day ATU) | 3 | 0 mg/L | 6.0 mg/L |
| Ammoniacal Nitrogen | 3 | 0 mg/L | 1.5 mg/L |
| Nitrate as N | 2 | 0 mg/L | 10.0 mg/L |
| Orthophosphate | 1 | 0 mg/L | 0.1 mg/L |
| Temperature | 1 | 15°C | 30°C |

**WQI Classification:**
- 91–100: Excellent
- 71–90: Good
- 51–70: Moderate
- 26–50: Bad
- 0–25: Very Bad

### Components
- `wqi_consumer.py`: Kafka consumer that:
  - Filters for WQI-relevant determinands from the raw stream.
  - Accumulates parameter values per sampling point in memory.
  - Calculates WQI when ≥ `WQI_MIN_PARAMS` parameters are available.
  - Batch-upserts to PostgreSQL every `WQI_FLUSH_INTERVAL` seconds.
  - Auto-creates the `wqi_scores` table on startup.

## How to Run

1. **Start Infrastructure**:
   ```bash
   cp .env.example .env

   # Edit .env file to change the default values if needed

   docker compose up -d
   ```

2. **Activate Environment**:
   ```bash
   .venv\Scripts\activate
   ```

3. **Start Ingestion Simulation**:
   ```bash
   python producer.py
   ```

4. **Start WQI Consumer** (in a separate terminal):
   ```bash
   .venv\Scripts\activate
   python wqi_consumer.py
   ```

5. **Monitor Feed**:
   Check the [Kafka UI](http://localhost:8080) to verify messages are flowing into the `water-quality-raw` topic.

6. **Verify WQI Results**:
   Open [pgAdmin](http://localhost:5050) and run:
   ```sql
   SELECT sampling_point_notation, sampling_point_label, region,
          wqi_score, wqi_category, parameters_used, sample_time
   FROM wqi_scores
   ORDER BY calculated_at DESC
   LIMIT 20;
   ```

7. **Access pgAdmin**:
   Check the [pgAdmin](http://localhost:5050) to verify the database.
   
   PgAdmin login:
      Email: admin@example.com
      Password: admin_secret_password
   
   Host name/address: db
   Port: 5432
   Maintenance database: app_database
   Username: admin
   Password: your_secure_password

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_USER` | `admin` | PostgreSQL username |
| `POSTGRES_PASSWORD` | `your_secure_password` | PostgreSQL password |
| `POSTGRES_DB` | `app_database` | PostgreSQL database name |
| `WQI_FLUSH_INTERVAL` | `60` | Seconds between batch writes to PostgreSQL |
| `WQI_MIN_PARAMS` | `3` | Minimum WQI parameters needed to calculate a score |
