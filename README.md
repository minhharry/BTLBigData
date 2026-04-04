# UK Water Quality Intelligence Platform

A real-time Big Data pipeline for monitoring and analyzing water quality across the UK, using Environment Agency Open Data.

## Phase 1 & 2: Ingestion Layer (Current Status)

The ingestion layer simulates real-time data flow by streaming row-level observations from a 741MB CSV file into a Kafka topic.

### Infrastructure
- **Apache Kafka**: Message broker for the raw data stream.
- **Kafka UI**: Monitoring interface available at http://localhost:8080.
- **Docker Compose**: Orchestrates all services.

### Components
- `producer.py`: Streams `observations-2026-4-3-sorted.csv` to the `water-quality-raw` Kafka topic.
  - Efficiently uses `csv.DictReader` for low memory footprint.
  - Simulates real-time ingestion with a configurable delay (default ~100 rows/sec).
  - Handles 1.8M rows of historical data.

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

   python consumer.py # Optional: to verify the data is flowing
   ```

4. **Monitor Feed**:
   Check the [Kafka UI](http://localhost:8080) to verify messages are flowing into the `water-quality-raw` topic.

5. **Access pgAdmin**:
   Check the [pgAdmin](http://localhost:5050) to verify the database.
   
   PgAdmin login:
      Email: admin@example.com
      Password: admin_secret_password
   
   Host name/address: db
   Port: 5432
   Maintenance database: app_database
   Username: admin
   Password: your_secure_password


## Next Steps

- **Spark Processing Layer**: Implement `spark_processing.py` to consume from Kafka, clean data, and perform multi-parameter analysis (WQI calculation, anomaly detection).
- **Storage Layer**: Persist processed results to PostgreSQL.
- **Visualization**: Connect Apache Superset to PostgreSQL for real-time dashboards.
