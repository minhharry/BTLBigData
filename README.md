# Water Quality Index (WQI) Data Pipeline

This project is a big data pipeline that calculates the Water Quality Index (WQI) from raw water quality observations. It streams sensor/observation data via Apache Kafka, processes and enriches it in real-time using PySpark Structured Streaming, and stores the resulting WQI scores into a PostgreSQL database for analysis.

## Architecture

1. **Data Ingestion (`producer.py`)**: Simulating realtime ingestion by reads raw water quality observation data from a CSV file (`data/observations-2026-4-3-sorted.csv`) and publishes each observation as a JSON message to an Apache Kafka topic (`water-quality-raw`).
2. **Message Broker (Apache Kafka)**: Handled via Docker Compose. Brokers the real-time messages. Kafka-UI is included for monitoring topics, partitions, and messages.
3. **Stream Processing (`wqi_consumer.py`)**: A PySpark Structured Streaming application. 
   - Subscribes to the `water-quality-raw` Kafka topic.
   - Cleans, parses, and filters raw sensor data.
   - Aggregates readings by sampling point.
   - Calculates the Water Quality Index (WQI)
4. **Data Storage (PostgreSQL)**: The processed WQI scores and localized categories (e.g., Excellent, Good, Bad) are upserted into a PostgreSQL database (`wqi_scores` table). PgAdmin is deployed alongside the database for UI-based querying and administration.

## Prerequisites

- **Docker & Docker Compose**: Needed to run Apache Kafka, PostgreSQL, Kafka-UI, and pgAdmin.
- **Python >= 3.13**
- **uv**: Recommended fast package manager for Python dependency management.

## Setup Instructions
1. **Configure Environment**
   Copy the example environment file and adjust the variables as needed:
   ```bash
   cp .env.example .env
   ```
2. **Install Dependencies**
   The project uses `uv` for dependency management. Create a virtual environment and synchronize the dependencies:
   ```bash
   uv venv
   .venv\Scripts\activate
   uv sync
   ```   

3. **Start the Infrastructure**
   Run the following command to spin up the required Docker containers:
   ```bash
   docker-compose up -d
   ```
   This starts:
   - Kafka on port `9092`
   - Kafka-UI at `http://localhost:8080`
   - PostgreSQL on port `5432`
   - pgAdmin at `http://localhost:5050`


## Running the Pipeline

Ensure that your local virtual environment is active in any terminal before running the Python scripts (`.venv\Scripts\activate`).

### 1. Start the Data Producer
Start the producer script to begin streaming the raw CSV observation data into Kafka:
```bash
.venv\Scripts\activate
python producer.py
```
*Note: This process runs continuously. Keep this running in its own terminal.*

### 2. Start the Stream Processor (Consumer)
In a separate terminal (with the `.venv` activated), Run the PySpark consumer to listen for incoming messages, calculate the WQI, and write the grouped results to PostgreSQL:
```bash
python wqi_consumer.py
```
*Note: This process runs continuously. Keep this running in its own terminal.*

### 3. Verification & Analysis
Once data is streaming, you can:
- Verify message production at **Kafka-UI**: `http://localhost:8080`
- Query the `wqi_scores` output table using **PgAdmin**: `http://localhost:5050`
   - Host name: `db`
   - Port: `5432`
   - Database: `app_database` (in .env file)
   - User: `admin` (in .env file)
   - Password: `your_secure_password` (in .env file)