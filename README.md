# UK Water Quality Real-Time Monitoring & Prediction Pipeline

This project is a big data pipeline that processes UK water quality observation data in real-time. It streams data via Apache Kafka, performs stateful aggregation using PySpark Structured Streaming, executes time-series forecasting using multiple machine learning models, and visualizes the results through an interactive Streamlit dashboard.

## Architecture

1.  **Data Ingestion (`producer.py`)**: Simulates real-time data flow by reading historical water quality observations from a CSV file (`data/observations-2026-4-3-sorted.csv`) and publishing them as JSON messages to the `water-quality-raw` Kafka topic.
2.  **Message Broker (Apache Kafka)**: Managed via Docker Compose. Handles the high-throughput stream of raw observation data.
3.  **Stream Processing & ML (`region_consumer.py`)**: A PySpark Structured Streaming application that:
    *   Subscribes to the `water-quality-raw` Kafka topic.
    *   Parses and cleans the JSON payload, handling numeric conversions (including detection limit cases like `<0.1`).
    *   Performs windowed aggregations (Daily) grouped by **Region**, **Sample Material Type**, and **Determinand**.
    *   Calculates statistical metrics: Average, Standard Deviation, and Sample Count.
    *   **Forecasting Phase**: For groups with sufficient data, it triggers a prediction pipeline using `LinearRegression`, `XGBoost`, `ARIMA`, and `ETS` models to forecast future water quality levels.
4.  **Data Storage (PostgreSQL)**: Stores aggregated daily averages and future predictions in separate tables (`region_daily_averages` and `daily_predictions`).
5.  **Visualization (`app.py`)**: A Streamlit dashboard that allows users to explore regional water quality trends and compare actual data with model predictions.

## Prerequisites

*   **Docker & Docker Compose**: To run Kafka, PostgreSQL, Kafka-UI, and pgAdmin.
*   **Python >= 3.11**
*   **uv**: Fast Python package manager (required for dependency management).
*   **Java (JRE/JDK)**: Required for PySpark.

## Setup Instructions

1.  **Configure Environment**
    Copy the example environment file and adjust the variables if necessary:
    ```powershell
    cp .env.example .env
    ```

2.  **Install Dependencies**
    Use `uv` to create a virtual environment and install all required packages:
    ```powershell
    uv sync
    ```

3.  **Start Infrastructure**
    Launch the required services using Docker Compose:
    ```powershell
    docker-compose up -d
    ```
    This starts:
    *   **Kafka**: `localhost:9092`
    *   **Kafka-UI**: `http://localhost:8080` (Monitor topics and messages)
    *   **PostgreSQL**: `localhost:5432`
    *   **pgAdmin**: `http://localhost:5050` (Database management)

4.  **Initialize/Clear Data (Optional)**
    If you need to reset the database before starting:
    ```powershell
    .venv\Scripts\activate
    python clear_data.py
    ```

## Running the Pipeline

Ensure your virtual environment is active in every terminal: `.venv\Scripts\activate`.

### 1. Start the Data Producer
Publishes raw observations to Kafka:
```powershell
python producer.py
```

### 2. Start the Stream Processor (Consumer)
Processes the stream, calculates daily stats, and generates predictions:
```powershell
python region_consumer.py
```
*Note: This process requires PySpark. Ensure Java is installed and `SPARK_HOME` or path is configured if necessary, though the script handles local execution.*

### 3. Launch the Dashboard
Visualize the processed data and predictions:
```powershell
streamlit run app.py
```

## Additional Tools

*   **`analyze_data.py`**: A standalone script for batch analysis of the raw CSV dataset to understand the distribution of regions, materials, and determinands.
*   **`models/`**: Contains the `WaterQualityPredictor` logic used by the consumer for forecasting.

## Monitoring & Access

*   **Kafka-UI**: `http://localhost:8080`
*   **Streamlit App**: `http://localhost:8501` (by default)
*   **pgAdmin**: `http://localhost:5050`
    *   **Host**: `db` (inside docker)
    *   **Maintenance DB**: `app_database`
    *   **Username**: `admin`
    *   **Password**: See `.env` file
