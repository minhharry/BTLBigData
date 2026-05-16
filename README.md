# UK Water Quality Real-Time Monitoring & Prediction Pipeline

This project is a high-performance big data pipeline designed to process and analyze UK water quality observation data in real-time. It leverages a modern data stack to ingest, process, predict, and visualize water quality trends across England.

## 🚀 System Architecture

The pipeline consists of five main stages:

1.  **Data Ingestion (`producer.py`)**: Simulates a real-time stream by reading historical observations from CSV and publishing JSON messages to the `water-quality-raw` Kafka topic.
2.  **Message Broker (Apache Kafka)**: Acts as the high-throughput backbone, managed via Docker Compose, handling the stream of raw observation data.
3.  **Real-Time Processing (PySpark Structured Streaming)**:
    *   **Regional Consumer (`region_consumer.py`)**: Performs daily aggregations by Region. It calculates statistical metrics and triggers a **Forecasting Pipeline** (Linear Regression, XGBoost, ARIMA, ETS). It also calculates **GQA (General Quality Assessment)** grades based on Dissolved Oxygen, BOD, and Ammonia.
    *   **Station Consumer (`station_consumer.py`)**: Performs granular daily aggregations by Station. It executes **Cross-Sectional Anomaly Detection** using Z-scores to identify outliers in real-time.
4.  **Persistent Storage (PostgreSQL)**: A relational database storing aggregated stats, GQA grades, detected anomalies, and future predictions.
5.  **Interactive Dashboard (`app.py`)**: A Streamlit application providing four distinct views:
    *   **Historical Trends**: Visualize regional levels with integrated AI predictions and performance metrics.
    *   **Anomaly Detection Map**: Geographic visualization of Z-score based anomalies.
    *   **Regional GQA Map**: Map-based assessment of river quality grades (A-F).
    *   **Model Performance**: Comparative analysis of AI models against a Persistence Baseline.

## 🛠️ Prerequisites

*   **Windows 11** (Optimized environment)
*   **Docker & Docker Compose**: For Kafka, PostgreSQL, and monitoring UIs.
*   **Python >= 3.11**
*   **uv**: Fast Python package manager (required for environment management).
*   **Java (JRE/JDK)**: Required for PySpark execution.

## ⚙️ Setup Instructions

1.  **Configure Environment**
    Copy the example environment file:
    ```powershell
    cp .env.example .env
    ```

2.  **Install Dependencies**
    Use `uv` to sync the environment:
    ```powershell
    uv sync
    ```

3.  **Start Infrastructure**
    Launch the core services:
    ```powershell
    docker-compose up -d
    ```
    *   **Kafka-UI**: `http://localhost:8080`
    *   **pgAdmin**: `http://localhost:5050` (DB: `app_database`, User: `admin`)

4.  **Initialize Database**
    (Optional) Clear existing data:
    ```powershell
    .venv\Scripts\activate
    python clear_data.py
    ```

## 🏃 Running the Pipeline

Open four separate terminals and activate the environment: `.venv\Scripts\activate`.

### 1. Start the Data Producer
```powershell
python producer.py
```

### 2. Start the Regional Stream Processor
Handles regional stats, GQA, and AI Predictions:
```powershell
python region_consumer.py
```

### 3. Start the Station Stream Processor
Handles station-level stats and Anomaly Detection:
```powershell
python station_consumer.py
```

### 4. Launch the Dashboard
```powershell
streamlit run app.py
```

## 📊 Feature Highlights

*   **AI Forecasting**: Uses a hybrid approach with `LinearRegression`, `XGBoost`, `ARIMA`, and `ETS` to predict future water quality.
*   **Performance Benchmarking**: Every model is evaluated using MSE, RMSE, and R² against a Persistence Baseline to ensure predictive value.
*   **Real-time GQA Grades**: Automatically classifies water quality from Grade A (Very Good) to Grade F (Bad) using standard environmental metrics.
*   **Spatiotemporal Anomalies**: Detects outliers by comparing station performance against its peers in the same region and time window.

## 📂 Project Structure

*   `models/`: Core prediction logic and model implementations.
*   `static/`: CSS and styling for the dashboard.
*   `db_manager.py`: Centralized database access layer.
*   `analyze_data.py`: Tool for initial CSV dataset exploration.

## 🖥️ Monitoring & Access

*   **Kafka-UI**: [http://localhost:8080](http://localhost:8080) (Monitor Kafka topics and messages)
*   **Streamlit App**: [http://localhost:8501](http://localhost:8501) (Visualization Dashboard)
*   **pgAdmin**: [http://localhost:5050](http://localhost:5050) (Database management)
    *   **Host**: `db` (when connecting from inside Docker) or `localhost` (from host)
    *   **Maintenance DB**: `app_database`
    *   **Username**: `admin`
    *   **Password**: Refer to your `.env` file
