import os
import pandas as pd
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

load_dotenv()

class DatabaseManager:
    def __init__(self):
        self.host = os.getenv("PG_HOST", "localhost")
        self.port = os.getenv("PG_PORT", "5432")
        self.dbname = os.getenv("POSTGRES_DB", "app_database")
        self.user = os.getenv("POSTGRES_USER", "admin")
        self.password = os.getenv("POSTGRES_PASSWORD", "your_secure_password")

    def _get_connection(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )

    def get_unique_regions(self):
        """Fetch unique regions and their total sample counts."""
        query = """
            SELECT region, SUM(num_samples) as total_samples
            FROM region_daily_averages
            GROUP BY region
            ORDER BY total_samples DESC;
        """
        return self._fetch_as_df(query)

    def get_materials_for_regions(self, regions):
        """Fetch materials for the selected regions."""
        if not regions:
            return pd.DataFrame()
        query = """
            SELECT sample_material_type, SUM(num_samples) as total_samples
            FROM region_daily_averages
            WHERE region IN %s
            GROUP BY sample_material_type
            ORDER BY total_samples DESC;
        """
        return self._fetch_as_df(query, (tuple(regions),))

    def get_determinands_for_filter(self, regions, material):
        """Fetch determinands for the selected regions and material."""
        if not regions or not material:
            return pd.DataFrame()
        query = """
            SELECT determinand_label, SUM(num_samples) as total_samples
            FROM region_daily_averages
            WHERE region IN %s AND sample_material_type = %s
            GROUP BY determinand_label
            ORDER BY total_samples DESC;
        """
        return self._fetch_as_df(query, (tuple(regions), material))

    def get_historical_data(self, regions, material, determinand):
        """Fetch daily averages for specific filters."""
        if not regions or not material or not determinand:
            return pd.DataFrame()
        query = """
            SELECT region, sample_material_type, determinand_label, unit,
                   window_start, window_end, avg_result, std_result, num_samples
            FROM region_daily_averages
            WHERE region IN %s 
              AND sample_material_type = %s 
              AND determinand_label = %s
            ORDER BY window_start;
        """
        return self._fetch_as_df(query, (tuple(regions), material, determinand))

    def get_predictions(self, regions, material, determinand, model_name=None):
        """Fetch predictions for specific filters."""
        if not regions or not material or not determinand:
            return pd.DataFrame()
        
        params = [tuple(regions), material, determinand]
        model_filter = ""
        if model_name:
            model_filter = "AND model_name = %s"
            params.append(model_name)
            
        query = f"""
            SELECT region, sample_material_type, determinand_label, unit,
                   model_name, prediction_date, target_date, predicted_value
            FROM daily_predictions
            WHERE region IN %s 
              AND sample_material_type = %s 
              AND determinand_label = %s
              {model_filter}
            ORDER BY target_date;
        """
        return self._fetch_as_df(query, tuple(params))

    def get_available_models(self):
        """Fetch unique model names."""
        query = "SELECT DISTINCT model_name FROM daily_predictions;"
        df = self._fetch_as_df(query)
        return df['model_name'].tolist() if not df.empty else []

    def get_anomalies(self, start_date=None, end_date=None, material="All", determinand="All"):
        """Fetch anomalies with filters, only including stations with enough history."""
        filters = []
        params = []
        
        # Filter for station-material-determinand triples with at least 3 records in station_daily_averages
        filters.append("""
            (station_id, sample_material_type, determinand_label) IN (
                SELECT station_id, sample_material_type, determinand_label
                FROM station_daily_averages 
                GROUP BY station_id, sample_material_type, determinand_label
                HAVING COUNT(*) >= 3
            )
        """)
        
        if start_date:
            filters.append("window_start::date >= %s")
            params.append(start_date)
        if end_date:
            filters.append("window_start::date <= %s")
            params.append(end_date)
        if material != "All":
            filters.append("sample_material_type = %s")
            params.append(material)
        if determinand != "All":
            filters.append("determinand_label = %s")
            params.append(determinand)
            
        where_clause = "WHERE " + " AND ".join(filters) if filters else ""
        
        query = f"""
            SELECT station_id, station_name, longitude, latitude, sample_material_type,
                   determinand_label, unit, window_start, avg_result, z_score, is_anomaly
            FROM station_anomalies
            {where_clause}
            ORDER BY window_start DESC;
        """
        return self._fetch_as_df(query, tuple(params) if params else None)

    def get_station_history(self, station_id, material, determinand):
        """Fetch daily averages and anomaly status for a specific station."""
        if not station_id or not material or not determinand:
            return pd.DataFrame()
        
        query = """
            SELECT s.window_start, s.avg_result, s.unit, 
                   COALESCE(a.is_anomaly, FALSE) as is_anomaly,
                   a.z_score
            FROM station_daily_averages s
            LEFT JOIN station_anomalies a ON 
                s.station_id = a.station_id AND 
                s.sample_material_type = a.sample_material_type AND 
                s.determinand_label = a.determinand_label AND 
                s.window_start = a.window_start
            WHERE s.station_id = %s 
              AND s.sample_material_type = %s 
              AND s.determinand_label = %s
            ORDER BY s.window_start;
        """
        return self._fetch_as_df(query, (station_id, material, determinand))

    def get_anomaly_metadata(self):
        """Fetch metadata for anomaly filters (dates, materials, determinands)."""
        dates_query = "SELECT MIN(window_start::date), MAX(window_start::date) FROM station_anomalies;"
        materials_query = "SELECT sample_material_type, COUNT(*) FROM station_anomalies GROUP BY sample_material_type ORDER BY COUNT(*) DESC;"
        determinands_query = "SELECT determinand_label, COUNT(*) FROM station_anomalies GROUP BY determinand_label ORDER BY COUNT(*) DESC;"
        
        dates = self._fetch_as_df(dates_query)
        materials = self._fetch_as_df(materials_query)
        determinands = self._fetch_as_df(determinands_query)
        
        return {
            "min_date": dates.iloc[0, 0] if not dates.empty else None,
            "max_date": dates.iloc[0, 1] if not dates.empty else None,
            "materials": materials,
            "determinands": determinands
        }

    def get_gqa_data(self, start_date=None, end_date=None, material="All"):
        """Fetch GQA data with filters."""
        filters = []
        params = []
        
        if start_date:
            filters.append("window_start::date >= %s")
            params.append(start_date)
        if end_date:
            filters.append("window_start::date <= %s")
            params.append(end_date)
        if material != "All":
            filters.append("sample_material_type = %s")
            params.append(material)
            
        where_clause = "WHERE " + " AND ".join(filters) if filters else ""
        
        query = f"""
            SELECT region, sample_material_type, window_start, window_end,
                   gqa_grade, do_value, bod_value, ammonia_value, latitude, longitude
            FROM region_daily_gqa
            {where_clause}
            ORDER BY window_start;
        """
        return self._fetch_as_df(query, tuple(params) if params else None)

    def get_gqa_metadata(self):
        """Fetch metadata for GQA filters."""
        dates_query = "SELECT MIN(window_start::date), MAX(window_start::date) FROM region_daily_gqa;"
        materials_query = "SELECT sample_material_type, COUNT(*) FROM region_daily_gqa GROUP BY sample_material_type ORDER BY COUNT(*) DESC;"
        
        dates = self._fetch_as_df(dates_query)
        materials = self._fetch_as_df(materials_query)
        
        return {
            "min_date": dates.iloc[0, 0] if not dates.empty else None,
            "max_date": dates.iloc[0, 1] if not dates.empty else None,
            "materials": materials
        }

    def get_model_performance_metrics(self, regions, material, determinand, model_name):
        """Fetch joined actual and predicted data to calculate performance metrics (MSE, RMSE, R2)."""
        if not regions or not material or not determinand or not model_name:
            return None
        
        query = """
            WITH latest_predictions AS (
                SELECT DISTINCT ON (region, sample_material_type, determinand_label, model_name, target_date)
                    region, sample_material_type, determinand_label, model_name, target_date, predicted_value
                FROM daily_predictions
                ORDER BY region, sample_material_type, determinand_label, model_name, target_date, prediction_date DESC
            )
            SELECT p.target_date, p.predicted_value, a.avg_result as actual_value
            FROM latest_predictions p
            JOIN region_daily_averages a ON 
                p.region = a.region AND 
                p.sample_material_type = a.sample_material_type AND 
                p.determinand_label = a.determinand_label AND 
                p.target_date = a.window_start
            WHERE p.region IN %s 
              AND p.sample_material_type = %s 
              AND p.determinand_label = %s
              AND p.model_name = %s
            ORDER BY p.target_date;
        """
        df = self._fetch_as_df(query, (tuple(regions), material, determinand, model_name))
        
        if df.empty or len(df) < 2:
            return None
            
        from sklearn.metrics import mean_squared_error, r2_score
        import numpy as np
        
        # Ensure we have numeric types
        actuals = df['actual_value'].astype(float)
        preds = df['predicted_value'].astype(float)
        
        mse = mean_squared_error(actuals, preds)
        rmse = np.sqrt(mse)
        r2 = r2_score(actuals, preds)
        
        return {
            "mse": mse,
            "rmse": rmse,
            "r2": r2,
            "count": len(df)
        }

    def get_overall_model_performance(self):
        """Fetch overall performance metrics for all models and a persistence baseline."""
        # 1. Fetch AI Model Metrics (using latest prediction for each target date)
        query = """
            WITH latest_predictions AS (
                SELECT DISTINCT ON (model_name, region, sample_material_type, determinand_label, target_date)
                    model_name, region, sample_material_type, determinand_label, target_date, predicted_value
                FROM daily_predictions
                ORDER BY model_name, region, sample_material_type, determinand_label, target_date, prediction_date DESC
            )
            SELECT p.model_name, p.predicted_value, a.avg_result as actual_value
            FROM latest_predictions p
            JOIN region_daily_averages a ON 
                p.region = a.region AND 
                p.sample_material_type = a.sample_material_type AND 
                p.determinand_label = a.determinand_label AND 
                p.target_date = a.window_start;
        """
        df = self._fetch_as_df(query)
        
        # 2. Fetch Persistence Baseline Data (Fair comparison: Most Recent available value)
        baseline_query = """
            WITH ai_scope AS (
                SELECT DISTINCT region, sample_material_type, determinand_label, target_date
                FROM daily_predictions
            )
            SELECT p.avg_result as predicted_value, a2.avg_result as actual_value
            FROM ai_scope s
            JOIN region_daily_averages a2 ON 
                s.region = a2.region AND 
                s.sample_material_type = a2.sample_material_type AND 
                s.determinand_label = a2.determinand_label AND 
                s.target_date = a2.window_start
            LEFT JOIN LATERAL (
                SELECT avg_result
                FROM region_daily_averages
                WHERE region = s.region 
                  AND sample_material_type = s.sample_material_type 
                  AND determinand_label = s.determinand_label
                  AND window_start < s.target_date
                ORDER BY window_start DESC
                LIMIT 1
            ) p ON TRUE
            WHERE p.avg_result IS NOT NULL;
        """
        baseline_df = self._fetch_as_df(baseline_query)
        
        from sklearn.metrics import mean_squared_error, r2_score
        import numpy as np
        
        results = []
        
        # Calculate for AI models
        if not df.empty:
            for model in df['model_name'].unique():
                m_df = df[df['model_name'] == model]
                if len(m_df) >= 2:
                    actuals = m_df['actual_value'].astype(float)
                    preds = m_df['predicted_value'].astype(float)
                    mse = mean_squared_error(actuals, preds)
                    results.append({
                        "Model": model,
                        "MSE": mse,
                        "RMSE": np.sqrt(mse),
                        "R2 Score": r2_score(actuals, preds),
                        "Samples": len(m_df)
                    })
        
        # Calculate for Baseline
        if not baseline_df.empty and len(baseline_df) >= 2:
            actuals = baseline_df['actual_value'].astype(float)
            preds = baseline_df['predicted_value'].astype(float)
            mse = mean_squared_error(actuals, preds)
            results.append({
                "Model": "Baseline (Persistence)",
                "MSE": mse,
                "RMSE": np.sqrt(mse),
                "R2 Score": r2_score(actuals, preds),
                "Samples": len(baseline_df)
            })
            
        return pd.DataFrame(results).sort_values("MSE") if results else pd.DataFrame()

    def get_predictable_groups_overall_stats(self):
        """Fetch overall stats for predictable groups vs all groups."""
        query = """
            SELECT 
                COUNT(*) as total_records,
                SUM(CASE WHEN num_samples >= 10 THEN 1 ELSE 0 END) as predictable_records,
                ROUND(100.0 * SUM(CASE WHEN num_samples >= 10 THEN 1 ELSE 0 END) / COUNT(*), 2) as eligibility_rate
            FROM region_daily_averages;
        """
        return self._fetch_as_df(query)

    def get_predictable_groups_regional_stats(self):
        """Fetch total and predictable records per region, ordered by predictable count."""
        query = """
            SELECT 
                region,
                COUNT(*) as total_records,
                SUM(CASE WHEN num_samples >= 10 THEN 1 ELSE 0 END) as predictable_records,
                ROUND(100.0 * SUM(CASE WHEN num_samples >= 10 THEN 1 ELSE 0 END) / COUNT(*), 2) as eligibility_rate
            FROM region_daily_averages
            GROUP BY region
            ORDER BY predictable_records DESC;
        """
        return self._fetch_as_df(query)

    def get_predictable_groups_material_stats(self):
        """Fetch total and predictable records per sample material type."""
        query = """
            SELECT 
                sample_material_type,
                COUNT(*) as total_records,
                SUM(CASE WHEN num_samples >= 10 THEN 1 ELSE 0 END) as predictable_records,
                ROUND(100.0 * SUM(CASE WHEN num_samples >= 10 THEN 1 ELSE 0 END) / COUNT(*), 2) as eligibility_rate
            FROM region_daily_averages
            GROUP BY sample_material_type
            ORDER BY predictable_records DESC;
        """
        return self._fetch_as_df(query)

    def get_predictable_groups_determinand_stats(self):
        """Fetch total and predictable records for the top 15 determinand labels."""
        query = """
            SELECT 
                determinand_label,
                COUNT(*) as total_records,
                SUM(CASE WHEN num_samples >= 10 THEN 1 ELSE 0 END) as predictable_records,
                ROUND(100.0 * SUM(CASE WHEN num_samples >= 10 THEN 1 ELSE 0 END) / COUNT(*), 2) as eligibility_rate
            FROM region_daily_averages
            GROUP BY determinand_label
            ORDER BY predictable_records DESC
            LIMIT 15;
        """
        return self._fetch_as_df(query)

    def get_predictable_groups_sample_distribution(self):
        """Fetch bucketed sample counts for sample size distribution analysis."""
        query = """
            SELECT 
                CASE 
                    WHEN num_samples = 1 THEN '1'
                    WHEN num_samples BETWEEN 2 AND 4 THEN '2-4'
                    WHEN num_samples BETWEEN 5 AND 9 THEN '5-9'
                    WHEN num_samples BETWEEN 10 AND 19 THEN '10-19'
                    WHEN num_samples BETWEEN 20 AND 49 THEN '20-49'
                    ELSE '50+'
                END as sample_bucket,
                COUNT(*) as group_count
            FROM region_daily_averages
            GROUP BY sample_bucket
            ORDER BY 
                CASE 
                    WHEN MIN(num_samples) = 1 THEN 1
                    WHEN MIN(num_samples) = 2 THEN 2
                    WHEN MIN(num_samples) = 5 THEN 3
                    WHEN MIN(num_samples) = 10 THEN 4
                    WHEN MIN(num_samples) = 20 THEN 5
                    ELSE 6
                END;
        """
        return self._fetch_as_df(query)

    def _fetch_as_df(self, query, params=None):
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            cursor.close()
            return df
        except Exception as e:
            print(f"Database error: {e}")
            return pd.DataFrame()
        finally:
            if conn:
                conn.close()
