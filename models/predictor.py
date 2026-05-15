import psycopg2
from psycopg2.extras import execute_values
from datetime import timedelta
import numpy as np

class WaterQualityPredictor:
    def __init__(self, model_type="LinearRegression"):
        self.model_type = model_type
        
        if self.model_type == "LinearRegression":
            from models.linear_regression import LinearRegressionModel
            self.model_instance = LinearRegressionModel()
        elif self.model_type == "XGBoost":
            from models.xgboost_model import XGBoostModel
            self.model_instance = XGBoostModel()
        elif self.model_type == "ARIMA":
            from models.arima_model import ARIMAModel
            self.model_instance = ARIMAModel()
        elif self.model_type == "ETS":
            from models.ets_model import ETSModel
            self.model_instance = ETSModel()
        else:
            raise ValueError(f"Unknown model_type: {model_type}")

    def train_and_predict_batch(self, groups, host, port, db, user, password):
        conn = None
        try:
            conn = psycopg2.connect(
                host=host, port=port, dbname=db, 
                user=user, password=password
            )
            cursor = conn.cursor()
            
            predictions_to_insert = []
            processed_groups = set()
            
            for row in groups:
                region = row.region
                material = row.sample_material_type
                determinand = row.determinand_label
                unit = row.unit
                
                group_key = (region, material, determinand)
                if group_key in processed_groups:
                    continue
                processed_groups.add(group_key)
                prediction_date = row.window_end # Time when prediction is made
                
                # Find the latest prediction date for this group and model to avoid redundant work
                latest_pred_query = """
                    SELECT MAX(prediction_date)
                    FROM daily_predictions
                    WHERE region=%s AND sample_material_type=%s 
                      AND determinand_label=%s AND model_name=%s
                """
                cursor.execute(latest_pred_query, (region, material, determinand, self.model_type))
                latest_pred_row = cursor.fetchone()
                latest_pred_date = latest_pred_row[0] if latest_pred_row else None

                # Fetch history for this group
                query = """
                    SELECT window_start, avg_result 
                    FROM region_daily_averages 
                    WHERE region=%s AND sample_material_type=%s 
                      AND determinand_label=%s
                      AND avg_result IS NOT NULL
                    ORDER BY window_start ASC
                """
                cursor.execute(query, (region, material, determinand))
                history = cursor.fetchall()
                
                # We need at least 2 points for a simple linear regression
                if len(history) < 2:
                    continue
                
                # Window size is decided by the model
                window_size = self.model_instance.window_size
                
                for i in range(1, len(history)):
                    # Extract the window ending at index i
                    start_idx = max(0, i - window_size + 1)
                    current_window = history[start_idx:i+1]
                    
                    if len(current_window) < 2:
                        continue
                        
                    last_time = current_window[-1][0]
                    window_prediction_date = last_time
                    
                    # Skip if we already made predictions for this or a newer window end date
                    if latest_pred_date and window_prediction_date <= latest_pred_date:
                        continue
                    
                    try:
                        # Delegate training and prediction to the specific model strategy
                        preds = self.model_instance.train_and_predict(current_window)
                        
                        for target_date, predicted_value in preds:
                            predictions_to_insert.append((
                                region, material, determinand, unit,
                                self.model_type, window_prediction_date, target_date, float(predicted_value)
                            ))
                    except Exception as e:
                        # Fail gracefully if model fails to converge for a specific window
                        print(f"Failed to train/predict for {region} - {determinand} at {last_time} with {self.model_type}: {e}")
                        continue
                        
            if predictions_to_insert:
                insert_query = """
                    INSERT INTO daily_predictions (
                        region, sample_material_type, determinand_label, unit,
                        model_name, prediction_date, target_date, predicted_value
                    ) VALUES %s
                    ON CONFLICT (region, sample_material_type, determinand_label, model_name, prediction_date, target_date)
                    DO UPDATE SET
                        unit = EXCLUDED.unit,
                        predicted_value = EXCLUDED.predicted_value;
                """
                execute_values(cursor, insert_query, predictions_to_insert)
                conn.commit()
                print(f"Inserted {len(predictions_to_insert)} future predictions using {self.model_type}.")
                
            cursor.close()
        except Exception as e:
            print(f"Error in prediction batch processing: {e}")
        finally:
            if conn is not None:
                conn.close()
