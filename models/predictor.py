import psycopg2
from psycopg2.extras import execute_values
from datetime import timedelta
import numpy as np

# We import sklearn inside the method to ensure it's available or we can import it at the top
try:
    from sklearn.linear_model import LinearRegression
except ImportError:
    pass # Will handle missing module below if needed

class WaterQualityPredictor:
    def __init__(self, model_type="LinearRegression"):
        self.model_type = model_type

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
                
                # Fetch history for this group
                query = """
                    SELECT window_start, avg_result 
                    FROM region_daily_averages 
                    WHERE region=%s AND sample_material_type=%s 
                      AND determinand_label=%s
                    ORDER BY window_start ASC
                """
                cursor.execute(query, (region, material, determinand))
                history = cursor.fetchall()
                
                # We need at least 2 points for a simple linear regression
                if len(history) < 2:
                    continue
                
                # Sliding window of up to 7 days, predicting the next 3 days
                window_size = 7
                for i in range(1, len(history)):
                    # Extract the window ending at index i
                    start_idx = max(0, i - window_size + 1)
                    current_window = history[start_idx:i+1]
                    
                    if len(current_window) < 2:
                        continue
                        
                    base_time = current_window[0][0]
                    
                    X_train = []
                    y_train = []
                    for rec in current_window:
                        day_index = (rec[0] - base_time).total_seconds() / 86400.0
                        X_train.append([day_index])
                        y_train.append(rec[1])
                    
                    if self.model_type == "LinearRegression":
                        try:
                            # Train using scikit-learn
                            model = LinearRegression()
                            model.fit(X_train, y_train)
                            
                            # Predict next 3 days
                            last_time = current_window[-1][0]
                            # Use the last time in the window as the prediction_date
                            window_prediction_date = last_time
                            
                            X_future = []
                            future_dates = []
                            for j in range(1, 4): # Next 1 to 3 days
                                target_time = last_time + timedelta(days=j)
                                target_index = (target_time - base_time).total_seconds() / 86400.0
                                X_future.append([target_index])
                                future_dates.append(target_time)
                                
                            preds = model.predict(X_future)
                            
                            for target_date, predicted_value in zip(future_dates, preds):
                                predictions_to_insert.append((
                                    region, material, determinand, unit,
                                    self.model_type, window_prediction_date, target_date, float(predicted_value)
                                ))
                        except Exception as e:
                            print(f"Failed to train/predict for {region} - {determinand} at {last_time}: {e}")
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
                print(f"Inserted {len(predictions_to_insert)} future predictions using sklearn.")
                
            cursor.close()
        except Exception as e:
            print(f"Error in prediction batch processing: {e}")
        finally:
            if conn is not None:
                conn.close()
