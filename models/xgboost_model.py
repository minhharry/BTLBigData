from datetime import timedelta

class XGBoostModel:
    """XGBoost-based time series forecasting model with lag features."""

    def __init__(self):
        """Initialize the model with a defined window size and lag steps."""
        self.window_size = 21  # Days to look back for training
        self.lags = [1, 2, 3, 7]  # Lag features to include

    def train_and_predict(self, current_window):
        """
        Train the model on historical data and predict the next 3 days using recursive lags.
        
        Args:
            current_window: List of (timestamp, value) tuples.
            
        Returns:
            List of (future_timestamp, predicted_value) tuples.
        """
        import xgboost as xgb
        
        base_time = current_window[0][0]
        max_lag = max(self.lags)
        
        # Ensure we have enough data for the specified lags
        if len(current_window) <= max_lag:
            # Fallback to simple index-based features if window is too small
            X_train = [[(rec[0] - base_time).total_seconds() / 86400.0] for rec in current_window]
            y_train = [rec[1] for rec in current_window]
            model = xgb.XGBRegressor(n_estimators=50, max_depth=3)
            model.fit(X_train, y_train)
            
            last_time = current_window[-1][0]
            preds = []
            for j in range(1, 4):
                target_time = last_time + timedelta(days=j)
                idx = (target_time - base_time).total_seconds() / 86400.0
                preds.append((target_time, model.predict([[idx]])[0]))
            return preds

        # Prepare training data with lag features
        X_train = []
        y_train = []
        for i in range(max_lag, len(current_window)):
            day_index = (current_window[i][0] - base_time).total_seconds() / 86400.0
            features = [day_index]
            for lag in self.lags:
                features.append(current_window[i - lag][1])
            X_train.append(features)
            y_train.append(current_window[i][1])
            
        model = xgb.XGBRegressor(n_estimators=50, max_depth=3)
        model.fit(X_train, y_train)
        
        # Recursive prediction for the next 3 days
        last_time = current_window[-1][0]
        history_values = [rec[1] for rec in current_window]
        future_predictions = []
        
        for j in range(1, 4):
            target_time = last_time + timedelta(days=j)
            target_index = (target_time - base_time).total_seconds() / 86400.0
            
            # Construct features using previous values (including predictions)
            features = [target_index]
            for lag in self.lags:
                features.append(history_values[-lag])
            
            pred = model.predict([features])[0]
            future_predictions.append((target_time, pred))
            history_values.append(pred)  # Add prediction to history for next step's lags
            
        return future_predictions
