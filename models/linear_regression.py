from datetime import timedelta

class LinearRegressionModel:
    def __init__(self):
        self.window_size = 7

    def train_and_predict(self, current_window):
        from sklearn.linear_model import LinearRegression
        
        base_time = current_window[0][0]
        X_train = []
        y_train = []
        for rec in current_window:
            day_index = (rec[0] - base_time).total_seconds() / 86400.0
            X_train.append([day_index])
            y_train.append(rec[1])
            
        model = LinearRegression()
        model.fit(X_train, y_train)
        
        last_time = current_window[-1][0]
        X_future = []
        future_dates = []
        for j in range(1, 4): # Next 1 to 3 days
            target_time = last_time + timedelta(days=j)
            target_index = (target_time - base_time).total_seconds() / 86400.0
            X_future.append([target_index])
            future_dates.append(target_time)
            
        preds = model.predict(X_future)
        return list(zip(future_dates, preds))
