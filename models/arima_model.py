from datetime import timedelta

class ARIMAModel:
    def __init__(self):
        self.window_size = 30

    def train_and_predict(self, current_window):
        from statsmodels.tsa.arima.model import ARIMA
        import warnings
        
        y_train = [rec[1] for rec in current_window]
        last_time = current_window[-1][0]
        
        future_dates = [last_time + timedelta(days=j) for j in range(1, 4)]
        
        if len(y_train) < 4:
            return list(zip(future_dates, [y_train[-1]] * 3))
            
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            try:
                model = ARIMA(y_train, order=(1, 1, 0))
                res = model.fit()
                preds = res.forecast(steps=3)
                return list(zip(future_dates, preds))
            except Exception:
                return list(zip(future_dates, [y_train[-1]] * 3))
