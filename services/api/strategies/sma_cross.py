import pandas as pd

class Strategy:
    def __init__(self, fast:int=10, slow:int=50):
        self.fast, self.slow = fast, slow
    def generate_positions(self, price: pd.Series, forecast: pd.Series) -> pd.Series:
        # simple: if forecast above current -> long signal, filtered by MA trend
        ma_fast = price.rolling(self.fast).mean()
        ma_slow = price.rolling(self.slow).mean()
        trend = (ma_fast > ma_slow).astype(float)
        edge = (forecast > price).astype(float)
        return (trend & edge).astype(float)
