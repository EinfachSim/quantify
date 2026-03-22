from quantify.features.base import BaseFeature
import pandas as pd

class MomentumFeature(BaseFeature):
    def __init__(self, period=20):
        self.period = period

    @property
    def name(self):
        return f"momentum_{self.period}"

    def compute(self, df):
        series = df.groupby(level=0)["close"].transform(lambda x: x.pct_change(self.period))
        return series.rename(self.name).to_frame()

class MomentumFeature(BaseFeature):
    def __init__(self, period=20):
        self.period = period

    @property
    def name(self):
        return f"momentum_{self.period}"

    def compute(self, df):
        series = df.groupby(level=0)["close"].transform(lambda x: x.pct_change(self.period))
        return series.rename(self.name).to_frame()

class RSIFeature(BaseFeature):
    def __init__(self, period=20):
        self.period = period

    @property
    def name(self):
        return f"rsi_{self.period}"

    def compute(self, df):
        def rsi_calc(close):
            daily_returns = close.pct_change(1)
            gains = daily_returns.clip(lower=0)
            losses = (-daily_returns).clip(lower=0)
            avg_gain = gains.ewm(alpha=1/self.period, min_periods=self.period, adjust=False).mean()
            avg_loss = losses.ewm(alpha=1/self.period, min_periods=self.period, adjust=False).mean()
            rs = avg_gain / avg_loss
            return 100 - (100 / (1+rs))
        rsi = df.groupby(level=0)["close"].transform(rsi_calc)

        return rsi.rename(self.name).to_frame()

class BollingerBandsFeature(BaseFeature):
    def __init__(self, period=20):
        self.period = period

    @property
    def name(self):
        return f"bb_{self.period}"

    def compute(self, df):
        def bb_calc(close):
            roll = close.rolling(self.period)
            middle = roll.mean()
            std = roll.std()
            upper = middle + 2*std
            lower = middle - 2*std
            bb_position = (close - lower)/(upper - lower)
            return pd.DataFrame({
                f"bb_upper_{self.period}": upper,
                f"bb_lower_{self.period}": lower,
                f"bb_position_{self.period}": bb_position,
            })
        return df.groupby(level=0, group_keys=False)["close"].apply(bb_calc)

class VolatilityFeature(BaseFeature):
    def __init__(self, period=20):
        self.period = period

    @property
    def name(self):
        return f"vol_{self.period}"

    def compute(self, df):
        def vol_calc(close):
            daily_returns = close.pct_change(1)
            vol = daily_returns.rolling(self.period).std()
            return vol
        vol = df.groupby(level=0)["close"].transform(vol_calc)

        return vol.rename(self.name).to_frame()
    
class VolumeMomentumFeature(BaseFeature):
    def __init__(self, period=20):
        self.period = period

    @property
    def name(self):
        return f"volume_momentum_{self.period}"

    def compute(self, df):
        def volume_calc(vol):
            rolling_volume_mean = vol.rolling(self.period).mean()
            
            return vol / rolling_volume_mean - 1
        vol = df.groupby(level=0)["volume"].transform(volume_calc)

        return vol.rename(self.name).to_frame()