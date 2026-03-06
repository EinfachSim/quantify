from quantify.features.base import BaseFeature

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