from quantify.features.base import BaseFeature

class MomentumFeature(BaseFeature):
    def __init__(self, period=20):
        self.period = period

    def name(self):
        return f"momentum_{self.period}"

    def compute(self, df):
        print(df)