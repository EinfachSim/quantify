import matplotlib.pyplot as plt
import pandas as pd
from quantify.data.source import YahooFinanceSource
from quantify.data.store import ParquetStore
from quantify.data.manager import DataManager
from quantify.features.technical import MomentumFeature, RSIFeature

source = YahooFinanceSource()
store = ParquetStore("./data/parquet")
manager = DataManager(source, store)

df = manager.get_many(["AAPL"], "1d")

momentum = MomentumFeature(period=20)
rsi = RSIFeature(period=14)

df["momentum_20"] = momentum.compute(df)
df["rsi_14"] = rsi.compute(df)

aapl = df.loc["AAPL"]

fig, axes = plt.subplots(3, 1, figsize=(7, 5), sharex=True)

# price
axes[0].plot(aapl.index, aapl["close"], label="Close", color="black")
axes[0].set_title("AAPL Close Price")
axes[0].legend()

# momentum
axes[1].plot(aapl.index, aapl["momentum_20"], label="Momentum 20", color="blue")
axes[1].axhline(0, color="black", linewidth=0.5, linestyle="--")
axes[1].set_title("Momentum (20 days)")
axes[1].legend()

# rsi
axes[2].plot(aapl.index, aapl["rsi_14"], label="RSI 14", color="purple")
axes[2].axhline(70, color="red", linewidth=0.5, linestyle="--", label="Overbought")
axes[2].axhline(30, color="green", linewidth=0.5, linestyle="--", label="Oversold")
axes[2].set_title("RSI (14 days)")
axes[2].legend()

plt.tight_layout()
plt.show()