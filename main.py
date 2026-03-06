import matplotlib.pyplot as plt
import pandas as pd
from quantify.data.source import YahooFinanceSource
from quantify.data.store import ParquetStore
from quantify.data.manager import DataManager
from quantify.features.technical import *

source = YahooFinanceSource()
store = ParquetStore("./data/parquet")
manager = DataManager(source, store)

df = manager.get_many(["AAPL"], "1d")

momentum = MomentumFeature(period=20)
rsi = RSIFeature(period=14)
bb = BollingerBandsFeature(period=14)

df["momentum_20"] = momentum.compute(df)
df["rsi_14"] = rsi.compute(df)
print(bb.compute(df))

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

bb = BollingerBandsFeature(period=20)
bb_result = bb.compute(df)
df = pd.concat([df, bb_result], axis=1)

aapl = df.loc["AAPL"]

fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

# price with bollinger bands
axes[0].plot(aapl.index, aapl["close"], label="Close", color="black", linewidth=1)
axes[0].plot(aapl.index, aapl["bb_upper_20"], label="Upper Band", color="red", linewidth=0.8, linestyle="--")
axes[0].plot(aapl.index, aapl["bb_lower_20"], label="Lower Band", color="green", linewidth=0.8, linestyle="--")
axes[0].fill_between(aapl.index, aapl["bb_upper_20"], aapl["bb_lower_20"], alpha=0.1, color="blue")
axes[0].set_title("AAPL Close Price with Bollinger Bands (20 days)")
axes[0].legend()

# bb position
axes[1].plot(aapl.index, aapl["bb_position_20"], label="BB Position", color="purple", linewidth=1)
axes[1].axhline(1.0, color="red", linewidth=0.5, linestyle="--", label="Upper Band")
axes[1].axhline(0.0, color="green", linewidth=0.5, linestyle="--", label="Lower Band")
axes[1].axhline(0.5, color="black", linewidth=0.5, linestyle="--", label="Middle")
axes[1].set_title("BB Position (0=lower band, 1=upper band)")
axes[1].legend()


plt.tight_layout()
plt.show()