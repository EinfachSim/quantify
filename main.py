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


from quantify.features.technical import VolumeMomentumFeature

vol_mom = VolumeMomentumFeature(period=20)
df["volume_momentum_20"] = vol_mom.compute(df)

aapl = df.loc["AAPL"]

fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)

axes[0].plot(aapl.index, aapl["close"], color="black", label="Close")
axes[0].set_title("AAPL Close Price")
axes[0].legend()

axes[1].bar(aapl.index, aapl["volume"], color="grey", label="Volume", width=0.8)
axes[1].set_title("Volume")
axes[1].legend()

axes[2].plot(aapl.index, aapl["volume_momentum_20"], color="blue", label="Volume Momentum 20")
axes[2].axhline(0, color="black", linewidth=0.5, linestyle="--")
axes[2].set_title("Volume Momentum (20 days)")
axes[2].legend()

from quantify.features.technical import PriceToMAFeature

pma_20 = PriceToMAFeature(period=20)
pma_50 = PriceToMAFeature(period=50)
df["price_to_ma_20"] = pma_20.compute(df)
df["price_to_ma_50"] = pma_50.compute(df)

aapl = df.loc["AAPL"]

fig, axes = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

axes[0].plot(aapl.index, aapl["close"], color="black", label="Close", linewidth=1)
axes[0].plot(aapl.index, aapl["close"].rolling(20).mean(), color="blue", label="MA 20", linewidth=0.8, linestyle="--")
axes[0].plot(aapl.index, aapl["close"].rolling(50).mean(), color="red", label="MA 50", linewidth=0.8, linestyle="--")
axes[0].set_title("AAPL Close Price with Moving Averages")
axes[0].legend()

axes[1].plot(aapl.index, aapl["price_to_ma_20"], color="blue", label="Price to MA 20", linewidth=1)
axes[1].plot(aapl.index, aapl["price_to_ma_50"], color="red", label="Price to MA 50", linewidth=1)
axes[1].axhline(0, color="black", linewidth=0.5, linestyle="--")
axes[1].set_title("Price to Moving Average (0 = at MA, positive = above MA)")
axes[1].legend()

from quantify.features.technical import CandleStructureFeature

candle = CandleStructureFeature()
candle_df = candle.compute(df)
aapl = df.loc["AAPL"]
aapl_candle = candle_df.loc["AAPL"]

import mplfinance as mpf

aapl = df.loc["AAPL"].copy()
aapl.index = aapl.index.tz_localize(None)  # mplfinance doesn't like tz-aware

# add candle structure as subplots
aapl_candle = candle_df.loc["AAPL"].copy()
aapl_candle.index = aapl_candle.index.tz_localize(None)

# slice to last 60 days so candles are visible
aapl_60 = aapl.iloc[-60:]
aapl_candle_60 = aapl_candle.iloc[-60:]

add_plots = [
    mpf.make_addplot(aapl_candle_60["body_size"], panel=2, color="blue", ylabel="Body Size"),
    mpf.make_addplot(aapl_candle_60["body_position"], panel=3, color="purple", ylabel="Body Position"),
]

mpf.plot(
    aapl_60,
    type="candle",
    style="charles",
    title="AAPL Candlestick Chart",
    volume=True,        # volume gets panel 1 automatically
    addplot=add_plots,  # your features start at panel 2
    figsize=(14, 12),
)

plt.tight_layout()
plt.show()