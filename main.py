from quantify.data.store import ParquetStore
import pandas as pd
import numpy as np

index = pd.date_range("2024-01-01", periods=10, freq="D", tz="UTC")
df = pd.DataFrame({
        "open":   np.random.uniform(100, 200, 10),
        "high":   np.random.uniform(100, 200, 10),
        "low":    np.random.uniform(100, 200, 10),
        "close":  np.random.uniform(100, 200, 10),
        "volume": np.random.uniform(1e6, 1e7, 10),
        "vwap":   np.random.uniform(100, 200, 10),
    }, index=index)
df.index.freq = None

store = ParquetStore("data/parquet")
store.write("AAPL", "1d", df)
store.write("GOOGLE", "1d", df)

store.read_many(["AAPL","GOOGLE"], "1d")

store.info()