from quantify.data.store import ParquetStore
import pandas as pd

df = pd.DataFrame()

ParquetStore("data/parquet").write("AAPL", "1d", df)