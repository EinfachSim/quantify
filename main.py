from quantify.data.source import YahooFinanceSource
from quantify.data.store import ParquetStore
from quantify.data.manager import DataManager
from quantify.features.technical import MomentumFeature

source = YahooFinanceSource()
store = ParquetStore("./data/parquet")
manager = DataManager(source, store)

manager.sync(["AAPL", "MSFT"], "1d", "2024-01-02", "2024-12-31")
df = manager.get("AAPL", "1d")
print(df.head())
print(store.info())


MomentumFeature(20).compute(df)