from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path
from quantify.constants import *

class BaseSource(ABC):
    
    #Fetching
    @abstractmethod
    def fetch(self, symbol, timeframe, start, end) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def fetch_many(self, symbols, timeframe, start, end) -> dict[str, pd.DataFrame]:
        pass

    #Discovery
    @abstractmethod
    def available_symbols(self) -> list[str]:
        pass

    @abstractmethod
    def available_timeframes(self) -> list[str]:
        pass

    # Metadata
    @abstractmethod
    def get_asset_info(self, symbol) -> dict:
        pass

class CSVSource(BaseSource):
    def __init__(self, root):
        super().__init__()
        self.root = Path(root)
    
    def _path(self, symbol, timeframe):
        path = self.root / timeframe.lower() / f"{symbol.upper()}.csv"
        return path

    def _validate_schema(self, df: pd.DataFrame) -> None:
        missing = set(CANONICAL_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"DataFrame missing canonical columns: {missing}")

    #Fetching
    def fetch(self, symbol, timeframe, start=None, end=None) -> pd.DataFrame:
        path = self._path(symbol, timeframe)
        df = pd.read_csv(path, index_col="datetime", parse_dates=True, date_format= "ISO8601")
        self._validate_schema(df)
        df.index = pd.to_datetime(df.index, utc=True)
        df.index.name = None
        if start:
            df = df[df.index >= start]
        if end:
            df = df[df.index <= end]
        return df
    
    def fetch_many(self, symbols, timeframe, start, end) -> dict[str, pd.DataFrame]:
        pass

    #Discovery
    def available_symbols(self) -> list[str]:
        pass

    def available_timeframes(self) -> list[str]:
        pass

    # Metadata
    def get_asset_info(self, symbol) -> dict:
        pass
