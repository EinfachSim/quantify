from abc import ABC, abstractmethod
import pandas as pd

class BaseSource(ABC):
    
    #Fetching
    @abstractmethod
    def fetch(symbol, timeframe, start, end) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def fetch_many(symbols, timeframe, start, end) -> dict[str, pd.DataFrame]:
        pass

    #Discovery
    @abstractmethod
    def available_symbols() -> list[str]:
        pass

    @abstractmethod
    def available_timeframes() -> list[str]:
        pass

    # Metadata
    @abstractmethod
    def get_asset_info(symbol) -> dict:
        pass
