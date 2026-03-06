from abc import ABC, abstractmethod
import pandas as pd

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
