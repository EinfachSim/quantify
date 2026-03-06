from abc import ABC, abstractmethod
import pandas as pd

class BaseStore(ABC):

    # Writing
    @abstractmethod
    def write(self, symbol, timeframe, df) -> None:
        pass
    
    @abstractmethod
    def append(self, symbol, timeframe, df) -> None:
        pass

    # Reading
    @abstractmethod
    def read(self, symbol, timeframe, start, end) -> pd.DataFrame:
        pass

    @abstractmethod
    def read_many(self, symbols, timeframe, start, end) -> pd.DataFrame:
        pass

    # Info
    @abstractmethod
    def available_symbols(self, timeframe) -> list[str]:
        pass

    @abstractmethod
    def available_timeframes(self, symbol) -> list[str]:
        pass

    @abstractmethod
    def date_range(self, symbol, timeframe) -> tuple[str, str]:
        pass

    @abstractmethod
    def missing_ranges(self, symbol, timeframe, start, end) -> list[tuple[str, str]]:
        pass

    @abstractmethod
    def delete(self, symbol, timeframe) -> None:
        pass

    @abstractmethod
    def info(self) -> dict:
        pass