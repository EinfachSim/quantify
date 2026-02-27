from abc import ABC, abstractmethod
import pandas as pd

class BaseStore(ABC):

    # Writing
    @abstractmethod
    def write(symbol, timeframe, df) -> None:
        pass
    
    @abstractmethod
    def append(symbol, timeframe, df) -> None:
        pass

    # Reading
    @abstractmethod
    def read(symbol, timeframe, start, end) -> pd.DataFrame:
        pass

    @abstractmethod
    def read_many(symbols, timeframe, start, end) -> pd.DataFrame:
        pass

    # Info
    @abstractmethod
    def available_symbols(timeframe) -> list[str]:
        pass

    @abstractmethod
    def available_timeframes(symbol) -> list[str]:
        pass

    @abstractmethod
    def date_range(symbol, timeframe) -> tuple[str, str]:
        pass

    @abstractmethod
    def missing_ranges(symbol, timeframe, start, end) -> list[tuple[str, str]]:
        pass

    @abstractmethod
    def delete(symbol, timeframe) -> None:
        pass

    def info() -> dict:
        pass