from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path
import numpy as np
from quantify.constants import *
import exchange_calendars as xcals

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

class ParquetStore(BaseStore):
    def __init__(self, root):
        super().__init__()
        self.root = Path(root)
        self.root.mkdir(exist_ok = True)

    def _validate_schema(self, df: pd.DataFrame) -> None:
        missing = set(CANONICAL_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"DataFrame missing canonical columns: {missing}")
    
    def _path(self, symbol: str, timeframe: str) -> Path:
        """Helper to construct the file path for a symbol/timeframe."""
        return self.root / timeframe.lower() / f"{symbol.upper()}.parquet"

    # Writing
    def write(self, symbol, timeframe, df) -> None:
        self._validate_schema(df)
        path = self._path(symbol, timeframe)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path)
    
    def append(self, symbol, timeframe, df) -> None:
        try:
            old_df = self.read(symbol, timeframe)
            combined = pd.concat([old_df, df])
            combined = combined[~combined.index.duplicated(keep="last")]
            combined = combined.sort_index()
        except(FileNotFoundError):
            combined = df
        self.write(symbol, timeframe, combined)


    # Reading
    def read(self, symbol, timeframe, start=None, end=None) -> pd.DataFrame:
        path = self._path(symbol, timeframe)
        tmp_df = pd.read_parquet(path)
        if start:
            tmp_df = tmp_df[tmp_df.index >= start]
        if end:
            tmp_df = tmp_df[tmp_df.index <= end]
        return tmp_df

    def read_many(self, symbols, timeframe, start=None, end=None) -> pd.DataFrame:
        if not symbols:
            return pd.DataFrame()
        dfs = []
        for s in symbols:
            try:
                dfs.append(self.read(s, timeframe, start=start, end=end))
            except(FileNotFoundError):
                raise FileNotFoundError(f"Symbol {s} not found in store!")
        
        return pd.concat(dfs, keys=symbols)



    # Info
    def available_symbols(self, timeframe) -> list[str]:
        path = self.root / timeframe.lower()
        if not path.exists():
            return []
        return [x.stem for x in path.glob("*.parquet")]

    def available_timeframes(self, symbol) -> list[str]:
        return [x.parent.stem for x in self.root.glob(f"*/{symbol.upper()}.parquet")]

    def date_range(self, symbol, timeframe) -> tuple[pd.Timestamp, pd.Timestamp]:
        df = self.read(symbol, timeframe)
        return (df.index.min(), df.index.max())

    def missing_ranges(self, symbol: str, timeframe: str, start=None, end=None) -> list[tuple]:
        if start is None or end is None:
            stored_start, stored_end = self.date_range(symbol, timeframe)
            start = start or stored_start
            end = end or stored_end

        if timeframe.lower() == "1d":
            cal = xcals.get_calendar(DEFAULT_CALENDAR)
            # strip timezone for exchange_calendars
            start_naive = pd.Timestamp(start).tz_localize(None) if pd.Timestamp(start).tzinfo is None else pd.Timestamp(start).tz_convert(None)
            end_naive = pd.Timestamp(end).tz_localize(None) if pd.Timestamp(end).tzinfo is None else pd.Timestamp(end).tz_convert(None)
            expected = pd.DatetimeIndex(cal.sessions_in_range(start_naive, end_naive)).tz_localize("UTC")
        else:
            expected = pd.date_range(start=start, end=end, freq=timeframe.lower(), tz="UTC")

        df = self.read(symbol, timeframe, start=start, end=end)
        missing_dates = expected[~expected.isin(df.index)]

        if len(missing_dates) == 0:
            return []

        if timeframe.lower() == "1d":
            cal = xcals.get_calendar(DEFAULT_CALENDAR)
            gaps = np.array([
                len(cal.sessions_in_range(
                    missing_dates[i].tz_convert(None),
                    missing_dates[i+1].tz_convert(None)
                )) > 2
                for i in range(len(missing_dates)-1)
            ])
            split_points = np.where(gaps)[0] + 1
        else:
            gaps = np.diff(missing_dates)
            split_points = np.where(gaps > pd.Timedelta(PANDAS_TIME_STR[timeframe.lower()]))[0] + 1

        groups = np.split(missing_dates, split_points)
        return [(g[0], g[-1]) for g in groups if len(g) > 0]
    
    def delete(self, symbol, timeframe) -> None:
        path = self._path(symbol, timeframe)
        if path.exists():
            path.unlink()

    def info(self) -> dict:
        all_data = self.root.glob("*/*.parquet")
        result = {}
        timeframes = set()
        for dp in all_data:
            tf_s = self.available_timeframes(dp.stem)
            timeframes.update(tf_s)
        for tf in timeframes:
            result[tf] = {}
            symbols = self.available_symbols(tf)
            for symbol in symbols:
                start, end = self.date_range(symbol, tf)
                df = self.read(symbol, tf)
                result[tf][symbol] = {
                    "start": start,
                    "end": end,
                    "rows": len(df)
                }
        return result