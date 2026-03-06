import exchange_calendars as xcals
import pytest
import pandas as pd
import numpy as np
@pytest.fixture
def sample_ohlcv():
    cal = xcals.get_calendar("XNYS")
    sessions = cal.sessions_in_range("2024-01-01", "2024-01-20")[:10]
    index = pd.DatetimeIndex(sessions).tz_localize("UTC")
    df = pd.DataFrame({
        "open":   np.random.uniform(100, 200, 10),
        "high":   np.random.uniform(100, 200, 10),
        "low":    np.random.uniform(100, 200, 10),
        "close":  np.random.uniform(100, 200, 10),
        "volume": np.random.uniform(1e6, 1e7, 10),
        "vwap":   np.random.uniform(100, 200, 10),
    }, index=index)
    df.index.freq = None
    return df

@pytest.fixture
def multi_index_ohlcv(sample_ohlcv):
    """MultiIndex (symbol, datetime) DataFrame with two symbols."""
    msft = sample_ohlcv.copy()
    aapl = sample_ohlcv.copy()
    return pd.concat({"AAPL": aapl, "MSFT": msft})