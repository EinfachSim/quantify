import pytest
import pandas as pd
import numpy as np
from quantify.data.source import YahooFinanceSource

class TestYahooFinanceSource:

    @pytest.fixture
    def source(self):
        return YahooFinanceSource()

    @pytest.fixture
    def mock_yf_response(self, sample_ohlcv):
        """Simulates raw yfinance output — MultiIndex columns, capitalized, no vwap, tz-naive"""
        df = sample_ohlcv.copy()
        df.index = df.index.tz_localize(None)  # yfinance returns tz-naive for daily
        df = df.drop(columns=["vwap"])
        df.columns = [c.capitalize() for c in df.columns]
        df.columns = pd.MultiIndex.from_tuples([(c, "AAPL") for c in df.columns])
        return df

    def test_fetch_returns_dataframe(self, source, mock_yf_response, mocker):
        mocker.patch("yfinance.download", return_value=mock_yf_response)
        result = source.fetch("AAPL", "1d", "2024-01-01", "2024-01-10")
        assert isinstance(result, pd.DataFrame)

    def test_fetch_canonical_columns(self, source, mock_yf_response, mocker):
        mocker.patch("yfinance.download", return_value=mock_yf_response)
        result = source.fetch("AAPL", "1d", "2024-01-01", "2024-01-10")
        assert list(result.columns) == ["open", "high", "low", "close", "volume", "vwap"]

    def test_fetch_index_is_utc(self, source, mock_yf_response, mocker):
        mocker.patch("yfinance.download", return_value=mock_yf_response)
        result = source.fetch("AAPL", "1d", "2024-01-01", "2024-01-10")
        assert result.index.tz == pd.Timestamp("now", tz="UTC").tz

    def test_fetch_index_name_is_none(self, source, mock_yf_response, mocker):
        mocker.patch("yfinance.download", return_value=mock_yf_response)
        result = source.fetch("AAPL", "1d", "2024-01-01", "2024-01-10")
        assert result.index.name is None

    def test_fetch_vwap_computed(self, source, mock_yf_response, mocker):
        mocker.patch("yfinance.download", return_value=mock_yf_response)
        result = source.fetch("AAPL", "1d", "2024-01-01", "2024-01-10")
        assert "vwap" in result.columns
        assert not result["vwap"].isnull().any()

    def test_fetch_empty_response_raises(self, source, mocker):
        mocker.patch("yfinance.download", return_value=pd.DataFrame())
        with pytest.raises(ValueError):
            source.fetch("NONEXISTENT", "1d", "2024-01-01", "2024-01-10")

    @pytest.mark.integration
    def test_fetch_real_data(self, source):
        result = source.fetch("AAPL", "1d", "2024-01-01", "2024-01-10")
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert list(result.columns) == ["open", "high", "low", "close", "volume", "vwap"]