import pytest
import pandas as pd
from unittest.mock import MagicMock
from quantify.data.manager import DataManager

class TestDataManager:

    @pytest.fixture
    def source(self):
        return MagicMock()

    @pytest.fixture
    def store(self):
        return MagicMock()

    @pytest.fixture
    def manager(self, source, store):
        return DataManager(source, store)

    # get
    def test_get_calls_store_read(self, manager, store):
        manager.get("AAPL", "1d", start="2024-01-01", end="2024-12-31")
        store.read.assert_called_once_with("AAPL", "1d", start="2024-01-01", end="2024-12-31")

    def test_get_returns_dataframe(self, manager, store, sample_ohlcv):
        store.read.return_value = sample_ohlcv
        result = manager.get("AAPL", "1d")
        assert isinstance(result, pd.DataFrame)

    # get_many
    def test_get_many_calls_store_read_many(self, manager, store):
        manager.get_many(["AAPL", "MSFT"], "1d", start="2024-01-01", end="2024-12-31")
        store.read_many.assert_called_once_with(["AAPL", "MSFT"], "1d", start="2024-01-01", end="2024-12-31")

    def test_get_many_returns_dataframe(self, manager, store, sample_ohlcv):
        store.read_many.return_value = sample_ohlcv
        result = manager.get_many(["AAPL", "MSFT"], "1d")
        assert isinstance(result, pd.DataFrame)

    # sync
    def test_sync_writes_new_symbol(self, manager, source, store, sample_ohlcv):
        store.available_symbols.return_value = []
        source.fetch.return_value = sample_ohlcv
        manager.sync(["AAPL"], "1d", "2024-01-01", "2024-12-31")
        store.write.assert_called_once_with("AAPL", "1d", sample_ohlcv)

    def test_sync_appends_missing_ranges(self, manager, source, store, sample_ohlcv):
        store.available_symbols.return_value = ["AAPL"]
        store.missing_ranges.return_value = [("2024-06-01", "2024-06-30")]
        source.fetch.return_value = sample_ohlcv
        manager.sync(["AAPL"], "1d", "2024-01-01", "2024-12-31")
        store.append.assert_called_once_with("AAPL", "1d", sample_ohlcv)

    def test_sync_no_missing_ranges_does_nothing(self, manager, source, store):
        store.available_symbols.return_value = ["AAPL"]
        store.missing_ranges.return_value = []
        manager.sync(["AAPL"], "1d", "2024-01-01", "2024-12-31")
        source.fetch.assert_not_called()
        store.append.assert_not_called()

    def test_sync_multiple_missing_ranges(self, manager, source, store, sample_ohlcv):
        store.available_symbols.return_value = ["AAPL"]
        store.missing_ranges.return_value = [
            ("2024-02-01", "2024-02-28"),
            ("2024-06-01", "2024-06-30"),
        ]
        source.fetch.return_value = sample_ohlcv
        manager.sync(["AAPL"], "1d", "2024-01-01", "2024-12-31")
        assert source.fetch.call_count == 2
        assert store.append.call_count == 2

    def test_sync_multiple_symbols(self, manager, source, store, sample_ohlcv):
        store.available_symbols.return_value = []
        source.fetch.return_value = sample_ohlcv
        manager.sync(["AAPL", "MSFT"], "1d", "2024-01-01", "2024-12-31")
        assert source.fetch.call_count == 2
        assert store.write.call_count == 2

    # update
    def test_update_raises_if_symbol_not_in_store(self, manager, store):
        store.available_symbols.return_value = []
        with pytest.raises(ValueError):
            manager.update(["AAPL"], "1d")

    def test_update_fetches_from_last_date(self, manager, source, store, sample_ohlcv):
        store.available_symbols.return_value = ["AAPL"]
        last_date = pd.Timestamp("2024-06-01", tz="UTC")
        store.date_range.return_value = (pd.Timestamp("2024-01-01", tz="UTC"), last_date)
        source.fetch.return_value = sample_ohlcv
        manager.update(["AAPL"], "1d")
        call_args = source.fetch.call_args
        assert call_args[1]["start"] == last_date

    def test_update_appends_to_store(self, manager, source, store, sample_ohlcv):
        store.available_symbols.return_value = ["AAPL"]
        store.date_range.return_value = (
            pd.Timestamp("2024-01-01", tz="UTC"),
            pd.Timestamp("2024-06-01", tz="UTC")
        )
        source.fetch.return_value = sample_ohlcv
        manager.update(["AAPL"], "1d")
        store.append.assert_called_once_with("AAPL", "1d", sample_ohlcv)