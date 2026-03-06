import pytest
import pandas as pd

# from quantlib.data.store import ParquetStore  # uncomment when ready

class TestParquetStore:

    @pytest.fixture
    def store(self, tmp_path):
        # tmp_path is a built-in pytest fixture — unique temp dir per test
        # return ParquetStore(tmp_path)
        pass

    def test_write_read_roundtrip(self, store, sample_ohlcv):
        # store.write("AAPL", "1d", sample_ohlcv)
        # result = store.read("AAPL", "1d")
        # pd.testing.assert_frame_equal(result, sample_ohlcv)
        pass

    def test_append(self, store, sample_ohlcv):
        pass

    def test_partial_read(self, store, sample_ohlcv):
        pass

    def test_available_symbols(self, store, sample_ohlcv):
        pass

    def test_date_range(self, store, sample_ohlcv):
        pass

    def test_missing_ranges(self, store, sample_ohlcv):
        pass

    def test_overwrite_behavior(self, store, sample_ohlcv):
        pass

    def test_empty_read_returns_empty_dataframe(self, store):
        pass

    def test_schema_enforcement(self, store):
        pass