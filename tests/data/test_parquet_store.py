import pytest
import pandas as pd

from quantify.data.store import ParquetStore

class TestParquetStore:

    @pytest.fixture
    def store(self, tmp_path):
        return ParquetStore(tmp_path)
    
    def test_write_creates_file(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv)
        expected_path = store.root / "1d" / "AAPL.parquet"
        assert expected_path.exists()
    
    def test_write_correct_subdir(self, store, sample_ohlcv):
        store.write("AAPL", "1h", sample_ohlcv)
        expected_path = store.root / "1h" / "AAPL.parquet"
        assert expected_path.exists()
    
    def test_write_invalid_schema_raises(self, store):
        bad_df = pd.DataFrame({"wrong_col": [1, 2, 3]})
        with pytest.raises(ValueError):
            store.write("AAPL", "1d", bad_df)
    
    def test_write_uppercase_symbol(self, store, sample_ohlcv):
        store.write("aapl", "1d", sample_ohlcv)
        expected_path = store.root / "1d" / "AAPL.parquet"
        assert expected_path.exists()
    
    def test_write_lowercase_timeframe(self, store, sample_ohlcv):
        store.write("AAPL", "1D", sample_ohlcv)
        expected_path = store.root / "1d" / "AAPL.parquet"
        assert expected_path.exists()

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