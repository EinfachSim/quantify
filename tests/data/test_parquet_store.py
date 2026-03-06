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

    def test_read_returns_dataframe(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv)
        result = store.read("AAPL", "1d")
        assert isinstance(result, pd.DataFrame)

    def test_read_roundtrip(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv)
        result = store.read("AAPL", "1d")
        pd.testing.assert_frame_equal(result, sample_ohlcv)

    def test_read_with_start(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv)
        start = str(sample_ohlcv.index[3].date())
        result = store.read("AAPL", "1d", start=start)
        assert result.index.min() >= pd.Timestamp(start, tz="UTC")

    def test_read_with_end(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv)
        end = str(sample_ohlcv.index[6].date())
        result = store.read("AAPL", "1d", end=end)
        assert result.index.max() <= pd.Timestamp(end, tz="UTC")
    
    def test_read_with_start_and_end(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv)
        start = str(sample_ohlcv.index[2].date())
        end = str(sample_ohlcv.index[7].date())
        result = store.read("AAPL", "1d", start=start, end=end)
        assert result.index.min() >= pd.Timestamp(start, tz="UTC")
        assert result.index.max() <= pd.Timestamp(end, tz="UTC")
    
    def test_read_file_not_found_raises(self, store):
        with pytest.raises(FileNotFoundError):
            store.read("NONEXISTENT", "1d")
    
    def test_append_to_existing(self, store, sample_ohlcv):
        first_half = sample_ohlcv.iloc[:5]
        second_half = sample_ohlcv.iloc[5:]
        store.write("AAPL", "1d", first_half)
        store.append("AAPL", "1d", second_half)
        result = store.read("AAPL", "1d")
        pd.testing.assert_frame_equal(result, sample_ohlcv)

    def test_append_sorts_index(self, store, sample_ohlcv):
        first_half = sample_ohlcv.iloc[:5]
        second_half = sample_ohlcv.iloc[5:]
        store.write("AAPL", "1d", second_half)   # write later data first
        store.append("AAPL", "1d", first_half)   # append earlier data
        result = store.read("AAPL", "1d")
        assert result.index.is_monotonic_increasing

    def test_append_drops_duplicates(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv)
        store.append("AAPL", "1d", sample_ohlcv)   # append same data again
        result = store.read("AAPL", "1d")
        assert len(result) == len(sample_ohlcv)

    def test_append_duplicate_keeps_last(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv)
        new_data = sample_ohlcv.iloc[-3:].copy()
        new_data["close"] = 99999.0                # modify close so we can verify which was kept
        store.append("AAPL", "1d", new_data)
        result = store.read("AAPL", "1d")
        assert (result.iloc[-3:]["close"] == 99999.0).all()

    def test_append_to_nonexistent_creates_file(self, store, sample_ohlcv):
        store.append("AAPL", "1d", sample_ohlcv)   # no prior write
        result = store.read("AAPL", "1d")
        pd.testing.assert_frame_equal(result, sample_ohlcv)
    
    def test_read_many_returns_multiindex(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv)
        store.write("MSFT", "1d", sample_ohlcv)
        result = store.read_many(["AAPL", "MSFT"], "1d")
        assert isinstance(result.index, pd.MultiIndex)

    def test_read_many_correct_symbols_in_index(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv)
        store.write("MSFT", "1d", sample_ohlcv)
        result = store.read_many(["AAPL", "MSFT"], "1d")
        assert set(result.index.get_level_values(0)) == {"AAPL", "MSFT"}

    def test_read_many_correct_data(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv)
        store.write("MSFT", "1d", sample_ohlcv)
        result = store.read_many(["AAPL", "MSFT"], "1d")
        pd.testing.assert_frame_equal(result.loc["AAPL"], sample_ohlcv)

    def test_read_many_with_start_and_end(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv)
        store.write("MSFT", "1d", sample_ohlcv)
        start = str(sample_ohlcv.index[2].date())
        end = str(sample_ohlcv.index[7].date())
        result = store.read_many(["AAPL", "MSFT"], "1d", start=start, end=end)
        assert result.loc["AAPL"].index.min() >= pd.Timestamp(start, tz="UTC")
        assert result.loc["AAPL"].index.max() <= pd.Timestamp(end, tz="UTC")

    def test_read_many_empty_symbols_returns_empty(self, store):
        result = store.read_many([], "1d")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_read_many_missing_symbol_raises(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv)
        with pytest.raises(FileNotFoundError):
            store.read_many(["AAPL", "NONEXISTENT"], "1d")

    def test_missing_ranges_no_gaps(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv)
        start = str(sample_ohlcv.index[0].date())
        end = str(sample_ohlcv.index[-1].date())
        result = store.missing_ranges("AAPL", "1d", start=start, end=end)
        assert result == []

    def test_missing_ranges_gap_in_middle(self, store, sample_ohlcv):
        # write data with a gap in the middle
        no_gap = pd.concat([sample_ohlcv.iloc[:3], sample_ohlcv.iloc[6:]])
        store.write("AAPL", "1d", no_gap)
        start = str(sample_ohlcv.index[0].date())
        end = str(sample_ohlcv.index[-1].date())
        result = store.missing_ranges("AAPL", "1d", start=start, end=end)
        assert len(result) == 1
        assert result[0][0] == sample_ohlcv.index[3]
        assert result[0][1] == sample_ohlcv.index[5]

    def test_missing_ranges_gap_at_start(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv.iloc[3:])
        start = str(sample_ohlcv.index[0].date())
        end = str(sample_ohlcv.index[-1].date())
        result = store.missing_ranges("AAPL", "1d", start=start, end=end)
        assert len(result) == 1
        assert result[0][0] == sample_ohlcv.index[0]
        assert result[0][1] == sample_ohlcv.index[2]

    def test_missing_ranges_gap_at_end(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv.iloc[:7])
        start = str(sample_ohlcv.index[0].date())
        end = str(sample_ohlcv.index[-1].date())
        result = store.missing_ranges("AAPL", "1d", start=start, end=end)
        assert len(result) == 1
        assert result[0][0] == sample_ohlcv.index[7]
        assert result[0][1] == sample_ohlcv.index[-1]

    def test_missing_ranges_multiple_gaps(self, store, sample_ohlcv):
        # keep only rows 0,1,2 and 6,7,8 — gap at 3,4,5
        data = pd.concat([sample_ohlcv.iloc[:3], sample_ohlcv.iloc[6:9]])
        store.write("AAPL", "1d", data)
        start = str(sample_ohlcv.index[0].date())
        end = str(sample_ohlcv.index[8].date())
        result = store.missing_ranges("AAPL", "1d", start=start, end=end)
        assert len(result) == 1
        assert result[0][0] == sample_ohlcv.index[3]
        assert result[0][1] == sample_ohlcv.index[5]

    def test_missing_ranges_no_args_uses_stored_range(self, store, sample_ohlcv):
        # gap in middle, no start/end passed
        data = pd.concat([sample_ohlcv.iloc[:3], sample_ohlcv.iloc[6:]])
        store.write("AAPL", "1d", data)
        result = store.missing_ranges("AAPL", "1d")
        assert len(result) == 1
    
    # delete
    def test_delete_removes_file(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv)
        store.delete("AAPL", "1d")
        assert not store._path("AAPL", "1d").exists()

    def test_delete_nonexistent_silent(self, store):
        store.delete("NONEXISTENT", "1d")  # should not raise

    # available_symbols
    def test_available_symbols_returns_list(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv)
        store.write("MSFT", "1d", sample_ohlcv)
        result = store.available_symbols("1d")
        assert set(result) == {"AAPL", "MSFT"}

    def test_available_symbols_empty_timeframe(self, store):
        result = store.available_symbols("1d")
        assert result == []

    def test_available_symbols_only_returns_correct_timeframe(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv)
        store.write("AAPL", "1h", sample_ohlcv)
        result = store.available_symbols("1d")
        assert result == ["AAPL"]

    # available_timeframes
    def test_available_timeframes_returns_list(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv)
        store.write("AAPL", "1h", sample_ohlcv)
        result = store.available_timeframes("AAPL")
        assert set(result) == {"1d", "1h"}

    def test_available_timeframes_empty(self, store):
        result = store.available_timeframes("AAPL")
        assert result == []

    def test_available_timeframes_only_returns_correct_symbol(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv)
        store.write("MSFT", "1d", sample_ohlcv)
        result = store.available_timeframes("AAPL")
        assert result == ["1d"]

    # date_range
    def test_date_range_returns_correct_bounds(self, store, sample_ohlcv):
        store.write("AAPL", "1d", sample_ohlcv)
        start, end = store.date_range("AAPL", "1d")
        assert start == sample_ohlcv.index.min()
        assert end == sample_ohlcv.index.max()

    def test_date_range_nonexistent_raises(self, store):
        with pytest.raises(FileNotFoundError):
            store.date_range("NONEXISTENT", "1d")