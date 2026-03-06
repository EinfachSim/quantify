import pytest
import pandas as pd
from quantify.data.source import CSVSource

class TestCSVSource:

    @pytest.fixture
    def csv_source(self, tmp_path, sample_ohlcv):
        path = tmp_path / "1d"
        path.mkdir()
        sample_ohlcv.to_csv(path / "AAPL.csv", index_label="datetime")
        return CSVSource(tmp_path)

    def test_fetch_returns_dataframe(self, csv_source, sample_ohlcv):
        result = csv_source.fetch("AAPL", "1d")
        assert isinstance(result, pd.DataFrame)

    def test_fetch_roundtrip(self, csv_source, sample_ohlcv):
        result = csv_source.fetch("AAPL", "1d")
        pd.testing.assert_frame_equal(result, sample_ohlcv)

    def test_fetch_with_start(self, csv_source, sample_ohlcv):
        start = str(sample_ohlcv.index[3].date())
        result = csv_source.fetch("AAPL", "1d", start=start)
        assert result.index.min() >= pd.Timestamp(start, tz="UTC")

    def test_fetch_with_end(self, csv_source, sample_ohlcv):
        end = str(sample_ohlcv.index[6].date())
        result = csv_source.fetch("AAPL", "1d", end=end)
        assert result.index.max() <= pd.Timestamp(end, tz="UTC")

    def test_fetch_with_start_and_end(self, csv_source, sample_ohlcv):
        start = str(sample_ohlcv.index[2].date())
        end = str(sample_ohlcv.index[7].date())
        result = csv_source.fetch("AAPL", "1d", start=start, end=end)
        assert result.index.min() >= pd.Timestamp(start, tz="UTC")
        assert result.index.max() <= pd.Timestamp(end, tz="UTC")

    def test_fetch_uppercase_symbol(self, csv_source, sample_ohlcv):
        result = csv_source.fetch("aapl", "1d")
        assert isinstance(result, pd.DataFrame)

    def test_fetch_file_not_found_raises(self, csv_source):
        with pytest.raises(FileNotFoundError):
            csv_source.fetch("NONEXISTENT", "1d")

    def test_fetch_invalid_schema_raises(self, tmp_path):
        path = tmp_path / "1d"
        path.mkdir()
        bad_df = pd.DataFrame({"wrong_col": [1, 2, 3]})
        bad_df.to_csv(path / "AAPL.csv", index_label="datetime")
        source = CSVSource(tmp_path)
        with pytest.raises(ValueError):
            source.fetch("AAPL", "1d")
    def test_fetch_many_returns_dict(self, csv_source, sample_ohlcv):
        tmp_path = csv_source.root
        sample_ohlcv.to_csv(tmp_path / "1d" / "MSFT.csv", index_label="datetime")
        result = csv_source.fetch_many(["AAPL", "MSFT"], "1d")
        assert isinstance(result, dict)

    def test_fetch_many_correct_keys(self, csv_source, sample_ohlcv):
        tmp_path = csv_source.root
        sample_ohlcv.to_csv(tmp_path / "1d" / "MSFT.csv", index_label="datetime")
        result = csv_source.fetch_many(["AAPL", "MSFT"], "1d")
        assert set(result.keys()) == {"AAPL", "MSFT"}

    def test_fetch_many_correct_data(self, csv_source, sample_ohlcv):
        result = csv_source.fetch_many(["AAPL"], "1d")
        pd.testing.assert_frame_equal(result["AAPL"], sample_ohlcv)

    def test_fetch_many_with_start_and_end(self, csv_source, sample_ohlcv):
        start = str(sample_ohlcv.index[2].date())
        end = str(sample_ohlcv.index[7].date())
        result = csv_source.fetch_many(["AAPL"], "1d", start=start, end=end)
        assert result["AAPL"].index.min() >= pd.Timestamp(start, tz="UTC")
        assert result["AAPL"].index.max() <= pd.Timestamp(end, tz="UTC")

    def test_fetch_many_empty_symbols_returns_empty(self, csv_source):
        result = csv_source.fetch_many([], "1d")
        assert result == {}

    def test_fetch_many_missing_symbol_raises(self, csv_source):
        with pytest.raises(FileNotFoundError):
            csv_source.fetch_many(["AAPL", "NONEXISTENT"], "1d")