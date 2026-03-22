import pytest
import pandas as pd
import numpy as np
import exchange_calendars as xcals
from quantify.features.technical import *

class TestMomentumFeature:

    @pytest.fixture
    def feature(self):
        return MomentumFeature(period=2)

    def test_name(self, feature):
        assert feature.name == "momentum_2"

    def test_compute_returns_dataframe(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        assert isinstance(result, pd.DataFrame)

    def test_compute_correct_column_name(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        assert "momentum_2" in result.columns

    def test_compute_warmup_is_nan(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        assert result.loc["AAPL"].iloc[:2]["momentum_2"].isna().all()

    def test_compute_after_warmup_not_nan(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        assert result.loc["AAPL"].iloc[2:]["momentum_2"].notna().all()

    def test_compute_correct_values(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        # manually verify: period=2, so momentum = close[t] / close[t-2] - 1
        aapl_close = multi_index_ohlcv.loc["AAPL"]["close"]
        expected = aapl_close.pct_change(2)
        pd.testing.assert_series_equal(
            result.loc["AAPL"]["momentum_2"],
            expected,
            check_names=False
        )

    def test_compute_multiple_symbols(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        symbols = result.index.get_level_values(0).unique()
        assert set(symbols) == {"AAPL", "MSFT"}

    def test_compute_symbols_independent(self, feature, multi_index_ohlcv):
        # momentum for AAPL should not be affected by MSFT data
        result_multi = feature.compute(multi_index_ohlcv)
        aapl_only = multi_index_ohlcv.loc[["AAPL"]]
        result_single = feature.compute(aapl_only)
        pd.testing.assert_series_equal(
            result_multi.loc["AAPL"]["momentum_2"],
            result_single.loc["AAPL"]["momentum_2"]
        )

class TestRSIFeature:

    @pytest.fixture
    def feature(self):
        return RSIFeature(period=3)  # small period so tests need less data

    def test_name(self, feature):
        assert feature.name == "rsi_3"

    def test_compute_returns_dataframe(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        assert isinstance(result, pd.DataFrame)

    def test_compute_correct_column_name(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        assert "rsi_3" in result.columns

    def test_compute_warmup_is_nan(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        assert result.loc["AAPL"].iloc[:3]["rsi_3"].isna().all()

    def test_compute_after_warmup_not_nan(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        assert result.loc["AAPL"].iloc[3:]["rsi_3"].notna().all()

    def test_compute_values_between_0_and_100(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        valid = result["rsi_3"].dropna()
        assert (valid >= 0).all() and (valid <= 100).all()

    def test_compute_symbols_independent(self, feature, multi_index_ohlcv):
        result_multi = feature.compute(multi_index_ohlcv)
        aapl_only = multi_index_ohlcv.loc[["AAPL"]]
        result_single = feature.compute(aapl_only)
        pd.testing.assert_series_equal(
            result_multi.loc["AAPL"]["rsi_3"],
            result_single.loc["AAPL"]["rsi_3"]
        )

    def test_compute_multiple_symbols(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        symbols = result.index.get_level_values(0).unique()
        assert set(symbols) == {"AAPL", "MSFT"}


class TestBollingerBandsFeature:

    @pytest.fixture
    def feature(self):
        return BollingerBandsFeature(period=3)

    def test_name(self, feature):
        assert feature.name == "bb_3"

    def test_compute_returns_dataframe(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        assert isinstance(result, pd.DataFrame)

    def test_compute_correct_columns(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        assert "bb_upper_3" in result.columns
        assert "bb_lower_3" in result.columns
        assert "bb_position_3" in result.columns

    def test_compute_warmup_is_nan(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        assert result.loc["AAPL"].iloc[:2]["bb_position_3"].isna().all()

    def test_compute_after_warmup_not_nan(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        assert result.loc["AAPL"].iloc[2:]["bb_position_3"].notna().all()

    def test_upper_above_lower(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        valid = result.dropna()
        assert (valid["bb_upper_3"] >= valid["bb_lower_3"]).all()

    def test_compute_symbols_independent(self, feature, multi_index_ohlcv):
        result_multi = feature.compute(multi_index_ohlcv)
        aapl_only = multi_index_ohlcv.loc[["AAPL"]]
        result_single = feature.compute(aapl_only)
        pd.testing.assert_frame_equal(
            result_multi.loc["AAPL"],
            result_single.loc["AAPL"]
        )

    def test_compute_multiple_symbols(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        symbols = result.index.get_level_values(0).unique()
        assert set(symbols) == {"AAPL", "MSFT"}

    def test_correct_index_structure(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        assert isinstance(result.index, pd.MultiIndex)
        assert result.index.nlevels == 2

class TestVolatilityFeature:

    @pytest.fixture
    def feature(self):
        return VolatilityFeature(period=3)

    def test_name(self, feature):
        assert feature.name == "vol_3"

    def test_compute_returns_dataframe(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        assert isinstance(result, pd.DataFrame)

    def test_compute_correct_column_name(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        assert "vol_3" in result.columns

    def test_compute_warmup_is_nan(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        assert result.loc["AAPL"].iloc[:3]["vol_3"].isna().all()

    def test_compute_after_warmup_not_nan(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        assert result.loc["AAPL"].iloc[3:]["vol_3"].notna().all()

    def test_compute_values_positive(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        valid = result["vol_3"].dropna()
        assert (valid >= 0).all()

    def test_compute_symbols_independent(self, feature, multi_index_ohlcv):
        result_multi = feature.compute(multi_index_ohlcv)
        aapl_only = multi_index_ohlcv.loc[["AAPL"]]
        result_single = feature.compute(aapl_only)
        pd.testing.assert_series_equal(
            result_multi.loc["AAPL"]["vol_3"],
            result_single.loc["AAPL"]["vol_3"]
        )

    def test_compute_multiple_symbols(self, feature, multi_index_ohlcv):
        result = feature.compute(multi_index_ohlcv)
        symbols = result.index.get_level_values(0).unique()
        assert set(symbols) == {"AAPL", "MSFT"}