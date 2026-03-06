import pytest

# from quantlib.data.sources.yfinance import YahooFinanceSource

class TestYahooFinanceSource:

    @pytest.fixture
    def source(self):
        # return YahooFinanceSource()
        pass

    def test_schema_normalization(self, source, sample_ohlcv, mocker):
        # mock yf.download, assert output has canonical columns
        pass

    def test_timeframe_translation(self, source, mocker):
        pass

    @pytest.mark.integration
    def test_fetch_real_data(self, source):
        # actually hits the network — only runs with pytest -m integration
        pass