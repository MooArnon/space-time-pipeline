from space_time_pipeline.scraper import BinanceScraper

params = {"interval": "15m", "limit": 1}
columns = [
    'open_time', 'open', 'high', 'low', 'close', 'volume',
    'close_time', 'quote_asset_volume', 'number_of_trades',
    'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
]
column_type = {
    'open_time': 'int64',
    'open': 'float64',
    'high': 'float64',
    'low': 'float64',
    'close': 'float64',
    'volume': 'float64',
    'close_time': 'int64',
    'quote_asset_volume': 'float64',
    'number_of_trades': 'int64',
    'taker_buy_base_asset_volume': 'float64',
    'taker_buy_quote_asset_volume': 'float64',
    'ignore': 'object'
}

scraper = BinanceScraper(
    key="https://fapi.binance.com/fapi/v1/klines",
)

data = scraper.detail_scrape_to_pd(
    assets=["BTCUSDT", "BNBUSDT", "ADAUSDT", "ETHUSDT", "DOGEUSDT", "XRPUSDT"],
    params=params,
    columns=columns,
    column_type=column_type,
)

data.to_csv("test-scraper.csv")