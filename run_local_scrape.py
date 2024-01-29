from space_time_pipeline.scraper import BinanceScraper


scraper = BinanceScraper()

scraper.scrape(
    assets=["BTCUSDT", "BNBUSDT"]
)