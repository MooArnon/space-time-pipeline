from space_time_pipeline.scraper import BinanceScraper


scraper = BinanceScraper()

scraper.scrape(
    assets=["BTCUSDT", "BNBUSDT"]
)

data = scraper.scrape(
    assets=["BTCUSDT", "BNBUSDT"],
    return_result = True,
    result_path = "tmp_scrape"
)

assert isinstance(data, list)

for element in data:
    assert isinstance(element, dict)
    assert isinstance(float(element["price"]), float)