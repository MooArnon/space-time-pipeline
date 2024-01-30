#--------#
# Import #
#----------------------------------------------------------------------------#

import os
import shutil

import pytest

from space_time_pipeline import BinanceScraper

#---------#
# Classes #
#----------------------------------------------------------------------------#

class TestScraper:
    
    @pytest.fixture
    def scraper(self):
        return BinanceScraper()
    
    #------------------------------------------------------------------------#
    
    def test_scrape_multiple(self, scraper: BinanceScraper):

        data = scraper.scrape(
            assets=["BTCUSDT", "BNBUSDT"],
            return_result = True,
            result_path = "tmp_tests"
        )
        
        assert isinstance(data, list)
        
        for element in data:
            assert isinstance(element, dict)
            assert isinstance(float(element["price"]), float)
        
    #------------------------------------------------------------------------#
    
    def test_export(self, scraper: BinanceScraper):

        scraper.scrape(
            assets=["BTCUSDT", "BNBUSDT"],
            return_result = False,
            result_path = "tmp_tests"
        )
        
        paths = os.listdir("tmp_tests")
        
        assert paths != []
        assert paths is not None
    
    #------------------------------------------------------------------------#
    
    def test_delete_result(self):
        shutil.rmtree("tmp_tests")
        
    #------------------------------------------------------------------------#
    
#----------------------------------------------------------------------------#
