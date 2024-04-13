##########
# Import #
##############################################################################

from datetime import datetime
import os
import json
import requests 

from .__base import BaseScraper

###########
# Classes #
##############################################################################

class BinanceScraper(BaseScraper):
    
    def __init__(
            self, 
            key: str = "https://api.binance.com/api/v3/ticker/price?symbol="
    ) -> None:
        """Initiate the BinanceScraper instance

        Parameters
        ----------
        key : str, optional
            The key for create the request, by default 
            "https://api.binance.com/api/v3/ticker/price?symbol="
        
        Notes
        -----
            "https://fapi.binance.com/fapi/v1/ticker/price?symbol="
        """
        super().__init__()
        
        # Set key to default
        self.set_key(key)
        
    
    ##########################################################################
    
    @property
    def key(self) -> str:
        return self.__key
    
    ##########################################################################
    
    def set_key(self, key: str) -> None:
        
        self.__key = key
        
    ##########################################################################
    
    def scrape(
            self, 
            assets: list[str],
            result_path: str = "tmp_scrape",
            return_result: list[dict] = False,
    ) -> list[dict]:
        """Scrape the data, use API in this case

        Parameters
        ----------
        assets : list[str]
            list of asset in Binance symbol

        Returns
        -------
        list[dict]
            List of scraped result
        """
        # Initiate scraped_result as empty list
        scraped_result = []
        
        # Iterate over assets
        for asset in assets:
            
            # Get data
            url = self.key + asset  
            data = requests.get(url) 
            
            # Get current date
            timestamp = datetime.utcnow()
            timestamp_json = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            timestamp_file_name = timestamp.strftime('%Y%m%d_%H%M%S')
            
            # JSONize
            data = data.json() 
            
            # Result
            result = self.get_default_dict()

            # Append value
            result["timestamp"] = timestamp_json
            result["asset"] = data["symbol"]
            result["price"] = data["price"]
            
            # Export
            # Convert date format
            file_name = f"{result['asset']}_{timestamp_file_name}.json"
            self.export_json(result, result_path, file_name)
            
            scraped_result.append(result)
        
        # Return value if needed
        if return_result is True:
            return scraped_result
        
        # delete result, prevent memory leak
        del scraped_result
    
    #############
    # Utilities #
    ##########################################################################
    
    @staticmethod
    def get_default_dict() -> dict:
        return {
            "timestamp": None,
            "asset": None,
            "price": None,
            "source_id": 0,
            "engine": 0,
        }
        
    ##########################################################################
    
    @staticmethod
    def export_json(
            object: dict, 
            export_path: str, 
            file_name: str = None   
    ) -> None:
        """Export json to the `export_path`

        Parameters
        ----------
        object : dict
            Python dictionary object
        export_path : str
            Path to export, only directory tree
        file_name : str
            The name of file, only file_name.json
        """
        # Create the directory if it doesn't exist
        if not os.path.exists(export_path):
            os.makedirs(export_path)
            
        # Combine path
        export_path = os.path.join(export_path, file_name)
        
        # Save
        with open(export_path, "w") as json_file:
            json.dump(object, json_file, indent=4)
    
    ##########################################################################

##############################################################################
