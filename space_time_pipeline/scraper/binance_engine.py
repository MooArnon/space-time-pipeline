##########
# Import #
##############################################################################

from datetime import datetime
import os
import json
import requests 

import pandas as pd

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
            for detailed scrape "https://fapi.binance.com/fapi/v1/klines"
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
    
    ##########################################################################
    
    def detail_scrape_to_pd(
            self, 
            assets: list[str],
            params: dict,
            columns: list[str],
            column_type: dict
    ) -> pd.DataFrame:
        """Scrape the data, validate types, and return as DataFrame.

        Parameters
        ----------
        assets : list[str]
            List of assets in Binance symbol format.
        params : dict
            Parameters for the API request.
        columns : list[str]
            Expected column names for the DataFrame.
        column_type : dict
            Expected types for each column.

        Returns
        -------
        pd.DataFrame
            A concatenated DataFrame containing data for all assets.
        """
        all_dataframes = []  # List to store DataFrames for each asset
        
        for asset in assets:
            try:
                # Update params with the current asset symbol
                params['symbol'] = asset

                # Make the API request
                response = requests.get(self.key, params=params)
                response.raise_for_status()  # Ensure no HTTP errors
                data = response.json()

                # Convert data to DataFrame
                df = pd.DataFrame(data, columns=columns)

                # Enforce data types
                for col, dtype in column_type.items():
                    try:
                        df[col] = df[col].astype(dtype)
                    except ValueError as e:
                        raise ValueError(f"Type conversion failed for column '{col}': {e}")

                # Add metadata columns
                timestamp = datetime.utcnow()
                df['scraped_time'] = timestamp
                df['asset'] = asset

                # Append to the list of DataFrames
                all_dataframes.append(df)

            except Exception as e:
                print(f"Error processing asset {asset}: {e}")

        # Combine all DataFrames into one
        if all_dataframes:
            final_df = pd.concat(all_dataframes, ignore_index=True)
            return final_df
        else:
            return pd.DataFrame()
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
    
    @staticmethod
    def generate_fingerprint(row):
        return [type(x).__name__ for x in row]
    
    ##########################################################################

##############################################################################
