##########
# Import #
##############################################################################

from abc import abstractmethod
import json

###########
# Classes #
##############################################################################
# Base class #
##############

class BaseScraper:
    
    @abstractmethod
    def scrape(self, assets: list[str]) -> dict:
        """Scrape data and export it at the working directory

        Parameters
        ----------
        assets : list[str]
            Name of asset

        Returns
        -------
        dict
            dictionary of scraped element
        """
    
    #############
    # Utilities #
    ##########################################################################
    
    @staticmethod
    def write_json(dictionary: dict):
        """Create JSON file from input dictionary

        Parameters
        ----------
        dictionary : dict
            Data
        """
        # Create json object
        json_object = json.dumps(dictionary, indent=4)
        
        # Extract the information
        scraped_asset = dictionary["asset"]
        scraped_time = dictionary["timestamp"]
        
        # create the name for json file
        name = f"{scraped_asset}_{scraped_time}.json"
        
        # Writing to sample.json
        with open(name, "w") as outfile:
            outfile.write(json_object)    
            
    ##########################################################################
    
    @staticmethod
    def export_json(json_file: dict, export_path: str) -> None:
        """_summary_

        Parameters
        ----------
        json_file : dict
            _description_
        export_path : str
            _description_
        """
        with open(export_path, "w") as json_file:
            json.dump(object, json_file, indent=4)
    
    ##########################################################################
    
##############################################################################
