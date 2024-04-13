##########
# Import #
##############################################################################

from abc import abstractmethod

###########
# classes #
##############################################################################

class BaseDataLake:
    
    @abstractmethod
    def upload_to_data_lake(target_file: object, prefix: str, **kwargs):
        """Upload file to prefix

        Parameters
        ----------
        target_file : object
            The target object
        prefix : str
            The prefix to store file
        """
        
    ##########################################################################
    
    @abstractmethod
    def download_file(target_prefix: str, local_path: str, **kwargs):
        """Download files to local working directory

        Parameters
        ----------
        target_prefix : str
            The target prefix
        local_path : str
            Path to store file at local
        """
    
    ##########################################################################
    
##############################################################################
