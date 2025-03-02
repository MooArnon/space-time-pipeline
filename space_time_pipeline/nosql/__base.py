##########
# Import #
##############################################################################

from abc import abstractmethod

###########
# Classes #
##############################################################################

class BaseNoSQL:
    def __init__(self):
        pass
    
    ##########################################################################
    
    @abstractmethod
    def query_data() -> None:
        raise NotImplementedError("Child class must implement `query_data`")
    
    ##########################################################################
    
    @abstractmethod
    def ingest_data() -> None:
        raise NotImplementedError("Child class must implement `query_data`")
    
    ##########################################################################

##############################################################################
