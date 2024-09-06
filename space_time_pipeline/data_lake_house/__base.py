##########
# Import #
##############################################################################

from logging import Logger

from ..data_lake.s3 import S3DataLake

###########
# classes #
##############################################################################

class BaseDataLakeHouse:
    
    def __init__(self, logger: Logger) -> None:
        self.data_lake = S3DataLake(
            logger = logger
        )
        self.logger = logger
    
    ##########################################################################
    
##############################################################################
