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
    
    @staticmethod
    def read_query_file(
            query: str = None,
            query_file_path: str = None,
            replace_condition_dict: dict = None,
    ) -> list[str]:
        """Read .sql file and convert to text
        Please assign only query or query_file_path

        Parameters
        ----------
        query : str
            query itself
        query_file_path : str
            Target path
        replace_condition_dict : dict
            Keep the replacing dict
            {
                "VALUE_IN_SQL": "value-to-replace"
            }

        Returns
        -------
        list[str]
            List of query as string
        """
        # Choose only one parameter
        if not query and not query_file_path:
            raise ValueError("Please, assign query_file_path or query")
        
        # Read the SQL file
        if query_file_path:
            with open(query_file_path, 'r') as sql_file:
                query = sql_file.read()
        
        # Replace with conditional dict
        if replace_condition_dict:
            sql_queries = query
            for key, value in replace_condition_dict.items():
                placeholder = f'{key}'
                sql_queries = sql_queries.replace(placeholder, str(value))
        return sql_queries
    
    ##########################################################################
    
##############################################################################
