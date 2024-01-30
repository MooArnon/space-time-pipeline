#--------#
# Import #
#----------------------------------------------------------------------------#

from abc import abstractmethod

import pandas

#---------#
# Classes #
#----------------------------------------------------------------------------#

class BaseDataWarehouse:
    
    def __init__(self) -> None:
        pass
    
    #------------------------------------------------------------------------#
    
    @abstractmethod
    def select(query: str) -> pandas.DataFrame:
        """Select data and convert to data frame

        Parameters
        ----------
        query : str
            Input query

        Returns
        -------
        pandas.DataFrame
            Data frame
        """
    
    #------------------------------------------------------------------------#
    
    @abstractmethod
    def execute_query(query: str) -> None:
        """Execute query, return nothing
        
        Parameters
        ----------
        query : str
            Input query
        """
    
    #-----------#
    # Utilities #
    #------------------------------------------------------------------------#
    
    @staticmethod
    def read_query_file(file_path: str) -> list[str]:
        """Read .sql file and convert to text

        Parameters
        ----------
        file_path : str
            Target path

        Returns
        -------
        list[str]
            List of query as string
        """
        # Read the SQL file
        with open(file_path, 'r') as sql_file:
            sql_queries = sql_file.read()

        # Split the queries (assuming they are separated by ';')
        queries = sql_queries.split(';')
        
        return queries
    
    #------------------------------------------------------------------------#
    
#----------------------------------------------------------------------------#