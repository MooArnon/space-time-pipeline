##########
# Import #
##############################################################################

from abc import abstractmethod

import pandas

###########
# Classes #
##############################################################################

class BaseDataWarehouse:
    
    def __init__(self) -> None:
        pass
    
    ##########################################################################
    
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
    
    ##########################################################################
    
    @abstractmethod
    def execute_query(query: str) -> None:
        """Execute query, return nothing
        
        Parameters
        ----------
        query : str
            Input query
        """
    
    #############
    # Utilities #
    ##########################################################################
    
    @staticmethod
    def read_query_file(
            file_path: str,
            replace_condition_dict: dict = None,
    ) -> list[str]:
        """Read .sql file and convert to text

        Parameters
        ----------
        file_path : str
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
        # Read the SQL file
        with open(file_path, 'r') as sql_file:
            sql_queries = sql_file.read()
        
        if replace_condition_dict:
            for key, value in replace_condition_dict.items():
                placeholder = f'{key}'
                sql_queries = sql_queries.replace(placeholder, str(value))

        # Split the queries (assuming they are separated by ';')
        queries = sql_queries.split(';')
        
        return queries
    
    ##########################################################################
    @staticmethod
    def construct_conditional_query(
        statement: str,
        condition_lst: list[str],
    ) -> str:
        """Adding where into query statement

        Parameters
        ----------
        statement : str
            Query statement
        condition_lst : list[str]
            List of conditional element,
            ex. ["DATE = '2023-01-04'", "ASSET = 'BTCUSDT'"]

        Returns
        -------
        str
            Statement with added where cause.
        """
        
        for idx, condition in enumerate(condition_lst):
            
            if idx == 0:
                statement += f' WHERE {condition}'
            
            else:
                statement += f' AND {condition}'
                
        return statement
    
    ##########################################################################
    
##############################################################################
