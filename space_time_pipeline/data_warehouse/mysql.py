#--------#
# Import #
#----------------------------------------------------------------------------#

import os 

import mysql.connector
import pandas as pd

from .__base import BaseDataWarehouse

#---------#
# Classes #
#----------------------------------------------------------------------------#

class MySQLDataWarehouse(BaseDataWarehouse):
    
    def __init__(
            self,
            host: str = os.environ['DW_HOST'], 
            user: str = os.environ['DW_USER'], 
            password: str = os.environ['DW_PASSWORD'], 
            database: str = os.environ['DW_DATABASE'],
    ) -> None:
        """Initiate MySQLDataWarehouse

        Parameters
        ----------
        host : str, optional
            Host of MySQL, 
            by default os.environ['DW_HOST']
        user : str, optional
            Name of user, 
            by default os.environ['DW_USER']
        password : str, optional
            Password, 
            by default os.environ['DW_PASSWORD']
        database : str, optional
            Name of database, 
            by default os.environ['DW_DATABASE']
        """
        self.set_connector(
            host = host,
            user = user,
            password = password,
            database = database,
        )
        
        
    #------------#
    # Properties #
    #------------------------------------------------------------------------#
    
    @property
    def connector(self) -> mysql.connector:
        return self.__connector
    
    #------------------------------------------------------------------------#
    
    @property
    def cursor(self):
        return self.__cursor
    
    #------------------------------------------------------------------------#
    
    def set_connector(
            self, 
            host: str, 
            user: str, 
            password: str, 
            database: str,
    ) -> None:
        """Set the connector and cursor

        Parameters
        ----------
        host : str
            Name of host
        user : str
            Name of user
        password : str
            Password
        database : str
            Name of data base
        """
        # Init connector
        self.__connector = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        
        # Init cursor
        self.__cursor = self.__connector.cursor()

    #--------#
    # Method #
    #------------------------------------------------------------------------#

    def execute_sql_file(self, file_path: str) -> None:
        """Execute the query, return nothing if select

        Parameters
        ----------
        file_path : str
            Path of .sql file
        """
        queries = self.read_query_file(file_path)

        # Execute each query
        for query in queries:
            query = query.strip()
            if query:
                self.cursor.execute(query)
        self.connector.commit()
        self.connector.close()
        
    #------------------------------------------------------------------------#
    
    def select(self, file_path: str) -> pd.DataFrame:
        """Run query that select the data, fetch all data to data frame

        Parameters
        ----------
        file_path : str
            Path to .sql file

        Returns
        -------
        DataFrame
            Selected data
        """
        # Read queries
        queries = self.read_query_file(file_path)
        
        # Execute each query
        for query in queries:
            query = query.strip()
            if query:
                self.cursor.execute(query)

        # Fetch the data
        data = self.cursor.fetchall()

        # Get column names
        column_names = [desc[0] for desc in self.cursor.description]

        # Create pandas DataFrame
        df = pd.DataFrame(data, columns=column_names)

        # Close the cursor and connection
        self.cursor.close()
        self.connector.close()

        return df
    
    #------------------------------------------------------------------------#
    
#----------------------------------------------------------------------------#