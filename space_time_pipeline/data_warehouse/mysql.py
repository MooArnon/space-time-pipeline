#--------#
# Import #
#----------------------------------------------------------------------------#

import logging
import json
import os 

import mysql.connector
from mysql.connector import Error
import pandas as pd

from .__base import BaseDataWarehouse

#---------#
# Classes #
#----------------------------------------------------------------------------#

class MySQLDataWarehouse(BaseDataWarehouse):
    
    def __init__(
            self,
            host: str = os.environ.get('DB_HOST'), 
            user: str = os.environ.get('DB_USERNAME'), 
            password: str = os.environ.get('DB_PASSWORD'), 
            database: str = os.environ.get('DB_NAME'),
    ) -> None:
        """Initiate MySQLDataWarehouse

        Parameters
        ----------
        host : str, optional
            Host of MySQL, 
            by default os.environ.get('DW_HOST')
        user : str, optional
            Name of user, 
            by default os.environ.get('DW_USER')
        password : str, optional
            Password, 
            by default os.environ.get('DW_PASSWORD')
        database : str, optional
            Name of database, 
            by default os.environ.get('DW_DATABASE')
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
    # SQL #
    #-----#
    
    def execute_sql_file(self, logger:logging, file_path: str) -> None:
        """Execute the query, return nothing if select

        Parameters
        ----------
        file_path : str
            Path of .sql file
        """
        queries = self.read_query_file(file_path)

        try:
            # Execute each query
            for query in queries:
                query = query.strip()
                if query:
                    self.cursor.execute(query)
                    
        except Error as e:
            logger.error("Error while inserting data into MySQL:", e)
            
        finally:
            self.cursor.close()
            self.connector.close()
        
    #------------------------------------------------------------------------#
    
    def select(self, logger: logging, file_path: str) -> pd.DataFrame:
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
        
        try:
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
            
        except Error as e:
            logger.error("Error while inserting data into MySQL:", e)
            
        finally:
            self.cursor.close()
            self.connector.close()

        return df
    
    #------------------------------------------------------------------------#
    
    def insert_data(
            self, 
            table_name: str, 
            df: pd.DataFrame, 
            logger:logging,
    ) -> None:
        """Insert data

        Parameters
        ----------
        table_name : str
            Name of table
        df : pd.DataFrame
            Data frame
        logger : logging
            Logger object
        """
        # Get column names from the DataFrame
        columns = ', '.join(df.columns.tolist())
        
        # Prepare the INSERT INTO statement with placeholders
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = \
            f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # Iterate over each row in the DataFrame
        for _, row in df.iterrows():
            # Extract values from the row
            values = tuple(row)
            
            # Execute the INSERT INTO statement
            self.cursor.execute(insert_query, values)
        
        # Commit the transaction
        self.connector.commit()
        logger.info(
            "Data inserted successfully into MySQL table:", 
            table_name,
        )

    #------------------------------------------------------------------------#
    # Insert #
    #--------#

    def iterative_read_insert_json(
            self,
            json_dir_path: str,
            table_name: str, 
            rename_dict: dict,
            logger:logging,
    ) -> None:
        """Iterative read and insert data to database

        Parameters
        ----------
        json_dir_path : str
            Path to folder that contains json
        table_name : str
            Name of table
        rename_dict : dict
            Rename dictionary
        logger : logging
            Logger object
        """
        # List all file
        json_dir = os.listdir(json_dir_path)
        
        try:
            # Iterate over json_dir
            for json_path in json_dir:
                
                json_path = os.path.join(json_dir_path, json_path)
                
                # Read and modify data frame
                df = self.modify_json_df(json_path, rename_dict)
                
                # Insert data
                self.insert_data(
                    table_name = table_name, 
                    df = df, 
                    logger = logger,
                )
        
        except Error as e:
            logger.error("Error while inserting data into MySQL:", e)
            self.connector.rollback()
            self.cursor.close()
            self.connector.close()
        
        finally:
            self.cursor.close()
            self.connector.close()
        
    #------------------------------------------------------------------------#
    
    @staticmethod
    def modify_json_df(json_path: str, rename_dict: dict) -> pd.DataFrame:
        """Read json and modify data frame

        Parameters
        ----------
        json_path : str
            Path of json
        rename_dict : dict
            Rename dictionary

        Returns
        -------
        pd.DataFrame
            Modified output
        """
        # read json
        with open(json_path, 'r') as file:
            json_data = json.load(file)

        # Check if single record
        if isinstance(json_data, dict):
            json_data = [json_data]
        
        # Rename
        df = pd.DataFrame(json_data).rename(columns = rename_dict)
        
        return df
    
    #------------------------------------------------------------------------#
    
#----------------------------------------------------------------------------#
