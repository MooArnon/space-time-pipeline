##########
# Import #
##############################################################################

import logging
import json
import os 

import psycopg2
import pandas as pd

from .__base import BaseDataWarehouse

###########
# Classes #
##############################################################################

class PostgreSQLDataWarehouse(BaseDataWarehouse):
    
    ##############
    # Properties #
    ##########################################################################
    
    @property
    def connector(self):
        return self.__connector
    
    ##########################################################################
    
    @property
    def cursor(self):
        return self.__cursor
    
    ##########################################################################
    
    def set_connector(
            self, 
            host = os.environ.get('DB_HOST'), 
            port = os.environ.get('DB_PORT'),
            user = os.environ.get('DB_USERNAME'), 
            password = os.environ.get('DB_PASSWORD'), 
            database = os.environ.get('DB_NAME'),
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
        self.__connector = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )
        
        # Init cursor
        self.__cursor = self.__connector.cursor()
        
        print("Warehouse's connection is successful")
        
    ##########################################################################
    
    def close_connection(self):
        """Close the connection
        """
        if self.connector:
            self.cursor.close()
            self.connector.close()
            print("Warehouse's connection is closed")
    
    #############
    # Decorator #
    ##########################################################################
    
    def connect_decorator(func):
        def wrapper(self, *args, **kwargs):
            self.set_connector()
            result = func(self, *args, **kwargs)
            self.close_connection()
            return result
        return wrapper

    ##########
    # Method #
    ##########################################################################
    # SQL #
    #######
    
    @connect_decorator
    def execute_sql_file(
            self, 
            logger:logging, 
            file_path: str, 
            replace_condition_dict: dict = None,
    ) -> None:
        """Execute the query, return nothing if select

        Parameters
        ----------
        logger : logging
            logging object
        file_path : str
            Path of .sql file
        replace_condition_dict : dict
            Replace dictionary
        """
        queries = self.read_query_file(
            file_path,
            replace_condition_dict 
        )
        
        try:

            # Execute each individual query
            for query in queries:
                query = query.strip()
                if query:
                    self.cursor.execute(query)
                    self.connector.commit()
                    
            logger.info(f"Execute {file_path} is successfully")

        except psycopg2.Error as e:
            logger.error("Error while executing SQL file:", e)
            raise SystemError(e)
        
    ##########################################################################
    
    @connect_decorator
    def select(
            self, 
            logger: logging, 
            file_path: str, 
            replace_condition_dict: dict = None,
    ) -> pd.DataFrame:
        """Run query that select the data, fetch all data to data frame

        Parameters
        ----------
        logger : logging
            logging object
        file_path : str
            Path of .sql file
        replace_condition_dict : dict
            Replace dictionary
            
        Returns
        -------
        DataFrame
            Selected data
        """
        df = pd.DataFrame()  # Initialize an empty DataFrame

        # Read queries
        queries = self.read_query_file(file_path, replace_condition_dict)

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
            
            logger.info(f"Select {file_path} is successfully")

        except psycopg2.Error as e:
            logger.error("Error while selecting data from PostgreSQL:", e)
            raise SystemError(e)

        return df
    
    ##########################################################################
    
    @connect_decorator
    def insert_data(
            self, 
            table_name: str, 
            df: pd.DataFrame, 
            logger:logging,
    ) -> None:
        """Insert data into the database

        Parameters
        ----------
        table_name : str
            Name of table
        df : pd.DataFrame
            Data frame
        logger : logging
            Logger object
        """
        try:
            # Get column names from the DataFrame
            columns = ', '.join(df.columns.tolist())
            
            # Prepare the INSERT INTO statement with placeholders
            placeholder = ', '.join(['%s'] * len(df.columns))
            insert_query = \
                f"INSERT INTO {table_name} ({columns}) VALUES ({placeholder})"
            
            # Iterate over each row in the DataFrame
            for _, row in df.iterrows():
                # Extract values from the row
                values = tuple(row)
                
                # Execute the INSERT INTO statement
                self.cursor.execute(insert_query, values)
            
            # Commit the transaction
            self.connector.commit()
            logger.info(
                "Data inserted successfully into PostgreSQL table: %s", 
                table_name,
            )
        
        except psycopg2.Error as e:
            logger.error("Error while inserting data into PostgreSQL:", e)
            raise SystemError(e)

    ##########################################################################
    # Insert #
    ##########

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

    ##########################################################################
    
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
    
    ##########################################################################
    
##############################################################################
