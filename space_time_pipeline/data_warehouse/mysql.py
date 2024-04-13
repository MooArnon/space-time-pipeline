##########
# Import #
##############################################################################

import logging
import json
import os 

import mysql.connector
import pandas as pd

from .__base import BaseDataWarehouse

###########
# Classes #
##############################################################################

class MySQLDataWarehouse(BaseDataWarehouse):
    
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
            host = os.environ.get('DB_BI_HOST'), 
            port = os.environ.get('DB_BI_PORT'),
            user = os.environ.get('DB_BI_USERNAME'), 
            password = os.environ.get('DB_BI_PASSWORD'), 
            database = os.environ.get('DB_BI_NAME'),
    ) -> None:
        """Set the connector and cursor

        Parameters
        ----------
        host : str
            Name of host
        port : str
            Port of database
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

        except mysql.connector.Error as e:
            logger.error("Error while executing SQL file:", e)
            raise SystemError(e)

    ##########################################################################
    
    @connect_decorator
    def truncate_all_insert_data(
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
            # Truncate table
            self.cursor.execute(f"TRUNCATE TABLE {table_name};")
            logger.info(f"table {table_name} was truncated")
            
            self.insert_data(
                table_name = table_name,
                df = df,
                logger = logger
            )
        
        except mysql.connector.Error as e:
            logger.error("Error while inserting data into PostgreSQL:", e)
            raise SystemError(e)
    
    ##########################################################################
    
    @connect_decorator
    def delete_insert_data(
            self, 
            table_name: str, 
            df: pd.DataFrame, 
            logger:logging,
            condition_lst: str = None,
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
        condition_lst: str
            Adding where condition
        """
        try:
            # Delete data
            self.delete_data(
                table_name = table_name,
                condition_lst = condition_lst,
                logger = logger,
            )
            
            # Insert data
            self.insert_data(
                table_name = table_name,
                df = df,
                logger = logger,
            )
        
        except mysql.connector.Error as e:
            logger.error("Error while inserting data into PostgreSQL:", e)
            raise SystemError(e)
        
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
        
        except mysql.connector.Error as e:
            logger.error("Error while inserting data into PostgreSQL:", e)
            raise SystemError(e)
    
    ##########################################################################
    
    @connect_decorator
    def delete_data(
            self, 
            table_name: str,  
            condition_lst: list[str],
            logger:logging,
    ) -> None:
        """Delete data from the database

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
            base_statement = f"DELETE FROM {table_name}"
            
            conditional_statement = self.construct_conditional_query(
                base_statement,
                condition_lst
            )
            
            # Delete from table by condition_lst
            self.cursor.execute(conditional_statement)
            self.connector.commit()
            
            logger.info(
                f"Delete {table_name} by query \n{conditional_statement}"
            )
            
        except mysql.connector.Error as e:
            logger.error("Error while inserting data into PostgreSQL:", e)
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

        except mysql.connector.Error as e:
            logger.error("Error while selecting data from PostgreSQL:", e)
            raise SystemError(e)

        return df
    
    ##########################################################################
    
##############################################################################
