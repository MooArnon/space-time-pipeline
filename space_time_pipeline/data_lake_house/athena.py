##########
# Import #
##############################################################################

from logging import Logger
import time

import awswrangler as wr
import boto3
import pandas as pd

from .__base import BaseDataLakeHouse

###########
# classes #
##############################################################################

class Athena(BaseDataLakeHouse):
    
    def __init__(self, logger: Logger) -> None:
        super().__init__(logger)
        
    ##########################################################################
    
    def insert_to_table(
            self,
            df: pd.DataFrame,
            data_schema: dict,
            table_name: str,
            partition_columns: list[str],
            s3_path: str,
            s3_path_tmp: str = None,
            batch_size: int = 100,    
            ingestion_mode: str = "overwrite_partitions",
    ) -> None:
        """Insert data into IceBerg

        Parameters
        ----------
        df : pd.DataFrame
            Source data frame
        data_schema : dict
            Key and value of column and type, respectively
        table_name : str
            Name of LakeHouse table
        partition_columns : list[str]
            List of partition columns
        s3_path : str
            Path to store iceberg format data
        s3_path_tmp : str, optional
            Temporary path to place data
            if None, will use s3_path with _tmp prefix, by default None
        batch_size : int, optional
            size of partition per one insert, by default 100
        ingestion_mode: str, optional
            `append` append data, without checking
            `overwrite` delete all data in table
            `overwrite_partitions` delete matched partition

        """
        if not s3_path_tmp:
            s3_path_tmp = f"{s3_path}_tmp"
        print(data_schema)
        # Confirm that we will have the same type as IceBerg
        df = df.astype(data_schema)
        self.logger.info(f"Shape of data frame is {df.shape}")
        
        # Select unique partition
        # Separate partition by batch_size
        unique_combinations = df[partition_columns].drop_duplicates()
        batches = [
            unique_combinations.iloc[i:i + batch_size] \
                for i in range(0, len(unique_combinations), batch_size)
        ]
        batch_length = len(batches)
        
        # Iterate over batches partitions
        for i, batch in enumerate(batches):
            
            # Extract value and filter data
            batch_values = batch.values
            batch_df = df[
                df[partition_columns].apply(tuple, axis=1).isin(
                    [tuple(row) for row in batch_values]
                )
            ]
            self.logger.info(f"Shape of batch is {batch_df.shape}")
            print(df.head(5))
            print(df.dtypes)
            # Add to data frame
            wr.athena.to_iceberg(
                df=batch_df,
                database="warehouse",
                table=table_name,
                table_location=s3_path,
                temp_path=s3_path_tmp,
                partition_cols=partition_columns,
                keep_files=False,
                mode=ingestion_mode
            )
            self.logger.info(f"Finished batch {i+1}/{batch_length}")
    
    ##########################################################################
    
    def select(
            self,
            replace_condition_dict: dict,
            database: str,
            query: str = None,
            query_file_path: str = None,
    ) -> dict:
        """Select data from Athena iceberg

        Parameters
        ----------
        replace_condition_dict : dict
            Filter dict, eg {<'LIMIT'>: 10}
        database : str
            Name of data in glue
        query : str, optional
            Query statement, by default None
        query_file_path : str, optional
            Path to .sql file, by default None

        Returns
        -------
        dict
            Dictionary of data, convert to data by using pd.DataFrame(data)

        Raises
        ------
        ValueError
            When no Limit was found at replace_condition_dict.
            To minimize cost per quey.
        """
        # If limit does not appeared at replace_condition_dict
        # Raise error to prevent cost charged on account.
        if '<LIMIT>' not in replace_condition_dict:
            raise ValueError(
                "The 'LIMIT' condition is missing in the replace_condition_dict."
            )
        
        query = self.read_query_file(
            query = query,
            query_file_path = query_file_path,
            replace_condition_dict = replace_condition_dict,
        )

        return wr.athena.read_sql_query(
            sql=query, 
            database=database,
            keep_files=False
        ).to_dict()
        
    ##########################################################################
    
    def delete_data(
        self, 
        table_name: str,
        database: str,
        output_location: str,
        logger: str
    ) -> None:
        """Delete data at table

        Parameters
        ----------
        table_name : str
            Name of target table you need to delete
        database : str
            Database name
        output_location : str
            Location to save output
        logger : str
            Logger object
        """
        client = boto3.client('athena')
        
        self.execute_athena_query(
            client=client,
            query=f"DELETE FROM {table_name}",
            database=database,
            output_location=output_location,
            logger=logger,
        )
        logger.info(f"Deleted data at table {table_name}")
    
    ##########################################################################
    
    #TODO create method to execute query and measure data scanned
    def aggregate_to_table(
            self,
            replace_condition_dict: dict,
            database: str,
            output_location: str,
            mode: str,
            logger: Logger,
            query: str = None,
            query_file_path: str = None,
            table_name: str = None,
    ) -> None:
        client = boto3.client('athena')
        
        query = self.read_query_file(
            query = query,
            query_file_path = query_file_path,
            replace_condition_dict = replace_condition_dict,
        )
        
        # Perform delete
        if (mode == "delete_insert") & (table_name is not None):
            self.execute_athena_query(
                client=client,
                query=f"DELETE FROM {table_name}",
                database=database,
                output_location=output_location,
                logger=logger,
            )
        
        # Execute query
        runtime_sec, data_scanned_mb = self.execute_athena_query(
            client = client,
            query = query,
            database = database,
            output_location = output_location,
            logger = logger,
        )
        
        return runtime_sec, data_scanned_mb
    
    ##########################################################################
    
    def execute_athena_query(
            self,
            client: boto3.client,
            query: str,
            database: str, 
            output_location: str,
            logger: Logger,
    ) -> tuple[float, float]:
        """Execute query at Athena, without waiting

        Parameters
        ----------
        client : boto3.client
            Athena client
        query : str
            Query statement
        database : str
            Name of database
        output_location : str
            Location to store temp output
            in the format of `s3://path/to/query/bucket/`

        Returns
        -------
        tuple[float, float]
            runtime_sec, data_scanned_mb
        """
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': output_location, # S3 bucket to store results
            }
        )
        runtime_sec, data_scanned_mb = self.wait_for_query_to_complete(
            client = client,
            query_execution_id = response['QueryExecutionId'],
            logger = logger
        )
        return runtime_sec, data_scanned_mb
    
    ##########################################################################
    
    @staticmethod
    def wait_for_query_to_complete(
            client: boto3, 
            query_execution_id: str,
            logger: Logger,
    ) -> tuple[float, float]:
        """Check the status of query and wait 'till it's done

        Parameters
        ----------
        client : boto3
            Athena client
        query_execution_id : str
            Query's id from `start_query_execution`
        logger : Logger
            Logger object

        Returns
        -------
        tuple[float, float]
            runtime_sec, data_scanned_mb

        Raises
        ------
        SystemError
            If status is `FAILED` and `CANCELLED`
        """
        # While util FAILED, CANCELLED or SUCCEEDED status
        while True:
            response = client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            status = response['QueryExecution']['Status']['State']
            
            if status == 'SUCCEEDED':
                logger.info("Query succeeded!")
                # Extract runtime and data scanned
                query_stats = response['QueryExecution']['Statistics']
                runtime_ms = query_stats['EngineExecutionTimeInMillis']  
                data_scanned_bytes = query_stats['DataScannedInBytes']  
                
                # Convert runtime to seconds and data scanned to MB
                runtime_sec = runtime_ms / 1000.0
                data_scanned_mb = data_scanned_bytes / (1024 * 1024)
                return runtime_sec, data_scanned_mb
            
            # If error or cancel raise error
            elif status == 'FAILED':
                raise SystemError("Query failed!")
            elif status == 'CANCELLED':
                raise SystemError("Query was cancelled!")

            # Wait for next status
            else:
                logger.info("Query is still running, waiting...")
                time.sleep(5)
    
    ##########################################################################
    
##############################################################################
