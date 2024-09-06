##########
# Import #
##############################################################################

import awswrangler as wr
from logging import Logger
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
    
##############################################################################
