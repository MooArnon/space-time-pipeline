#--------#
# Import #
#----------------------------------------------------------------------------#

import os
import json
import logging

import pandas as pd

from space_time_pipeline.data_warehouse import PostgreSQLDataWarehouse

logger = logging.getLogger("TestDW")

#--------#
# Select #
#----------------------------------------------------------------------------#

"""
sql = MySQLDataWarehouse()

data = sql.select("run_local_data_warehouse.sql")

print(data.head(10))
"""

#----------------------------------------------------------------------------#

"""
sql = MySQLDataWarehouse()

data = sql.execute_sql_file("run_local_data_warehouse.sql")
"""

#----------------------------------------------------------------------------#

rename_dict = {
    "timestamp": "scraped_timestamp",
    "source": "source_id",
    "engine": "engine_id"
}

sql = PostgreSQLDataWarehouse()
"""
sql.iterative_read_insert_json(
    json_dir_path = "tmp_download",
    table_name = "staging.fact_raw_data",
    rename_dict = rename_dict,
    logger = logger
)
"""

sql.execute_sql_file(logger=logger, file_path="run_local_data_warehouse.sql")

#----------------------------------------------------------------------------#
