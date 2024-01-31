#--------#
# Import #
#----------------------------------------------------------------------------#

import os
import json
import logging

import pandas as pd

from space_time_pipeline.data_warehouse import MySQLDataWarehouse

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

sql = MySQLDataWarehouse(
    host = os.environ.get('DB_HOST'),
    user = os.environ.get('DB_USERNAME'),
    password = os.environ.get('DB_PASSWORD'),
    database = os.environ.get('DB_NAME'),
)

sql.iterative_read_insert_json(
    json_dir_path = "tmp_scrape",
    table_name = "staging__fact_raw_data",
    rename_dict = rename_dict,
    logger = logger
)

#----------------------------------------------------------------------------#