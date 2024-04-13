##########
# Import #
##############################################################################

import os
import json
import logging

import pandas as pd
from mysql.connector import Error

from space_time_pipeline.data_warehouse import (
    PostgreSQLDataWarehouse,
    MySQLDataWarehouse,
)

logger = logging.getLogger("TestDW")

##########
# Select #
##############################################################################
"""
change_name_dict = {
    "ASSETS":"'BTCUSDT'",
    "LIMIT": "5",
    "UPPER_DATE": "2024-02-01"
}

sql = PostgreSQLDataWarehouse()

data = sql.select(
    logger = logger,
    file_path = "run_local_data_warehouse.sql",
    # replace_condition_dict = change_name_dict
)

print(data.head(10))
"""

##############################################################################
"""
sql = PostgreSQLDataWarehouse()
change_name_dict = {
    "ASSETS":"'BTCUSDT'",
    "LIMIT": "5",
    "UPPER_DATE": "2024-02-01"
}
print("RUNNING execute_sql_file")
data = sql.execute_sql_file(
    logger = logger,
    file_path = "run_local_data_warehouse.sql",
    replace_condition_dict = change_name_dict
)
"""

##############################################################################
"""
rename_dict = {
    "timestamp": "scraped_timestamp",
    "source": "source_id",
    "engine": "engine_id"
}

sql = PostgreSQLDataWarehouse()

sql.iterative_read_insert_json(
    json_dir_path = "tmp_scrape",
    table_name = "staging.fact_raw_data",
    rename_dict = rename_dict,
    logger = logger
)
"""
"""
sql.execute_sql_file(logger=logger, file_path="run_local_data_warehouse.sql")
"""
##############################################################################

#################
# Select Insert #
##############################################################################

"""
postgresql = PostgreSQLDataWarehouse()
mysql = MySQLDataWarehouse()

data = postgresql.select(
    logger = logger,
    file_path = "run_local_data_warehouse.sql",
    # replace_condition_dict = change_name_dict
)
print(data.head(10))
try:
    # Assuming your table name is "your_table_name"
    table_name = "dim_model_type_weight"
    mysql.truncate_all_insert_data(
        table_name=table_name,
        df=data,
        logger=logger
    )
    print("Data inserted successfully into MySQL table")
except Error as e:
    print("Error inserting data into MySQL table:", e)
finally:
    mysql.close_connection()
"""

table_name = "aggregated_classifier_voted_evaluation"
postgresql = PostgreSQLDataWarehouse()
mysql = MySQLDataWarehouse()

data = postgresql.select(
    logger = logger,
    file_path = "run_local_data_warehouse.sql",
    # replace_condition_dict = change_name_dict
)
print(data.head(20))

# Assuming your table name is "your_table_name"
mysql.delete_insert_data(
    table_name=table_name,
    df=data,
    logger=logger,
    condition_lst=[
        "scraped_timestamp > '2024-03-12 00:00:00'",
        "scraped_timestamp < '2024-03-12 00:00:00'",
    ],
)

print("Data inserted successfully into MySQL table")
