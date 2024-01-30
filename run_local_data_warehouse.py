#--------#
# Import #
#----------------------------------------------------------------------------#

import os

from space_time_pipeline.data_warehouse import MySQLDataWarehouse

#--------#
# Select #
#----------------------------------------------------------------------------#

"""
sql = MySQLDataWarehouse()

data = sql.select("run_local_data_warehouse.sql")

print(data.head(10))
"""

#----------------------------------------------------------------------------#

sql = MySQLDataWarehouse()

data = sql.execute_sql_file("run_local_data_warehouse.sql")

#----------------------------------------------------------------------------#
