import pandas as pd
import json
import logging
import awswrangler as wr
from space_time_pipeline.data_lake_house import Athena

# Configurations
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

###########
# Statics #
##############################################################################

table_name = "fact_raw_data"
bucket_name = "space-time-lake-house"

s3_path = f"s3://{bucket_name}/{table_name}"
s3_path_tmp = f"{s3_path}_tmp"

# Load Data
with open("data/btc.json", encoding="utf-8") as file:
    data = json.load(file)['btc']

df = pd.DataFrame(data).head(10)

partition_columns = ["asset", "scraped_date"]
data_schema = {
    "id": "int64",
    "scraped_timestamp": "datetime64[ns]",
    "scraped_date": "datetime64[ns]",
    "price": "float64",
    "source_id": "int32" ,
    "engine_id": "int32" ,
    "created_timestamp": "datetime64[ns]",
    "updated_timestamp": "datetime64[ns]",
}

##############################################################################

athena = Athena(logger = logger)

"""
athena.insert_to_table(
    df = df,
    data_schema = data_schema,
    table_name = table_name,
    partition_columns = partition_columns,
    s3_path = s3_path,
)
"""

data = athena.select(
    replace_condition_dict = {
        "<ASSET>": "BTCUSDT", 
        "<LOWER_TIMESTAMP_BOUNDARY>": "2024-09-09 00:07:28",
        "<LIMIT>": 10,
    },
    database = "warehouse",
    query_file_path = "run_local_data_lake_house.sql",
    
)

data = pd.DataFrame(data)
print(data)
##############################################################################
