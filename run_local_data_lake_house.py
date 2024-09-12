from datetime import datetime, timezone

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
"""
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


athena.insert_to_table(
    df = df,
    data_schema = data_schema,
    table_name = table_name,
    partition_columns = partition_columns,
    s3_path = s3_path,
)
"""

##############################################################################

data_schema = {
    "model_type": "string",
    "asset": "string",
    "corrected_prediction": "int32",
    "total_predicted": "int32",
    "accuracy": "float32" ,
    "weight": "float32" ,
}

partition_columns = ["asset"]
table_name = "aggregated_classifier_weight"
bucket_name = "space-time-lake-house"
s3_path = f"s3://{bucket_name}/{table_name}"

athena = Athena(logger = logger)

data = athena.select(
    replace_condition_dict = {
        "<ASSET>": "BTCUSDT", 
        "<NUMBER_LOOK_BACK_DATE>": "1",
        "<EVALUATION_RANGE>": 15,
        "<LIMIT>": 1000
    },
    database = "warehouse",
    query_file_path = "run_local_data_lake_house.sql",
)

data = pd.DataFrame(data)

athena.insert_to_table(
    df = data,
    data_schema = data_schema,
    table_name = table_name,
    partition_columns = partition_columns,
    s3_path = s3_path,
)
##############################################################################
