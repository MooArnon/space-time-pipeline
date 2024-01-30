#--------#
# Import #
#----------------------------------------------------------------------------#

import os
import logging
from space_time_pipeline.data_lake import S3DataLake

#---------#
# Statics #
#----------------------------------------------------------------------------#

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
s3 = S3DataLake(logger)

#-----------#
# Procedure #
#----------------------------------------------------------------------------#
# Upload #
#--------#
"""
s3.upload_to_data_lake(
    s3_bucket = os.environ['BUCKET_RAW_DATA'],
    prefix = f"{os.environ['ENV_STATE']}/journal/raw/raw_asset_data/to_be_processed",
    target_dir = "tmp_scrape",
)
"""

#----------------------------------------------------------------------------#
# Move file #
#-----------#
"""
s3.move_file(
    source_bucket = os.environ['BUCKET_RAW_DATA'],
    source_path = f"{os.environ['ENV_STATE']}/journal/raw/raw_asset_data/to_be_processed/BTCUSDT_20240129_191815.json",
    destination_bucket = os.environ['BUCKET_RAW_DATA'],
    destination_path = f"{os.environ['ENV_STATE']}/journal/raw/raw_asset_data/processing/BTCUSDT_20240129_191815.json",
    logger = logger
)
"""

#----------------------------------------------------------------------------#
# Download file #
#---------------#
"""
s3.download_file(
    bucket_name = os.environ['BUCKET_RAW_DATA'],
    target_prefix = f"{os.environ['ENV_STATE']}/journal/raw/raw_asset_data/processing",
    logger = logger
)
"""
#----------------------------------------------------------------------------#
