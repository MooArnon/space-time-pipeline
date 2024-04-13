##########
# Import #
##############################################################################

import os
import logging
from space_time_pipeline.data_lake import DOSpacesDataLake

###########
# Statics #
##############################################################################

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
spaces = DOSpacesDataLake(logger)

raw_target_prefix_to_be_processed = r"journal/raw/raw_asset_data/to_be_processed/"
raw_target_prefix_processing = r"journal/raw/raw_asset_data/processing/"

#############
# Procedure #
##############################################################################
# Upload #
##########

spaces.upload_to_data_lake(
    spaces_bucket = os.environ['BUCKET_RAW_DATA'],
    prefix = "journal/raw/raw_asset_data/to_be_processed",
    target_dir = "tmp_scrape",
)


##############################################################################
# Move file #
#############

spaces.move_file(
        source_bucket = os.environ['BUCKET_RAW_DATA'],
        source_path = raw_target_prefix_to_be_processed,
        destination_bucket = os.environ['BUCKET_RAW_DATA'],
        destination_path = raw_target_prefix_processing,
        logger = logger
    )

##############################################################################
# Download file #
#################

spaces.download_file(
    bucket_name = os.environ['BUCKET_RAW_DATA'],
    target_prefix = raw_target_prefix_processing,
    logger = logger
)

##############################################################################
