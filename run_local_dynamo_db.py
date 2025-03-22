from space_time_pipeline.nosql.dynamo_db import DynamoDB
import logging
from datetime import datetime, timezone

##############################################################################

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Example usage:
if __name__ == '__main__':
    current_timestamp = datetime.now(timezone.utc)
    model_type = [
        "random_forest",
        "xgboost",
        "logistic_regression",
        "gru",
        "lstm",
        "cnn",
        "dnn",
    ]
    
    dynamo = DynamoDB(logger)
    table = 'classifier_weight'
    for model in model_type:
        # Insert
        prediction_data = {
            "model_type": model,
            "asset": "BTCUSDT",
            "weight": "0.14",
            "created_timestamp": str(current_timestamp),
        }
        prediction_data = dynamo.to_decimal(prediction_data)
        
        response = dynamo.ingest_data('classifier_weight', item=prediction_data)
    # print(response)
    
    """
    key = {
        'asset': 'BTCUSDT',
        'model_type': 'xgboost'
    }
    
    # data = dynamo.query_data('predictions', key)
    # print(data)
    
    data = dynamo.print_all_records('predictions')
    print(data)
    
    
    dynamo.delete_data(table, key)

    
    data = dynamo.query_data(table, key)
    print(data)
    """

##############################################################################
