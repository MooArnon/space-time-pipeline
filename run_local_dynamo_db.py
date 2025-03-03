from space_time_pipeline.nosql.dynamo_db import DynamoDB
import logging

##############################################################################

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Example usage:
if __name__ == '__main__':
    
    dynamo = DynamoDB(logger)
    
    # Insert
    prediction_data = {}
    asset = "BTCUSDT"
    model_type = 'GRU'
    prediction = {"value": 1, "confident": 0.6}
    table = 'predictions'
    """
    prediction_data['asset'] = asset
    prediction_data['model_type'] = 'GRU'
    prediction_data['asset'] = model_type
    prediction_data['prediction'] = prediction
    prediction_data['record_type'] = 'MODEL'
    
    response = upsert_prediction(
        table = table,
        prediction_data=prediction_data,
    )
    
    prediction = {"value": 1, "confident": 0.8}
    
    prediction_data = {}
    
    prediction_data['asset'] = asset
    prediction_data['model_type'] = 'GRU'
    prediction_data['prediction'] = prediction
    prediction_data['record_type'] = 'MODEL'
    
    prediction_data = dynamo.to_decimal(prediction_data)
    
    response = dynamo.ingest_data(table, item=prediction_data)
    # print(response)
    
    """
    key = {
        'asset': asset,
        'model_type': model_type
    }
    
    data = dynamo.query_data(table, key)
    print(data)
    
    dynamo.delete_data(table, key)
    """
    data = dynamo.query_data(table, key)
    print(data)
    """

##############################################################################
