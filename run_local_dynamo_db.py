from space_time_pipeline.nosql.dynamo_db import DynamoDB

##############################################################################

# Example usage:
if __name__ == '__main__':
    """
    # Insert
    prediction_data = {}
    asset = "BTCUSDT"
    prediction = {"value": 1, "confident": 0.6}
    
    prediction_data['asset'] = 'BTCUSDT'
    prediction_data['model_type'] = 'GRU'
    prediction_data['asset'] = 'BTCUSDT'
    prediction_data['prediction'] = prediction
    prediction_data['record_type'] = 'MODEL'
    
    response = upsert_prediction(
        table = 'predictions',
        prediction_data=prediction_data,
    )
    print("Upsert succeeded:", response)
    
    """
    dynamo = DynamoDB()
    table = 'predictions'
    model_type = 'GRU'
    prediction = {"value": 0, "confident": 0.8}
    
    prediction_data = {}
    
    prediction_data['asset'] = 'BTCUSDT'
    prediction_data['model_type'] = 'GRU'
    prediction_data['asset'] = 'BTCUSDT'
    prediction_data['prediction'] = prediction
    prediction_data['record_type'] = 'MODEL'
    
    prediction_data = dynamo.to_decimal(prediction_data)
    
    # response = dynamo.ingest_data(table, item=prediction_data)
    # print(response)
    
    
    key = {
        'asset': 'BTCUSDT',
        'model_type': 'GRU'
    }
    data = dynamo.query_data(table, key)
    print(data)

##############################################################################
