##########
# Import #
##############################################################################

import boto3
import datetime
from datetime import timezone
from decimal import Decimal
import functools

from .__base import BaseNoSQL

###########
# Classes #
##############################################################################

class DynamoDB(BaseNoSQL):
    def __init__(self):
        self.client = boto3.resource('dynamodb')
    
    ###########
    # Wrapper #
    ##########################################################################
    
    def capture_response_status(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            response = func(*args, **kwargs)
            
            # Retrieve the HTTP status code from the response metadata.
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode", None)
            
            # If status is missing or not in the 2xx range, raise an exception.
            if status is None or not (200 <= status < 300):
                raise Exception(
                    f"Operation failed with status code {status}. Response: {response}"
                )
            # Otherwise, return a dict containing the status and full response.
            return {"status": status, "response": response}
        return wrapper

    ##################
    # Data ingestion #
    ##########################################################################
    
    def query_data(self, table: str, key: dict) -> dict:
        """To query data at `table` with `key`

        Parameters
        ----------
        table : str
            Name of table
        key : dict
            Key that need to select

        Returns
        -------
        dict
            Matched key data
        """
        # Create a DynamoDB resource and reference the Predictions table.
        table = self.client.Table(table)
        
        # Retrieve the item with the given partition key and sort key.
        response = table.get_item(
            Key=key
        )
        
        # Return the item if it exists; otherwise, return None.
        return response.get('Item')
    
    ##########################################################################
    
    @capture_response_status
    def ingest_data(
            self,
            table: str, 
            item: dict, 
    ) -> dict:
        """To Insert or update data into dynamoDB
        Proceed insert when we dont have key value, included in item dict.
        And update at the existed value

        Parameters
        ----------
        table : str
            Name of table at dynamo
        item : dict
            Dict od item, must specify the key

        Returns
        -------
        dict
            Response from API
        """
        # Create a DynamoDB resource and reference the Predictions table.
        table = self.client.Table('predictions')
        
        # Get the current UTC time as an ISO 8601 formatted string.
        updated_at = datetime.datetime.now(tz=timezone.utc).isoformat()

        # Build the item with the required keys and additional attributes.
        item['updated_at'] = updated_at
        
        # Put the item into the table. This will overwrite an existing record with the same key.
        response = table.put_item(Item=item)
        return response

    ##########################################################################
    
    @capture_response_status
    def delete_data(self, table: str, item: dict) -> dict:
        """Delete data from `table`, by item key.

        Parameters
        ----------
        table : str
            Name of the table in DynamoDB.
        item : dict
            Dictionary of the item. Must specify the primary key attributes.

        Returns
        -------
        dict
            Response from the DynamoDB API.
        """
        # Create a DynamoDB resource and reference the table by the provided name.
        table_ref = self.client.Table(table)
        
        # Put the item into the table (this performs an upsert).
        response = table_ref.delete_item(Item=item)
        
        return response

    #############
    # Utilities #
    ##########################################################################
    
    def to_decimal(self, obj):
        """
        Recursively convert floats in the given object to Decimal.
        """
        if isinstance(obj, float):
            return Decimal(str(obj))
        elif isinstance(obj, dict):
            return {k: self.to_decimal(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.to_decimal(x) for x in obj]
        else:
            return obj
    
    ##########################################################################

##############################################################################
