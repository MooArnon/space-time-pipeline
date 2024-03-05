#--------#
# Import #
#----------------------------------------------------------------------------#

import os 

from binance.cm_futures import CMFutures

from .__base import BaseTradingBot

#---------#
# Classes #
#----------------------------------------------------------------------------#

class BinanceTradingBot(BaseTradingBot):
    def __init__(
            self,
            api_key: str = os.environ.get('BINANCE_API_KEY'), 
            api_secret: str = os.environ.get('BINANCE_SECRET'), 
    ) -> None:
        self.set_client(api_key, api_secret)
    
    #------------#
    # Properties #
    #------------------------------------------------------------------------#
    
    @property
    def client(self) -> CMFutures:
        return self.__client
    
    #------------------------------------------------------------------------#
    
    def set_client(self, api_key: str, api_secret: str) -> None:
        """Set client attribute

        Parameters
        ----------
        api_key : str
            from Binance
        api_secret : str
            from Binance
        """
        cm_futures_client = CMFutures(key=api_key, secret=api_secret)
        self.__client = cm_futures_client
    
    #--------------#
    # Create order #
    #------------------------------------------------------------------------#
    
    def create_order(self, payload: dict) -> None:
        """_summary_

        Parameters
        ----------
        payload : dict
            _description_
        """
        params = {
            'symbol': 'BTCUSDT',
            'side': 'SELL',
            'type': 'LIMIT',
            'timeInForce': 'GTC',
            'quantity': 0.002,
            'price': 59808
        }

        response = self.client.new_order(**params)
        print(response)
    
    #------------------------------------------------------------------------#
    
    
    
    #------------------------------------------------------------------------#
    
#----------------------------------------------------------------------------#
