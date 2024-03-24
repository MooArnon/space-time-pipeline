import os

from binance.client import Client 
import pandas as pd

api_key =  os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_SECRET")

client = Client(api_key, api_secret)

symbol = "BTCUSDT" 
interval='1h' 
start_date = "1 Jan,2018"
klines = client.futures_historical_klines(
    symbol, 
    interval, 
    start_date, 
) 

data = pd.DataFrame(klines)

data.columns = [
    'datetime', 
    'open', 
    'high', 
    'low', 
    'close', 
    'volume',
    'close_time', 
    'qav', 
    'num_trades',
    'taker_base_vol',
    'taker_quote_vol', 
    'ignore'
]

data['datetime'] = pd.to_datetime(data['datetime'], unit='ms')

data.to_csv("BTCUSDT_future_1-jam-23.csv")