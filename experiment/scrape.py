"""
from binance.client import Client
api_key = 's2XR9YXOSHUBKg4bi8e0YOWHpI4LDJV7ZaO09QvjPO0IEdqkCgBPcqIHyUfnJCC7'
api_secret = 'MwbVRSE3iBcPfzivOlcO2ogBzIX8Nr8FSK1yKMeQoqzQrIqRO5BdJzK7Xx4sSTtf'
client = Client(api_key, api_secret)

prices = client.get()
print(prices)
"""

# Import libraries 
import json 
import requests 

# Defining Binance API URL 
key = "https://api.binance.com/api/v3/ticker/price?symbol="

# Making list for multiple crypto's 
currencies = ["BTCUSDT", "DOGEUSDT", "LTCUSDT"] 

# running loop to print all crypto prices 
for currency in currencies: 
    
    # completing API for request 
    url = key+currency  
    data = requests.get(url) 
    data = data.json() 
    
    print(data)
    