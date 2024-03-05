from space_time_pipeline.trading_bot import BinanceTradingBot

bot = BinanceTradingBot()

client = bot.client

print(client.account())