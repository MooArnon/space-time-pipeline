##########
# Import #
##############################################################################

from space_time_pipeline.trader.binance import BinanceTrader

##############################################################################

trader = BinanceTrader()

available_balance = trader.available_balance
print(f"Available Balance: {available_balance}")

trader.client.futures_account_balance
"""
trader.create_order(
    symbol="BTCUSDT",
    position_type="LONG",
    tp_percent=10,
    sl_percent=5,
    leverage=8,
)
"""
# trader.close_all_positions("ETHUSDT")


"""
trader.check_and_create_stop_loss(
    "ETHUSDT",
    stop_loss_percent = 0.01,
    max_stop_loss_percent = 15,
)
"""

position = trader.check_position_opening(symbol="BTCUSDT")
print(position)
