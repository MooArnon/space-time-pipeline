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
    symbol="ETHUSDT",
    position_type="SHORT",
    tp_percent=5,
    sl_percent=5,
    leverage=30,
)


trader.close_all_positions("ETHUSDT")
"""

trader.check_and_create_stop_loss(
    "ETHUSDT",
    stop_loss_percent = 0.01,
    max_stop_loss_percent = 15,
)


print(trader.daily_pnl)