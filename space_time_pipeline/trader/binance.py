##########
# Import #
##############################################################################

from datetime import datetime, timezone
import os
import logging
import math
import traceback
import time

from binance.client import Client
from binance.exceptions import BinanceAPIException

from .__base import BaseTrader

###########
# Classes #
##############################################################################

class BinanceTrader(BaseTrader):
    
    def __init__(
            self,
            api_key: str = os.environ["BINANCE_API_KEY"],
            secret_key: str = os.environ["BINANCE_SECRET_KEY"],
            logger: logging.Logger = None
    ) -> None:
        super().__init__()
        self.client = Client(api_key, secret_key)
        
        # Set logger if None
        if not logger:
            self.logger = logging.getLogger(__name__)
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s'
            )
        if logger:
            self.logger = logger
    
    ##############
    # Properties #
    ##########################################################################
    
    @property
    def get_leverage(self, symbol: str) -> int:
        try:
            response = self.client.futures_position_information(symbol=symbol)
            for position in response:
                if position['symbol'] == symbol:
                    return int(position['leverage'])
            return None
        except Exception as e:
            print(f"Error getting leverage: {e}")
            return None

    ##########################################################################
    
    @property
    def available_balance(self, asset: str = "USDT") -> float:
        """Return the amount balance for asset `asset`

        Parameters
        ----------
        asset : str, optional
            Target asset, by default "USDT"

        Returns
        -------
        float
            Float of available balance
        """
        future_account_asset = self.client.futures_account()['assets']
        return float(
            next(
                item['availableBalance'] \
                    for item in future_account_asset \
                        if item['asset'] == asset
            )
        )
    
    ##########################################################################
    
    @property
    def daily_pnl(self) -> float:
        return self.calculate_pnl()
    
    ##########################################################################
    
    def set_leverage(self, symbol: str, leverage: int) -> dict:
        """Set leverage  of symbol

        Parameters
        ----------
        client : Client
            Client for binance
        symbol : str
            Target symbol
        leverage : int
            Leverage to adjust

        Returns
        -------
        dict
            respond
        """
        response = self.client.futures_change_leverage(
            symbol=symbol, 
            leverage=leverage,
        )
        return response
    
    ################
    # Main methods #
    ##########################################################################
    # Create order #
    ################
    
    def create_order(
            self, 
            symbol: str, 
            position_type: str, 
            tp_percent: float, 
            sl_percent: float,
            leverage: int,
    ) -> None:
        """Create order using current price.
        
        Cancel all open orders and positions, then the create new one.
        This method also create stop-loss and take-profit.

        Parameters
        ----------
        symbol : str
            Target symbol to crate order
        position_type : str
            Type of position, `LONG` or `SHORT`
        tp_percent : float
            Percentage for take profit, calculated from current
            price and leverage
        sl_percent : float
            Percentage for stop loss, calculated from current
            price and leverage
        leverage : int
            Multiplier open unit

        Raises
        ------
        ValueError
            If position type is not correct.
        """
        # Clear all past orders and position
        # Delay 5 second to ensure the respond from server
        self.cancel_all_open_orders(symbol)
        self.close_all_positions(symbol)
        time.sleep(5)
        
        try:
            if position_type not in ('LONG', 'SHORT'):
                raise ValueError(f"{position_type} does not suitable")
            
            # Get available USDT balance
            # Use only 95 % of balance to prevent insufficient margin
            usdt_balance = self.available_balance * 0.95
            self.logger.info(f"Available USDT balance: {usdt_balance}")

            # Calculate the maximum quantity based on USDT balance
            # Also round the precision
            self.set_leverage(symbol, leverage)
            quantity = self.calculate_quantity(symbol, usdt_balance, leverage)
            quantity = self.round_to_precision(quantity)
            
            # Get current price
            ticker = self.client.futures_symbol_ticker(symbol=symbol)
            price = float(ticker['price'])
            price = self.round_to_precision(price)
            self.logger.info(f"Maximum quantity to buy: {quantity}")
            
            # Calculate TP and SL prices
            take_profit_price, stop_loss_price = self.calculate_tp_sl_prices(
                entry_price=price,
                tp_percent=tp_percent,
                sl_percent=sl_percent,
                position_type=position_type,
                leverage = leverage,
            )
            self.logger.info(f"Take Profit price: {take_profit_price}")
            self.logger.info(f"Stop Loss price: {stop_loss_price}")
            
            # Place a Market Order (long or short based on position_type)
            if position_type == 'LONG':
                market_order = self.client.futures_create_order(
                    symbol=symbol,
                    side=Client.SIDE_BUY,
                    type=Client.ORDER_TYPE_LIMIT,
                    quantity=quantity,
                    price=price,
                    timeInForce=Client.TIME_IN_FORCE_GTC,
                )
            elif position_type == 'SHORT':
                market_order = self.client.futures_create_order(
                    symbol=symbol,
                    side=Client.SIDE_SELL,
                    type=Client.ORDER_TYPE_LIMIT,
                    quantity=quantity,
                    price=price,
                    timeInForce=Client.TIME_IN_FORCE_GTC,
                )
            self.logger.info(f"Market order placed: {market_order}")

            # Place a Take Profit order
            tp_order = self.client.futures_create_order(
                symbol=symbol,
                side=Client.SIDE_SELL if position_type == 'LONG' else Client.SIDE_BUY,
                type=Client.FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET,
                quantity=quantity,
                stopPrice=take_profit_price,
                reduceOnly=True,
                timeInForce=Client.TIME_IN_FORCE_GTC,
            )
            self.logger.info(f"Take Profit order placed: {tp_order}")

            # Place a Stop Loss order
            sl_order = self.client.futures_create_order(
                symbol=symbol,
                side=Client.SIDE_SELL if position_type == 'LONG' else Client.SIDE_BUY,
                type=Client.FUTURE_ORDER_TYPE_STOP_MARKET,
                quantity=quantity,
                stopPrice=stop_loss_price,
                reduceOnly=True,
                timeInForce=Client.TIME_IN_FORCE_GTC,
            )
            self.logger.info(f"Stop Loss order placed: {sl_order}")
            
        except Exception as e:
            self.logger.info(traceback.format_exc())
            self.logger.info(f"Error placing orders: {e}")
    
    ##########################################################################
    # Manage position #
    ###################
    
    def close_all_positions(self, symbol: str):
        """Force close all positions

        Parameters
        ----------
        symbol : str
            Target asset
        """
        try:
            # Get the current positions information
            positions_info = self.client.futures_position_information()
            
            # Loop all position found
            for position in positions_info:
                if position['symbol'] == symbol:
                    
                    # Get the position amount (positive for long, negative for short)
                    position_amt = float(position['positionAmt']) 
                    
                    # Close long position by selling
                    if position_amt > 0:
                        self.logger.info(
                            f"Closing LONG position for {symbol}, Amount: {position_amt}"
                        )
                        close_order = self.client.futures_create_order(
                            symbol=symbol,
                            side=Client.SIDE_SELL,
                            type=Client.ORDER_TYPE_MARKET,
                            quantity=abs(position_amt)
                        )
                        self.logger.info(
                            f"Closed LONG position: {close_order}"
                        )
                    
                    # Close short position by buying
                    elif position_amt < 0:
                        self.logger.info(
                            f"Closing SHORT position for {symbol}, Amount: {position_amt}"
                        )
                        close_order = self.client.futures_create_order(
                            symbol=symbol,
                            side=Client.SIDE_BUY,
                            type=Client.ORDER_TYPE_MARKET,
                            quantity=abs(position_amt)  
                        )
                        self.logger.info(
                            f"Closed SHORT position: {close_order}"
                        )
                        
                    else:
                        self.logger.info(f"No open positions for {symbol}")
                        
        except BinanceAPIException as e:
            self.logger.info(f"Error closing position: {e}")
            traceback.print_exc()
        except Exception as e:
            self.logger.info(f"Unexpected error: {e}")
            traceback.print_exc()
    
    ##########################################################################
    
    def check_and_create_stop_loss(
        self,
        symbol: str,
        stop_loss_percent: int = 0.01,
        max_stop_loss_percent: int = 15,
        leverage: int = 20,
    ) -> None:
        """
        Check open positions and add stop loss

        Parameters
        ----------
        symbol : str
            Target symbol
        stop_loss_percent : int
            Initial percentage of stop loss threshold, default is 5
        max_stop_loss_percent : int
            Maximum percentage for stop loss, default is 15 
            (to prevent endless loop)
        leverage: int 
            default is 20
        """
        try:
            # Get the current position for the symbol
            positions_info = self.client.futures_position_information()
            position_amt = 0.0
            entry_price = 0.0

            # Loop over positions to find the symbol
            for position in positions_info:
                if position['symbol'] == symbol:
                    position_amt = float(position['positionAmt'])
                    entry_price = float(position['entryPrice'])
                    break

            # If no open position, exit the function
            if position_amt == 0.0:
                self.logger.info(f"No open position for {symbol}")
                return

            self.logger.info(
                f"Open position for {symbol}: {position_amt} at entry price {entry_price}"
            )

            # Check open orders to see if there's an existing stop-loss order
            open_orders = self.client.futures_get_open_orders(symbol=symbol)
            has_stop_loss = False
            for order in open_orders:
                if order['type'] == 'STOP_MARKET':
                    has_stop_loss = True
                    self.logger.info(f"Stop-loss already exists for {symbol}")
                    break

            # If a stop-loss exists, return without doing anything
            if has_stop_loss:
                return

            # If no stop-loss, calculate and create one with retry logic
            # Calculate stop-loss price based on position type (long/short)
            while stop_loss_percent <= max_stop_loss_percent:
                try:
                    
                    # Long position
                    if position_amt > 0: 
                        stop_loss_price = entry_price * (1 - (stop_loss_percent / 100)/leverage)
                        stop_loss_side = Client.SIDE_SELL
                    
                    # Short position
                    else: 
                        stop_loss_price = entry_price * (1 + (stop_loss_percent / 100)/leverage )
                        stop_loss_side = Client.SIDE_BUY

                    # Round stop-loss price to correct precision
                    stop_loss_price = self.round_down_to_precision(stop_loss_price)
                    self.logger.info(
                        f"Attempting to create stop-loss at {stop_loss_price} with {stop_loss_percent}% threshold for {symbol}"
                    )

                    # Place stop-loss order
                    stop_loss_order = self.client.futures_create_order(
                        symbol=symbol,
                        side=stop_loss_side,
                        type='STOP_MARKET',
                        stopPrice=stop_loss_price,
                        quantity=abs(position_amt)
                    )
                    self.logger.info(f"Stop-loss created successfully: {stop_loss_order}")
                    
                    # Exit the loop after successful order
                    break  

                except BinanceAPIException as e:
                    
                    # Check if the error is due to "Order would immediately trigger"
                    if "Order would immediately trigger" in str(e):
                        self.logger.warning(
                            f"Stop-loss failed: {e}. Increasing stop-loss percent to {stop_loss_percent + 0.5}%"
                        )
                        
                        # Increment stop loss by 0.5% and retry
                        stop_loss_percent += 0.5 
                    else:
                        self.logger.error(f"Error creating stop-loss: {e}")
                        traceback.print_exc()
                        
                        # Break loop for other API errors
                        break  
                except Exception as e:
                    self.logger.error(f"Unexpected error: {e}")
                    traceback.print_exc()
                    break

            if stop_loss_percent > max_stop_loss_percent:
                self.logger.error(
                    f"Failed to create stop-loss for {symbol} after reaching max stop-loss percent of {max_stop_loss_percent}%."
                )

        except BinanceAPIException as e:
            self.logger.error(f"Error checking/creating stop-loss: {e}")
            traceback.print_exc()
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            traceback.print_exc()

    ##########################################################################
    
    def cancel_all_open_orders(self, symbol: str):
        """Cancel all open orders

        Parameters
        ----------
        symbol : str
            Target symbol
        """
        try:
            # Cancel all open orders for the given symbol
            response = self.client.futures_cancel_all_open_orders(symbol=symbol)
            self.logger.info(
                f"Successfully canceled all open orders for {symbol}: {response}"
            )
        except BinanceAPIException as e:
            self.logger.info(f"Error cancelling orders: {e}")
            traceback.print_exc()
        except Exception as e:
            self.logger.info(f"Unexpected error: {e}")
            traceback.print_exc()

    #############
    # Utilities #
    ##########################################################################
    
    def calculate_quantity(
            self, 
            symbol: str, 
            usdt_balance: float, 
            leverage: int    
    ) -> float:
        """Calculate quantity to buy

        Parameters
        ----------
        symbol : str
            Target symbol to buy
        usdt_balance : float
            Balance to buy
        leverage : int
            Leverage to multiply quantity

        Returns
        -------
        float
            Float of quantity with 6th precision
        """
        # Fetch current price for the symbol (e.g., BTCUSDT)
        ticker = self.client.futures_symbol_ticker(symbol=symbol)
        current_price = float(ticker['price'])
        
        # Calculate maximum quantity you can buy with leverage
        quantity = (usdt_balance * leverage) / current_price
        return round(quantity, 6) 

    ##########################################################################
    
    @staticmethod
    def calculate_tp_sl_prices(
            entry_price: float, 
            tp_percent: float, 
            sl_percent: float, 
            position_type: str,
            leverage: int
    ) -> tuple[float, float]:
        """Return tuple(take-profit, stop-loss) with leverage factor.

        Parameters
        ----------
        entry_price : float
            Price placed at main order
        tp_percent : float
            Percentage for take profit
        sl_percent : float
            Percentage for stop loss
        position_type : str
            Type of position, LONG | SELL
        leverage : int
            Leverage to calculate

        Returns
        -------
        tuple[float, float]
        """
        # Calculate take profit 
        tp_price = \
            entry_price * (1 + (tp_percent / 100)/leverage) if position_type == 'LONG' \
                else entry_price * (1 - (tp_percent / 100)/leverage)
        
        # Calculate stop loss
        sl_price = \
            entry_price * (1 - (sl_percent / 100)/leverage) if position_type == 'LONG' \
                else entry_price * (1 + (sl_percent / 100)/leverage)
        return round(tp_price, 2), round(sl_price, 2)

    ##########################################################################
    
    @staticmethod
    def round_to_precision(value: float):
        return math.floor(value * 1000) / 1000
    
    ##########################################################################
    
    @staticmethod
    def round_down_to_precision(price):
        return round(price, 2)
    
    ##########################################################################
    
    def calculate_pnl(
            self, 
            end_time: datetime = None,  
            start_time: datetime = None,
    ) -> float:
        """Calculate PNL, if both `end_time` and `start_time` are none,
        it will use current timestamp as a end_time and 00:00:00
        of the same date as a `start_date`.

        Parameters
        ----------
        end_time : datetime
            End date in datetime object
            , by default is None
        start_time : datetime
            Start time in datetime object
            , by default is None

        Returns
        -------
        float
            PNL in usdt
        """
        try:
            
            # Get the current UTC time (end time)
            # Calculate the start time for the current day 
            # in UTC (midnight)
            if (end_time is None) and (start_time is None):
                end_time = datetime.now(timezone.utc)
                start_time = end_time.replace(
                    hour=0, 
                    minute=0,
                    second=0,
                    microsecond=0
                )
            
            # Convert datetime to milliseconds 
            # (Binance API uses timestamps in milliseconds)
            start_time_ms = int(start_time.timestamp() * 1000)
            end_time_ms = int(end_time.timestamp() * 1000)
            
            # Fetch daily income (realized PnL)
            pnl_data = self.client.futures_income_history(
                incomeType='REALIZED_PNL',
                startTime=start_time_ms,
                endTime=end_time_ms
            )
            
            # Calculate total daily PnL
            total_pnl = sum(float(entry['income']) for entry in pnl_data)
            
            self.logger.info(f"Total Daily PnL: {total_pnl} USDT")
            
            # Optionally, print detailed PnL entries
            for entry in pnl_data:
                timestamp = datetime.fromtimestamp(
                    entry['time'] / 1000, tz=timezone.utc
                )
                income = float(entry['income'])
                self.logger.info(f"Time: {timestamp}, PnL: {income} USDT")
            
            return total_pnl

        except Exception as e:
            self.logger.info(f"Unexpected error: {e}")
    
    ##########################################################################

##############################################################################
