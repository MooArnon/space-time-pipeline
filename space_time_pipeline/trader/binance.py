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
        time.sleep(3)
        
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
    
    def update_dynamic_stop_loss(
            self,
            symbol: str,
            trailing_levels: list = [
                (3, 1.5), 
                (5, 3),
                (7, 5),
                (9, 7),
                (11, 9),
                (15, 12),
                (20, 18),
                (25, 20),
                (30, 25),
                (40, 35),
                (50, 45),
                (60, 55),
                (70, 65),
                (80, 75),
                (90, 85),
            ]
        ) -> None:
        """
        Dynamically adjust stop-loss based on current unrealized profit thresholds.
        
        If an existing stop-loss is already set within the correct range (within tolerance)
        or if updating would reduce your locked-in profit, no new stop-loss order is created.

        Parameters
        ----------
        symbol : str
            Futures symbol (e.g., "BTCUSDT")
        trailing_levels : list of (float, float)
            Pairs of (profit_threshold%, new_stop_loss%), e.g. [(3, 1.5), (5, 3)] means:
                - If position is +3% in profit, move stop-loss to +1.5%
                - If position is +5% in profit, move stop-loss to +3%
        """
        try:
            # 1. Get position info
            position_info = self.client.futures_position_information(symbol=symbol)[0]
            roi = self.get_roi(symbol, position_info)  # ROI is returned in percentage

            position_amt = float(position_info['positionAmt'])
            entry_price = float(position_info['entryPrice'])

            # If no open position or entry_price is 0, do nothing
            if position_amt == 0.0 or entry_price == 0.0:
                self.logger.info(f"No open position for {symbol} or entry_price=0.")
                return

            # 2. Determine if LONG or SHORT
            side = "LONG" if position_amt > 0 else "SHORT"

            # 3. Get current mark price for logging
            mark_price_data = self.client.futures_mark_price(symbol=symbol)
            mark_price = float(mark_price_data['markPrice'])

            # Use roi as the current profit percentage
            profit_pct = roi  # already in %

            self.logger.info(
                f"Position side: {side}, Entry: {entry_price}, Mark: {mark_price}, "
                f"Unrealized PnL% ~ {profit_pct:.2f}"
            )

            if profit_pct <= 0:
                self.logger.info("Currently not in profit, skipping trailing stop update.")
                return

            # 4. Check open orders for an existing stop-loss order
            open_orders = self.client.futures_get_open_orders(symbol=symbol)
            current_sl_order = None
            for order in open_orders:
                if order.get('type') in ['STOP_MARKET', 'STOP']:
                    current_sl_order = order
                    break

            # 5. Determine which trailing level to trigger
            trailing_levels_sorted = sorted(trailing_levels, key=lambda x: x[0])
            triggered_level = None
            
            for (profit_trigger, stop_loss_level) in trailing_levels_sorted:
                if profit_pct >= profit_trigger:
                    triggered_level = (profit_trigger, stop_loss_level)
                else:
                    break

            if not triggered_level:
                self.logger.info(
                    f"Current profit {profit_pct:.2f}% has not reached the lowest threshold "
                    f"{trailing_levels_sorted[0][0]}%."
                )
                return

            # 6. Compute the new stop-loss price based on the triggered trailing level.
            stop_loss_target = triggered_level[1]  # in percent
            if side == "LONG":
                # For LONG: new_sl_price = entry_price * (1 + (stop_loss_target/100))
                new_sl_price = entry_price * (1 + (stop_loss_target / 100 / self.get_leverage(symbol)))
            else:
                # For SHORT: new_sl_price = entry_price * (1 - (stop_loss_target/100))
                new_sl_price = entry_price * (1 - (stop_loss_target / 100 / self.get_leverage(symbol)))
            
            # 7. Round to precision (assuming you have a function to do so)
            new_sl_price = self.round_down_to_precision(new_sl_price)

            # Additional check: ensure that updating stop-loss doesn't reduce locked profit.
            # For LONG: new SL should be >= current SL, for SHORT: new SL should be <= current SL.
            if current_sl_order:
                current_sl_price = float(current_sl_order.get('stopPrice', 0))
                if side == "LONG" and new_sl_price < current_sl_price:
                    self.logger.info(
                        f"New stop-loss price {new_sl_price} is lower than current SL {current_sl_price}. "
                        "No update necessary as it would reduce locked profit."
                    )
                    return
                elif side == "SHORT" and new_sl_price > current_sl_price:
                    self.logger.info(
                        f"New stop-loss price {new_sl_price} is higher than current SL {current_sl_price}. "
                        "No update necessary as it would reduce locked profit."
                    )
                    return

            self.logger.info(
                f"Triggered level {triggered_level[0]}% => Adjusting stop-loss to lock in "
                f"{triggered_level[1]}% profit. New SL Price ~ {new_sl_price}"
            )

            # 8. If an existing SL order is already within the correct range, do nothing.
            tolerance = 0.0001  # Adjust tolerance based on the precision of your asset
            if current_sl_order:
                current_sl_price = float(current_sl_order.get('stopPrice', 0))
                if abs(current_sl_price - new_sl_price) < tolerance:
                    self.logger.info(
                        f"Existing stop-loss order at {current_sl_price} is within tolerance of new SL {new_sl_price}. "
                        "No update necessary."
                    )
                    return

            # 9. Cancel any existing stop-loss order
            if current_sl_order:
                try:
                    cancel_response = self.client.futures_cancel_order(
                        symbol=symbol,
                        orderId=current_sl_order['orderId']
                    )
                    self.logger.info(f"Canceled existing stop-loss order: {cancel_response}")
                except BinanceAPIException as e:
                    self.logger.warning(f"Could not cancel existing stop-loss order: {e}")

            # 10. Place the updated stop-loss order.
            # For a LONG position, the stop-loss order side is SELL;
            # for a SHORT position, it is BUY.
            sl_side = Client.SIDE_SELL if side == "LONG" else Client.SIDE_BUY
            try:
                position_size = abs(position_amt)
                new_stop_loss_order = self.client.futures_create_order(
                    symbol=symbol,
                    side=sl_side,
                    type=Client.FUTURE_ORDER_TYPE_STOP_MARKET,
                    stopPrice=new_sl_price,
                    quantity=position_size,
                    reduceOnly=True,
                    timeInForce=Client.TIME_IN_FORCE_GTC
                )
                self.logger.info(f"Updated stop-loss order placed: {new_stop_loss_order}")
            except BinanceAPIException as e:
                self.check_and_create_stop_loss(symbol=symbol)
                if "Order would immediately trigger" in str(e):
                    self.logger.error(
                        f"Stop-loss at {new_sl_price} is too close or above current price. "
                        "Consider adjusting thresholds or handling with incremental logic."
                    )
                else:
                    self.logger.error(f"Error placing updated stop-loss: {e}")
                traceback.print_exc()
        except Exception as e:
            self.logger.error(f"Unexpected error placing new stop-loss: {e}")
            self.check_and_create_stop_loss(symbol=symbol)
            traceback.print_exc()

    ##########################################################################
    
    def get_roi(self, symbol: str, position_info: dict) -> float:
        """
        Calculate the percentage ROI for the open futures position on the given symbol.

        For a LONG position:
        ROI = ((mark_price - entry_price) / entry_price) * 100
        For a SHORT position:
        ROI = ((entry_price - mark_price) / entry_price) * 100

        Parameters
        ----------
        symbol : str
            Futures symbol (e.g., "BTCUSDT")

        Returns
        -------
        float
            The ROI in percentage. Returns 0.0 if there is no open position.
        """
        # Fetch current position details for the symbol
        position_amt = float(position_info['positionAmt'])
        entry_price = float(position_info['entryPrice'])
        leverage = self.get_leverage(symbol=symbol)
        
        # If there's no open position, return 0.0 ROI
        if position_amt == 0 or entry_price == 0:
            return 0.0

        # Get the current mark price
        mark_price_data = self.client.futures_mark_price(symbol=symbol)
        mark_price = float(mark_price_data['markPrice'])

        # Calculate ROI based on the position side
        if position_amt > 0:  # LONG position
            roi = leverage * (mark_price - entry_price) / entry_price * 100
        else:  # SHORT position
            roi = leverage * (entry_price - mark_price) / entry_price * 100

        return roi

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
            positions_info = self.client.futures_position_information(symbol=symbol)[0]
            position_amt = 0.0
            entry_price = 0.0

            position_amt = float(positions_info['positionAmt'])
            entry_price = float(positions_info['entryPrice'])

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
    
    def check_position_opening(
            self,
            symbol: str,
    ) -> dict:
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
        # Get the current position for the symbol
        positions_info = self.client.futures_position_information(symbol=symbol)[0]
        position_amt = 0.0
        entry_price = 0.0
        
        position_amt = float(positions_info['positionAmt'])
        entry_price = float(positions_info['entryPrice'])

        # If no open position, exit the function
        if position_amt == 0.0:
            self.logger.info(f"No open position for {symbol}")
            return

        self.logger.info(
            f"Open position for {symbol}: {position_amt} at entry price {entry_price}"
        )
        
        if position_amt > 0:
            position = "LONG"
        elif position_amt < 0:
            position = 'SHORT'
        elif position_amt == 0:
            position = 'HOLD'
        else:
            self.logger.info(f"Position is wrong with position_amt = {position_amt}")
            raise SystemError
        
        return position

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
