##########
# Import #
##############################################################################

from abc import abstractmethod

###########
# Classes #
##############################################################################

class BaseTrader:
    @abstractmethod
    def create_order() -> None:
        """Method for create order at market.
        Please also implement logic to create stop loss and take profit
        order.
        """
        raise NotImplementedError(
            """Child model does not implement `create_order`"""
        )

    ##########################################################################
    
    @abstractmethod
    def get_pnl() -> None:
        """Method to get current pnl.
        """
        raise NotImplementedError(
        """child model does not implement `close_all_order`
        """
        )
    
    ##########################################################################

##############################################################################
