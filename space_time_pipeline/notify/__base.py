##########
# Import #
##############################################################################

from abc import abstractmethod

###########
# Classes #
##############################################################################

class BaseNotify:
    
    def __init__(self) -> None:
        pass
    
    ##########################################################################
    
    @abstractmethod
    def sent_message(message: str) -> None:
        """Sent  the message"""
    
    ########
    # Body #
    ##########################################################################
    # Signal #
    ##########
    
    def multi_signal_body(self, asset: str, signal_list: list[dict]) -> str:
        """Create the multiple signal for all models

        Parameters
        ----------
        asset : str
            Name of asset
        signal_list : list[dict]
            List of signal object

        Returns
        -------
        str
            Full body
        """
        body = f"asset: {asset}"
        
        # Iterate over signal_list
        for object in signal_list:
            
            # Construct the signal element
            body += self.single_signal_body(
                model = object["model"], 
                signal = object["signal"], 
            )
        
        return body
    
    ##########################################################################
    
    @staticmethod
    def single_signal_body(model: str, signal: str) -> str:
        """Create body for single signal object
        
        Parameters
        ----------
        model : str
            Name of model
        signal : list[dict]
            signal

        Returns
        -------
        str
            element body
        """
        body = f"""
        model: {model}
        signal: {signal}
        {'---'*5}
        """
        return body
    
    ##########################################################################

##############################################################################
