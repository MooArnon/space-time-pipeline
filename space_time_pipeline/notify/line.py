##########
# Import #
##############################################################################

import requests

from .__base import BaseNotify

###########
# Classes #
##############################################################################

class LineNotify(BaseNotify):
    
    url = 'https://notify-api.line.me/api/notify'
    
    def __init__(
            self,
            token: str, 
    ) -> None:
        self.set_token(token)
        
        self.headers = {
            'content-type':'application/x-www-form-urlencoded',
            'Authorization':'Bearer ' + self.token
        }
    
    ##############
    # Properties #
    ##########################################################################
    
    @property
    def token(self) -> str:
        return self.__token
    
    ##########################################################################
    
    def set_token(self, token: str) -> None:
        """The Line token

        Parameters
        ----------
        token : str
            Line token
        """
        self.__token = token
    
    ##########
    # Method #
    ##########################################################################
    
    def sent_message(self, msg: str):
        requests.post(
            self.url, 
            headers = self.headers, 
            data = {'message':msg}
        )
    
    ##########################################################################

##############################################################################
