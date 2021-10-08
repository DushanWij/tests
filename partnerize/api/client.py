"""Client connecting to Partnerize."""

__author__ = 'your name'

# Standard library
from typing import Dict, Type

# Third-part library
import requests
import requests.models

# Custom-library

# username = 'RPOCYajLY1'
# password = 'tzTOPt8x'


class Partnerize(object):

    HOST_NAME = 'https://api.performancehorizon.com'

    def __init__(self, username: str = None, password: str = None) -> None:
        """
        Initialise the partnerize class.

        Parameters
        ----------
        username: str
            Connection username.
        password: str
            Connection password.
        """

        self.headers = {'accept': 'application/json',
                        'content-type': 'application/json',
                        'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 '
                                      '(KHTML, like Gecko) Version/7.0.3 Safari/7046A194A'
                        }

        self.auth = (username, password)
        return

    def _make_request(self, endpoint: str = None) -> Type[requests.models.Response]:
        """
        Private method helping with all the requests on the various endpoints.

        Parameters
        ----------
        endpoint: str
            Endpoint defined as in the partnerize documentation.

        Returns
        -------
        requests.models.Response: requests.models.Response
            Returned object containing the response. It is an python object.
        """
        return requests.get(f'{self.HOST_NAME}{endpoint}', auth=self.auth, headers=self.headers)

    def get_campaigns(self):
        return self._make_request(endpoint='/campaign/')

    # TODO: get_publisher etc.
