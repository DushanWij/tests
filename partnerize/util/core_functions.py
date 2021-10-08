from typing import Dict, Union, Type

import requests
import pandas as pd


def check_status_code(response: Type[requests.models.Response]) -> Union[Dict,
                                                                         Type[requests.exceptions.HTTPError]]:  # https://docs.python.org/3/library/typing.html#typing.Union
    """

    Parameters
    ----------
    response: Type[requests.models.Response]
        Response is the object from `requests`.

    Returns
    -------
    df or Error: Union[pd.DataFrame,Type[requests.exceptions.HTTPError]]
        Return the dataframe from the response or return an error if the response is different than 200.
    """
    if response.status_code == 200:
        return response.json()
    else:
        raise requests.exceptions.HTTPError(f'The response code is incorrect: {response.status_code}')


def json_to_dataframe(dict_obj: Dict = None, column_name: str = None) -> pd.DataFrame:
    """

    Parameters
    ----------
    dict_obj
    column_name

    Returns
    -------

    """
    return pd.json_normalize(dict_obj, column_name)
