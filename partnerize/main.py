"""This file will run all the main modules and functions."""

__author__ = 'your_name'

# Standard library
import logging
import sys
from typing import Type, Union

# Third party library

# Custom library
from api.client import Partnerize
from util.core_functions import check_status_code
from util.core_functions import json_to_dataframe


# instantiate connection a single time
connection = Partnerize(username='RPOCYajLY1', password='tzTOPt8x')

# extract campaign data
campaigns = connection.get_campaigns()  # request
campaigns = check_status_code(campaigns)  # check status - it fails here if the status is wrong
df_campaigns = json_to_dataframe(dict_obj=campaigns, column_name='campaigns')  # convert the dict to a json
df_campaigns = df_campaigns[['campaign.campaign_id', 'campaign.title']]
cols = ['campaign_id', 'title']
df_campaigns.columns = cols  # you rename the two columns
print(df_campaigns.head())
