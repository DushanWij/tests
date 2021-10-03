import requests
import pandas as pd
import re
import hashlib
from urllib.parse import urlparse



username = 'RPOCYajLY1'
password = 'tzTOPt8x'
auth = (username, password)
headers = {'accept': "application/json"}
base_url_connection = 'https://api.performancehorizon.com'

def read_json(suffix):
    endpoint = f'{base_url_connection}{suffix}'
    r = requests.get(endpoint, auth=auth, headers=headers)
    return r.json()

def data_extraction(df_02, something: str = ''):
    return df_02.iloc[:, lambda df: df.columns.str.contains(something, case=False)]

# function for cleaning up websites
def _clean_up_websites(df_websites):
    df_websites.dropna(subset=['website_url'], inplace=True)
    # Drop any websites that have the wrong urls like the below types. Update the list monthly. Or find better rule.
    wrong_urls_list = [
        'http://no',
        'http://na',
        'http://n/a',
        'http://www',
        'http://none',
        'http://www.f',
        'http://wwwww',
        'http://123456',
    ]
    df_websites = df_websites[~df_websites['website_url'].isin(wrong_urls_list)]
    df_websites = df_websites[~df_websites['website_url'].str.contains('gmail.com')]
    df_websites.reset_index(drop=True, inplace=True)
    return df_websites

# establishing connection to extract json
json_campaign = read_json('/campaign/')

# extracting campaign data
df = pd.json_normalize(json_campaign, 'campaigns')
df = df[['campaign.campaign_id', 'campaign.title']]


# establishing connection to extract json
json_publishers = read_json('/user/publisher/')

# list of all publishers
df_original_publishers = pd.json_normalize(json_publishers, 'publishers')

# extracting meta data of all partners
df_meta_data = data_extraction(df_original_publishers, 'websites')

# extracting websites for each publisher_id
df_websites = data_extraction(df_original_publishers, 'websites')
df_websites = df_websites.explode('publisher.websites')
df_websites = df_websites['publisher.websites'].apply(pd.Series)
df_websites = _clean_up_websites(df_websites)
cols = ['publisher_id', 'website_id', 'website_url', 'description', 'website_type_name',
        'website_vertical_name', 'website_country']
df_websites = df_websites[cols]

# publishers cleaning
df_publishers = pd.concat([df_original_publishers, df_meta_data], axis=1)
df_publishers = df_publishers.rename(
    columns={
    'publisher.publisher_id':'publisher_id', 'publisher.contact_name':'contact_name', 'publisher.account_name':'account_name',
    'publisher.contact_email':'email', 'publisher.im_username':'username', 'publisher.company_name':'company_name',
    'publisher.created':'creation_date', 'publisher.operating_country':'country', 'publisher.promotional_method':'promotional_method', 'publisher.meta.transferwise_existing_customer':'meta_existing_customer',
    'publisher.meta.transferwise_invite_a_friend_link':'meta_invite', 'publisher.meta.transferwise_used_invite_a_friend':'meta_used_invite',
    'publisher.meta.transferwise_requested_paid_currency':'meta_req_paid_currency',
    'publisher.meta.transferwise_site_promotional_category':'meta_site_prom_cat', 'publisher.meta.transferwise_how_did_you_hear_about_our_program':'meta_hdyhau'})

df_publishers = df_publishers[
    ['publisher_id', 'contact_name', 'account_name', 'email', 'username', 'company_name', 'creation_date', 'country',
     'promotional_method', 'meta_existing_customer', 'meta_invite', 'meta_used_invite', 'meta_req_paid_currency',
     'meta_site_prom_cat', 'meta_hdyhau']]

# encoding PII
df_publishers['email'] = df_publishers['email'].apply(lambda email: email.encode('utf-8'))
df_publishers['email'] = df_publishers['email'].apply(
    lambda email: hashlib.sha256(email).hexdigest())
df_publishers['email'] = df_publishers['email'].astype(str)
