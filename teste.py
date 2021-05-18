import json
import os
from os import path
import re
import urllib
from time import sleep

import dagster as dg
from dagster import check
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Response
from tqdm import tqdm
from urllib3.util import Retry


endpoint = 'https://acervos.ims.com.br/CIP/metadata/search/portals-general-access/situatedviews'
def query_portals(endpoint):
    print('start')    
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    API_STEPS = ["0", "55000"]
    a=0

    for i in API_STEPS:
        a = a + 1
        print(a)
        payload = {
            "table": "AssetRecords",
            "quicksearchstring": "jpg",
            "maxreturned": "55000",
            "startindex": i,
        }

        params = urllib.parse.urlencode(payload, quote_via=urllib.parse.quote)
        response = http.post(endpoint, params=params)
        data = response.json()
        results = pd.json_normalize(data["items"])           

        print(type(results))

query_portals(endpoint)

