import json
import os
import time
import urllib

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


def load(PATH):

    try:

        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "OPTIONS"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)

        dataframe = pd.DataFrame()

        API_STEPS = ["0", "55000"]

        for i in API_STEPS:

            payload = {
                "table": "AssetRecords",
                "quicksearchstring": "jpg",
                "maxreturned": "55000",
                "startindex": i,
            }

            params = urllib.parse.urlencode(payload, quote_via=urllib.parse.quote)

            response = http.post(os.environ["PORTALS_API"], params=params)

            data = response.json()

            result = pd.json_normalize(data["items"])

            dataframe = dataframe.append(result, ignore_index=True)

        dataframe = dataframe.rename(
            columns={
                "id": "portals_id",
                "RecordName": "id",
                "Author.displaystring": "creator",
                "Title": "title",
                "Date": "date",
            }
        )

        dataframe["id"] = dataframe["id"].str.split(".", n=1, expand=True)

        dataframe["portals_id"] = dataframe["portals_id"].astype(str)

        dataframe["portals_url"] = (
            os.environ["PORTALS_PREFIX"] + dataframe["portals_id"]
        )

        dataframe = dataframe[
            [
                "id",
                # "title", "creator", "date",
                "portals_id",
                "portals_url",
            ]
        ]

        dataframe = dataframe.drop_duplicates(subset="id")

        dataframe.to_csv(PATH, index=False)

        return dataframe.astype(str)

    except Exception as e:

        print("ERRO", str(e))

        dataframe = pd.read_csv(PATH)

        print("Portals loaded from .csv \n")

        return dataframe


if __name__ == "__main__":
    load(os.environ["PORTALS"])
