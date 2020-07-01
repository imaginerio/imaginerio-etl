import os
import json
import time
import urllib

import pandas as pd
import requests


API_URL = "http://201.73.128.131:8080/CIP/metadata/search/portals-general-access/situatedviews"

PREFIX = "http://acervos.ims.com.br/portals/#/detailpage/"


def load(PATH):

    try:

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

            response = requests.post(API_URL, params=params)

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

        dataframe["portals_url"] = PREFIX + dataframe["portals_id"].astype(str)

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

        return dataframe

    except Exception as e:

        print("ERRO", str(e))

        dataframe = pd.read_csv(PATH)

        print("Portals loaded from .csv \n")

        return dataframe


if __name__ == "__main__":
    load()
