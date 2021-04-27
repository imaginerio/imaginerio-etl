import os
import re
from time import sleep

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util import Retry


def load(endpoint):
    # start session
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    response = http.get(endpoint, params={"per_page": 1})

    # loop over pages until response is blank
    results = {}
    l1 = []
    l2 = []
    page = 1
    while response != []:
        response = http.get(endpoint, params={"page": page, "per_page": 250}).json()
        for item in response:
            l1.append(item["dcterms:identifier"][0]["@value"])
            l2.append(item["@id"])
        page += 1
        sleep(0.5)

    results.update({"id": l1, "omeka_url": l2})

    # create dataframes
    omeka_df = pd.DataFrame(results)
    omeka_duplicated = omeka_df[omeka_df.duplicated(subset="id")]
    if len(omeka_duplicated) > 0:
        omeka_duplicated.to_csv("data-out/duplicated-omeka.csv")
    omeka_df.drop_duplicates(subset="id", inplace=True)
    omeka_df.to_csv(os.environ["OMEKA"])
    return omeka_df


if __name__ == "__main__":
    omeka_df = load(os.environ["OMEKA_API"])
    print(omeka_df)
