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

    response = http.get(endpoint)
    headers = response.headers

    # find out number of pages
    last_page = re.findall(r'\d+(?=>; rel="last)', headers["Link"])[0]
    last_page = int(last_page)

    # loop over pages
    results = {}
    l1 = []
    l2 = []
    for page in tqdm(range(1, last_page + 1)):
        response = http.get(endpoint, params={"page": page}).json()
        for item in response:
            l1.append(item["dcterms:identifier"][0]["@value"])
            l2.append(item["@id"])
        sleep(0.5)

    results.update({"id": l1, "omeka_url": l2})

    # create dataframes
    omeka_df = pd.DataFrame(results)
    omeka_duplicated = omeka_df[omeka_df.duplicated(subset="id")]
    if len(omeka_duplicated) > 0:
        omeka_duplicated.to_csv("metadata/omeka/omeka_duplicated.csv")
    omeka_df.drop_duplicates(subset="id", inplace=True)
    return omeka_df


if __name__ == "__main__":
    omeka_df = load(os.environ["OMEKA_API_URL"])
    print(omeka_df)
