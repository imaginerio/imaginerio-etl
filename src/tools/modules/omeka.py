import pandas as pd
import os, re, requests
from time import sleep

# from requests.adapters import HTTPAdapter
# from requests.packages.urllib3.util.retry import Retry


def load(endpoint):
    results = {}
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    response = http.get(endpoint, verify=False)
    headers = response.headers
    # print(response.headers["Link"])
    last_page = re.findall(r'\d+(?=>; rel="last)', headers["Link"])[0]
    last_page = int(last_page)
    # last_page = int(headers["Link"].split("&page")[-1][:3].strip("=>"))
    # print(last_page)

    for page in range(1, last_page + 1):
        l1 = []
        l2 = []
        response = http.get(endpoint, verify=False, params={"page": page}).json()
        for item in response:
            l1.append(item["dcterms:identifier"][0]["@value"])
            l2.append(item["@id"])
        sleep(1)

    results.update({"id": l1, "omeka_url": l2})

    omeka_df = pd.DataFrame(results)

    return omeka_df


if __name__ == "__main__":
    omeka_df = load(os.environ["OMEKA_API_URL"])
    print(omeka_df)
