import os
import pandas as pd
import urllib

from dotenv import load_dotenv

from helpers import session, update_metadata

load_dotenv(override=True)

MAX_RETURNED = 55000


def get_total_count():
    """
    Get total amount of published items
    """

    payload = {
        "table": "AssetRecords",
        "quicksearchstring": "jpg",
        "maxreturned": 1,
        "startindex": 0,
    }

    params = urllib.parse.urlencode(payload, quote_via=urllib.parse.quote)
    response = session.post(os.environ["PORTALS_API"], params=params)
    data = response.json()
    return data["totalcount"]


def query_portals(start_index):
    """
    Returns dataframe with MAX_RETURNED items beggining at start_index.
    """

    payload = {
        "table": "AssetRecords",
        "quicksearchstring": "jpg",
        "maxreturned": MAX_RETURNED,
        "startindex": start_index,
    }

    params = urllib.parse.urlencode(payload, quote_via=urllib.parse.quote)
    # try:
    response = session.post(os.environ["PORTALS_API"], params=params)
    data = response.json()
    portals = pd.json_normalize(data["items"])[["id", "RecordName"]]
    portals.rename(
        columns={"id": "Document URL", "RecordName": "Document ID"}, inplace=True
    )
    portals["Document ID"] = portals["Document ID"].str.split(".", n=1, expand=True)[0]
    portals["Document URL"] = os.environ["PORTALS_PREFIX"] + portals[
        "Document URL"
    ].astype(str)

    return portals


def main():
    start_index = 0
    total_count = get_total_count()
    portals_df = pd.DataFrame()

    while start_index < total_count:
        print("Querying portals from index {}".format(start_index))
        portals_df = pd.concat(
            [portals_df, query_portals(start_index)], ignore_index=True
        )
        start_index += MAX_RETURNED

    portals_df.drop_duplicates(subset="Document ID", inplace=True)
    # portals_df.to_csv(os.environ["PORTALS"])
    portals_df.set_index("Document ID", inplace=True)
    update_metadata(portals_df)


if __name__ == "__main__":
    main()
