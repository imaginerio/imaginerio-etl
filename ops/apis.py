import os
import urllib
from time import sleep
from typing import Dict

from dagster import In, Out, op
import dagster_pandas as dp
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from tests.dataframe_types import *
from tests.objects_types import *
from tqdm import tqdm
from urllib3.util import Retry


# WIKIDATA


@op(config_schema=dg.StringSource)
def query_wikidata(context):
    """
    Query Wikidata's SPARQL endpoint for entities
    having Instituto Moreira Salles as collection
    """
    endpoint = context.solid_config

    query = None
    if query == None:
        query = """SELECT DISTINCT (?inventoryNumber as ?id) (?item as ?wikidata_id_url) (?imsid as ?wikidata_ims_id) (?image as ?wikidata_image) ?depict ?depictLabel
    WHERE {
    BIND(wdt:P31 AS ?instanceOf) .
    ?item wdt:P195* wd:Q71989864 .
    ?item wdt:P217 ?inventoryNumber .
        
    OPTIONAL { ?item wdt:P18 ?image . }
    OPTIONAL { ?item wdt:P180 ?depict . }
    OPTIONAL { ?item wdt:P7835 ?imsid . }

    SERVICE wikibase:label { bd:serviceParam wikibase:language "pt-br", "en" . }

    }"""
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

        response = http.get(endpoint, params={"format": "json", "query": query})
        data = response.json()
        return data

    except Exception:
        context.log.info("Couldn't update")
        return None


@op(out={"api_wikidata": Out(io_manager_key="pandas_csv")})
def wikidata_dataframe(context, results):
    """
    Create dataframe from Wikidata's response
    """
    if results == None:
        context.log.info("Couldn't update")
        return None

    else:
        ls = []
        for result in tqdm(results["results"]["bindings"], desc="Results"):
            dic = {}
            for key in result:
                dic[f"{key}"] = result[f"{key}"]["value"]
            ls.append(dic)

        wikidata_df = pd.DataFrame(ls)

        wikidata_df["Depicts"] = (
            wikidata_df["depict"] + " " + wikidata_df["depictLabel"]
        )

        wikidata_df.drop(columns=["depict", "depictLabel"], inplace=True)

        wikidata_df = wikidata_df.groupby("id", as_index=False).agg(lambda x: set(x))

        def concat(a_set):
            list_of_strings = [str(s) for s in a_set]
            joined_string = "||".join(list_of_strings)
            return joined_string

        wikidata_df["Depicts"] = wikidata_df["Depicts"].apply(concat)

        wikidata_df = wikidata_df.applymap(lambda x: str(x).strip("{'}"))

        wikidata_df = wikidata_df.applymap(lambda x: x.replace("nan", ""))

        wikidata_df = wikidata_df.drop_duplicates(subset="id")

        wikidata_df.name = "api_wikidata"

        wikidata_df["Wikidata ID"] = (
            wikidata_df["wikidata_id_url"].str.split("/").str[-1]
        )

        wikidata_df = wikidata_df.rename(columns={"id": "Source ID"})

        return wikidata_df.set_index("Source ID")


# PORTALS
@op(
    config_schema=dg.StringSource,
    out=Out(dagster_type=dp.DataFrame),
)
def query_portals(context):
    """
    Query Cumulus Portals for all published items
    """
    endpoint = context.solid_config

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

    dataframe = pd.DataFrame()

    for i in tqdm(API_STEPS, desc="Steps"):

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
        dataframe = dataframe.append(results, ignore_index=True)

    return dataframe


@op(
    config_schema=dg.StringSource,
    out={"api_portals": Out(io_manager_key="pandas_csv")},
)
def portals_dataframe(context, results: dp.DataFrame):
    """
    Create dataframe from Cumulus Portals response
    """
    if isinstance(results, pd.DataFrame):
        prefix = context.solid_config
        dataframe = pd.DataFrame()
        dataframe = dataframe.append(results, ignore_index=True)
        dataframe = dataframe.rename(
            columns={
                "id": "portals_id",
                "RecordName": "Source ID",
                "Author.displaystring": "Creator",
                "Title": "Title",
                "Date": "Date",
            }
        )

        dataframe["Source ID"] = dataframe["Source ID"].str.split(
            ".", n=1, expand=True
        )[0]

        dataframe["portals_id"] = dataframe["portals_id"].astype(str)

        dataframe["Source URL"] = prefix + dataframe["portals_id"]

        portals_df = dataframe[
            [
                "Source ID",
                "portals_id",
                "Source URL",
            ]
        ]

        portals_df = portals_df.drop_duplicates(subset="Source ID")

        return portals_df.set_index("Source ID")

    else:
        context.log.info("Couldn't update")
        return None
