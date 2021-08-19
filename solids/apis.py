import os
import urllib
from time import sleep
from typing import Dict

import dagster as dg
import dagster_pandas as dp
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from tests.dataframe_types import *
from tests.objects_types import *
from tqdm import tqdm
from urllib3.util import Retry


# OMEKA
@dg.solid(config_schema=dg.StringSource, output_defs=[dg.OutputDefinition(dagster_type=dict)])
def query_omeka(context):
    endpoint = context.solid_config

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
    l1,l2 = [],[]
    page = 1
    while response != []:
        response = http.get(
            endpoint, params={"page": page, "per_page": 250}).json()

        for item in response:
            try:
                l1.append(item["dcterms:identifier"][0]["@value"])
                l2.append(item["@id"])
            except:
                pass
        page += 1
        sleep(0.5)

    results.update({"Source ID": l1, "omeka_url": l2})

    return results


@dg.solid(config_schema=dg.StringSource, output_defs=[dg.OutputDefinition(
    io_manager_key="pandas_csv", name="api_omeka")])
def omeka_dataframe(context, results: dict):
    print(type(results))
    path_output = context.solid_config
    path = os.path.join(path_output, "duplicated-omeka.csv")
    if results == None:
        context.log.info("Couldn't update")
        return None

    else:
        # create dataframes
        omeka_df = pd.DataFrame(results)
        omeka_duplicated = omeka_df[omeka_df.duplicated(subset="Source ID")]
        if len(omeka_duplicated) > 0:
            omeka_duplicated.to_csv(path)
        omeka_df.drop_duplicates(subset="Source ID", inplace=True)

        omeka_df.name = "api_omeka"

        return omeka_df.set_index("Source ID")


# @dg.solid(output_defs=[dg.OutputDefinition(
#     io_manager_key="pandas_csv", name="api_omeka")])
# def validate_omeka(context, df: api_omeka_dataframe_types):
#     name = context.name
#     print(name.upper(), " valid")
#     return df.set_index("Source ID")

# WIKIDATA


@dg.solid(config_schema=dg.StringSource)
def query_wikidata(context):
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

        response = http.get(
            endpoint, params={"format": "json", "query": query})
        data = response.json()
        return data

    except Exception:
        context.log.info("Couldn't update")
        return None


@dg.solid(output_defs=[dg.OutputDefinition(
    io_manager_key="pandas_csv", name="api_wikidata")])
def wikidata_dataframe(context, results):
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

        wikidata_df = wikidata_df.groupby(
            "id", as_index=False).agg(lambda x: set(x))

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

        return wikidata_df


# PORTALS
@dg.solid(config_schema=dg.StringSource, output_defs=[dg.OutputDefinition(dagster_type=dp.DataFrame)])
def query_portals(context):
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


@dg.solid(
    config_schema=dg.StringSource, output_defs=[dg.OutputDefinition(
        io_manager_key="pandas_csv", name="api_portals")]
)
def portals_dataframe(context, results: dp.DataFrame):
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
            ".", n=1, expand=True)[0]

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
