import json
import os
import re
import urllib
from time import sleep

import dagster as dg
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Response
from tqdm import tqdm
from urllib3.util import Retry


#OMEKA   
@dg.solid
def query_omeka(context):  
     # start session
    endpoint = context.solid_config['url']
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

    return results

@dg.solid(output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="api_omeka")])
def omeka_dataframe(context,results):
    # create dataframes
    omeka_df = pd.DataFrame(results)
    omeka_duplicated = omeka_df[omeka_df.duplicated(subset="id")]
    if len(omeka_duplicated) > 0:
        omeka_duplicated.to_csv("data-out/duplicated-omeka.csv")
    omeka_df.drop_duplicates(subset="id", inplace=True)
    #omeka_df.to_csv(os.environ["OMEKA"])

    return omeka_df


#WIKIDATA
@dg.solid
def query_wikidata(context):    
    endpoint = context.solid_config['url']
    path = context.solid_config['path']

    query = None
    try:
        response = requests.get(endpoint, params={"format": "json", "query": query})
        data = response.json()

    except Exception as e:
        path = context.solid_config #????
        if os.path.isfile(path):
            print(e)
            print("Couldn't update. Returning existing file")
            return pd.read_csv(path)
        print(str(e))

    return data

@dg.solid(output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="api_wikidata")])
def wikidata_dataframe(context, data):    
    ls = []
    for results in data["result"]["bindings"]:
        dic = {}
        for key in results:
            dic[f"{key}"] = results[f"{key}"]["value"]
        ls.append(dic)

    wikidata_df = pd.DataFrame(ls)

    wikidata_df["wikidata_depict"] = (
        wikidata_df["depict"] + " " + wikidata_df["depictLabel"]
    )

    wikidata_df.drop(columns=["depict", "depictLabel"], inplace=True)

    wikidata_df = wikidata_df.groupby("id", as_index=False).agg(lambda x: set(x))

    def concat(a_set):
        list_of_strings = [str(s) for s in a_set]
        joined_string = "||".join(list_of_strings)
        return joined_string

    wikidata_df["wikidata_depict"] = wikidata_df["wikidata_depict"].apply(concat)

    wikidata_df = wikidata_df.applymap(lambda x: str(x).strip("{'}"))

    wikidata_df = wikidata_df.applymap(lambda x: x.replace("nan", ""))

    wikidata_df = wikidata_df.drop_duplicates(subset="id")

    return wikidata_df

#PORTALS
@dg.solid
def query_portals(context):
    endpoint = context.solid_config['url']
    path = context.solid_config['path']
    
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

        API_STEPS = ["0", "55000"]

        for i in API_STEPS:

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

            return results

    except Exception as e:

        print("ERRO", str(e))
        results = pd.read_csv(path)
        print("Portals loaded from .csv \n")
        return results
    
@dg.solid(output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="api_portals")])
def portals_dataframe(contex, results):
    dataframe = pd.DataFrame()
    if isinstance(results, pd.DataFrame):
        return results 

    else: 

        dataframe = dataframe.append(results, ignore_index=True)
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

        dataframe["portals_url"] = (os.environ["PORTALS_PREFIX"] + dataframe["portals_id"])

        dataframe = dataframe[
            [
                "id",
                # "title", "creator", "date",
                "portals_id",
                "portals_url",
            ]
        ]

        dataframe = dataframe.drop_duplicates(subset="id")        

        return dataframe

        

    

   



