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


#OMEKA   
@dg.solid
def query_omeka(context):
    endpoint = context.solid_config['url']  

    try:  
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

        return results

    except Exception as e:
        context.log.info(e)
        return None    
          

@dg.solid(output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="api_omeka")])
def omeka_dataframe(context,results):
    if results == None :        
        context.log.info("Couldn't update")
        return None

    else:
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
    
    query = None
    if query == None:
        query = """SELECT DISTINCT (?inventoryNumber as ?id) (?item as ?wikidata_id) (?imsid as ?wikidata_ims_id) (?image as ?wikidata_image) ?depict ?depictLabel
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

    except Exception as e:
        context.log.info(e)
        return None    
   

@dg.solid(output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="api_wikidata")])
def wikidata_dataframe(context, results):
    if results == None :        
        context.log.info("Couldn't update")
        return None

    else:
        ls = []
        for result in results["results"]["bindings"]:
            dic = {}
            for key in result:
                dic[f"{key}"] = result[f"{key}"]["value"]
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
        context.log.info(e)
        return None 

   
@dg.solid(output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="api_portals")])
def portals_dataframe(context,results):      
    if isinstance(results, pd.DataFrame):        
        dataframe = pd.DataFrame()
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
                "portals_id",
                "portals_url",
            ]
        ]

        dataframe = dataframe.drop_duplicates(subset="id")        

        return dataframe  

    else:
        context.log.info("Couldn't update")
        return None
        

        

    

   



