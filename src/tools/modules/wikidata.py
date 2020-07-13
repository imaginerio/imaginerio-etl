import os, requests, json
import pandas as pd
import numpy as np


# Search for anything related to IMS Collection (Q71989864)
# Search for ids on property inventory number (P217)
# Search for images on property image (P18)
# Add results to metadata.csv


def load(path, query=None):
    
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
        r = requests.get(os.environ['WIKIDATA_API_URL'], params={"format": "json", "query": query})
        data = r.json()
        
    except Exception as e:
        if os.path.isfile(path):
            print(e)
            print("Couldn't update. Returning existing file")
            return pd.read_csv(path)
        print(str(e))


    ls = []
    for result in data["results"]["bindings"]:
        dic = {}
        for key in result:
            dic[f"{key}"] = result[f"{key}"]["value"]
        ls.append(dic)

    wikidata_df = pd.DataFrame(ls)

    wikidata_df['wikidata_depict'] = wikidata_df['depict'] + " " + wikidata_df['depictLabel']

    wikidata_df.drop(columns=['depict', 'depictLabel'], inplace=True)

    wikidata_df = wikidata_df.groupby("id", as_index=False).agg(lambda x: set(x))
    
    def concat(a_set):
        list_of_strings = [str(s) for s in a_set]
        joined_string = "||".join(list_of_strings)   
        return joined_string

    wikidata_df['wikidata_depict'] = wikidata_df['wikidata_depict'].apply(concat)

    wikidata_df = wikidata_df.applymap(lambda x: str(x).strip("{''}"))
    
    wikidata_df = wikidata_df.drop_duplicates(subset="id")  

    wikidata_df.to_csv(path, index=False)

    return wikidata_df
