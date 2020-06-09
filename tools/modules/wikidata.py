import os, requests, json
import pandas as pd


# Search for anything related to IMS Collection (Q71989864)
# Search for ids on property inventory number (P217)
# Search for images on property image (P18)
# Add results to metadata.csv


def load(path, query=None):

    url = "https://query.wikidata.org/sparql"
    if query == None:
        query = """SELECT DISTINCT
    (?inventoryNumber as ?id)
    (?item as ?wikidata_id)
    (?imsid as ?wikidata_ims_id)
    (?image as ?wikidata_image)
    ( GROUP_CONCAT ( DISTINCT ?depict; separator="; " ) AS ?wikidata_depicts )

    WHERE {

    ?item wdt:P195* wd:Q71989864 .
    ?item wdt:P217 ?inventoryNumber.
        
    OPTIONAL { ?item wdt:P18 ?image . }
    OPTIONAL { ?item wdt:P180 ?depict . }
    OPTIONAL { ?item wdt:P7835 ?imsid . }

    SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE]". }

    }

    GROUP BY ?item ?inventoryNumber ?imsid ?image 
    """

    try:
        r = requests.get(url, params={"format": "json", "query": query})
        data = r.json()

    except Exception as e:
        if os.path.isfile(path):
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

    wikidata_df = wikidata_df.sort_values(by=["id"])

    wikidata_df = wikidata_df.drop_duplicates(subset="id")

    wikidata_df.to_csv(path, index=False)

    print("Wikidata Done")

    return wikidata_df
