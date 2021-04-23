import json
import os
import re
from html import unescape

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

load_dotenv(override=True)


OMEKA_ALL = os.environ["OMEKA_ALL"]
IMPORT_VIKUS = os.environ["IMPORT_VIKUS"]


def json_extract(url, key):
    """Recursively fetch values from nested JSON."""
    obj = requests.get(url, timeout = None).json()
    obj = obj["items"]
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    return values


def find_new_ids(list_of_manifests, df):
    list_of_ids = [url.split("/")[-2] for url in list_of_manifests]
    new_ids = [id for id in list_of_ids if id not in list(df["id"])]
    return new_ids


def manifests_to_df(list_of_ids):
    
    all_items = []
    for i, id in enumerate(list_of_ids):

        retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)          
      
		
        url = "https://images.imaginerio.org/iiif/3/" + id + "/manifest"
        
        try:
            item_json = http.get(url).json()
        except:
            print(f"Couldn't acess manifest {id}")
			
        labels = []
        values = []

        for field in item_json["metadata"]:
            try:
                label = field["label"]["none"][0]
                value = field["value"]["none"][0]
                labels.append(label)
                values.append(value)
            except KeyError:
                label = field["label"]["pt-br"][0]
                value = field["value"]["pt-br"][0]
                labels.append(label)
                values.append(value)
        item_dict = dict(zip(labels, values))
        for field in item_json["seeAlso"]:
            if "imaginerio" in field["id"]:
                omeka_api = requests.get(field["id"],timeout = None).json()
                mapping = requests.get(
                    omeka_api["o-module-mapping:marker"][0]["@id"],
                timeout = None).json()
                lat = mapping["o-module-mapping:lat"]
                lng = mapping["o-module-mapping:lng"]
                item_dict["_Lat"] = lat
                item_dict["_Lon"] = lng
            else:
                continue
        print(f"{item_dict['Identifier']} - {i+1} of {len(list_of_ids)} processed")
        all_items.append(item_dict)

    df = pd.DataFrame(all_items)
    return df




def vikus_format(df):
    # format data
    df["year"] = 0
    try:
        df[["circa-min", "circa-max"]] = (
            df["Date (Circa)"].str.split("–", expand=True).fillna(0).astype(int)
        )
        df["year"] = ((df["circa-min"] + df["circa-max"]) / 2).astype(int)
    except:
        print("No circa dates")
    year_zero = df["year"] == 0
    df.loc[year_zero, "year"] = df["Date Created"]
    full_date = df["year"].str.contains(r"[a-z]", na=False,)
    df.loc[full_date, "year"] = df["year"].str.extract(r"(\d{4})", expand=False)
    # no_year = df["year"].isna()
    # df.loc[no_year, "year"] = 0

    try:
        has_width = df["Width"].notna()
        has_height = df["Height"].notna()
        df.loc[has_width, "Width"] = df["Width"] + " cm"
        df.loc[has_height, "Height"] = df["Height"] + " cm"
    except:
        print("No dimensions")

    try:
        df["Title"] = df["Title"].astype(str).apply(html_decode)
    except:
        print("No title")

    try:
        df["Description"] = df["Description"].astype(str).apply(html_decode)
    except:
        print("No description")
    # no_creator = df["Creator"].isna()
    # df.loc[no_creator, "Creator"] = "Not available"
    df.fillna(value="N.A.", inplace=True)

    df.rename(
        columns={
            "Identifier": "id",
            "Title": "_title",
            "Description": "_description",
            "Creator": "keywords",
            "Date (Circa)": "_circa",
            "Date Created": "_date created",
            "Type": "_material",
            "Width": "_width",
            "Height": "_height",
            "Depicts": "_depicts",
        },
        inplace=True,
    )
    if "circa-min" and "circa-max" in df:
        df.drop(columns=["circa-min", "circa-max"], inplace=True)

    def organize_keywords(cell):
        if '&' in cell:
            l = cell.split(' &')
            name = [] 
            for i in range(len(l)):
                if ',' in l[i]:        
                    n = l[i].split(',')[1] +' '+ l[i].split(',')[0]
                    name.append(n)
                else:
                    n  = l[i]
                    name.append(n)
            try: 
                return name[0] + ',' +name[1]
            except:
                return name[0]

        elif ',' in cell:
            name = cell.split(', ')    
            return name[1] + ' ' + name[0]
            
        else:
            return cell

#Oganize the keywords
    df['_creator'] = df['keywords']
    filt_brackets= df['keywords'].str.contains('\(') #Filter of bracktes
    filt_coowork = (df['keywords'].str.contains(';'))

    df.loc[filt_brackets,'keywords'] = df['keywords'].str.replace('\([^)]*\)', '')
    df['keywords'] = df['keywords'].apply(organize_keywords)

#Fix especific names
    df['keywords'] = df['keywords'].str.replace('Augusto Cesar de  Malta Campos','Augusto Malta')
    df['keywords'] = df['keywords'].str.replace('Marc  Ferrez','Marc Ferrez')

    return df



def html_decode(s):
    """
    Returns the ASCII decoded version of the given HTML string. This does
    NOT remove normal HTML tags like <p>.
    """
    htmlCodes = (
        ("'", "&#39;"),
        ("'", "&#039,"),
        ("'","&#039;"),
        ('"', "&quot;"),
        (">", "&gt;"),
        ("<", "&lt;"),
        ("&", "&amp;"),
        ("&", "&amp,"),
        ("&", ",amp;"),
        ("&", "d&#039;"),
        (" ", "<br />\n"),
    )
    for code in htmlCodes:
        s = s.replace(code[1], code[0])
    return s


def download_images(series, destination):
    to_download = [
        item
        for item in list(series)
        if not os.path.exists(os.path.join(destination, f"{item}.jpg"))
    ]
    if to_download:
        for i, id in enumerate(to_download):
            name = os.path.join(destination, f"{id}.jpg")
            print(f"Downloading image {i+1} of {len(to_download)}...")
            try:
                with open(name, "wb") as img:
                    img.write(
                        requests.get(
                            "https://images.imaginerio.org/iiif-img/"
                            + id
                            + "/full/%5E!4000,4000/0/default.jpg",
                        timeout = None).content
                    )
                    img.close()
            except:
                print(f"Could not download image {id}")
    else:
        print("Nothing to download!")


# extract object manifests from collection manifest
print("Extracting manifests from collection...")
all_links = json_extract(OMEKA_ALL, "id")
all_manifests = all_links[::2]

# look for new items
try:
    df_old = pd.read_csv("src/" + IMPORT_VIKUS)
except FileNotFoundError:
    df_old = pd.DataFrame(
        columns=[
            "id",
            "_title",
            "_description",
            "keywords",
            "_circa",
            "_date created",
            "_material",
            "_width",
            "_height",
            "_depicts",
        ]
    )

new_manifests = find_new_ids(all_manifests, df_old)


# create and format dataframe
if new_manifests:
    df_new = manifests_to_df(new_manifests)
    df_new = vikus_format(df_new)
    df = df_old.append(df_new)
    

    # download images
    download_images(df["id"], "D:/dev/vikus-viewer/data/images/4000")

    # save file
    df.to_csv("src/" + IMPORT_VIKUS, index=False)

else:
    print("Nothing to add!")
    df = pd.read_csv("src/" + IMPORT_VIKUS, index=False)
    download_images(df["id"], "D:/dev/vikus-viewer/data/images/4000")
