import os
import re
import sys
from datetime import datetime
from json import JSONDecodeError

import boto3
import pandas as pd
import requests
from iiif_prezi3 import Collection
from pyproj import Proj
from requests.adapters import HTTPAdapter
from shapely.geometry import Point
from SPARQLWrapper import JSON, SPARQLWrapper
from urllib3.util import Retry

from ..config import (
    REPROCESS,
    CLOUDFRONT,
    BUCKET_NAME,
    DISTRIBUTION_ID,
    IIIFConfig as IC,
)
from .logger import CustomFormatter as cf
from .logger import logger

# from lxml import etree

s3_client = boto3.client("s3")

session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
session.mount("http://", HTTPAdapter(max_retries=retries))
session.mount("https://", HTTPAdapter(max_retries=retries))

float2str = lambda x: x.split(".")[0]


# def get_items(metadata, vocabulary):
#     return [Item(id, row, vocabulary) for id, row in metadata.fillna("").iterrows()]


def get_collections(metadata):  # , index
    collections = {}
    # list all collection names
    labels = metadata["Collection"].dropna().str.split("|").explode().unique()
    # create collection(s)
    for label in labels:
        # if index == "all":
        #     collection = create_collection(label)
        #     collections[label] = collection
        # else:
        try:
            response = requests.get(
                f"https://iiif.imaginerio.org/iiif/collection/{label.lower()}.json"
            )
            collection = Collection(**response.json())
        except JSONDecodeError:
            collection = create_collection(label)
        collections[label] = collection
    return collections


def get_metadata_changes(current_file, new_file):
    # Load current (filtered) file
    current_data = load_xls(current_file, "SSID")

    # Load downloaded file and filter data
    new_data = load_xls(new_file, "SSID")
    filtered_new_data = new_data.drop(columns=["Notes"]).loc[
        new_data["Status"] == "In imagineRio"
    ]

    # Compare files and get changed rows
    comparison = filtered_new_data.compare(current_data, keep_shape=True)
    changes = comparison.notna().any(axis=1)
    changed_data = filtered_new_data[changes]

    # Replace current with new filtered data if any
    if not changed_data.empty:
        new_file = filtered_new_data.to_excel(current_file, engine="openpyxl")

    if REPROCESS == "true": # github action input, not boolean
        changed_data = filtered_new_data

    return new_data, changed_data


def get_vocabulary(vocabulary_path):
    vocabulary = load_xls(vocabulary_path, "Label (en)")
    try:
        vocabulary = vocabulary.to_dict("index")
        return vocabulary
    except ValueError:
        logger.error(
            "Vocabulary labels must be unique. Duplicated labels: "
            f"{vocabulary[vocabulary.index.duplicated()].index.to_list()}"
        )
        sys.exit(1)


# def get_metadata(metadata_path, index):
#     logger.info("Loading metadata")
#     # open files and rename columns
#     #metadata = compare_data()

#     # filter rows
#     if index != "all":
#         metadata = pd.DataFrame(metadata.loc[index])
#         if len(index) == 1:
#             metadata = metadata.T
#     else:
#         metadata = metadata.loc[metadata["Status"] == "In imagineRio"]

#     return metadata


def summarize(viewcones_info, manifests_info):
    summary = ""
    if manifests_info:
        summary += (
            f"SUMMARY: Processing done. Parsed {cf.BLUE}{manifests_info['n_items']}{cf.RESET} "
            f"items and created/updated {cf.GREEN}{manifests_info['n_manifests']}{cf.RESET} IIIF manifests. "
        )

        if manifests_info.get("no_collection"):
            summary += (
                f"Items {cf.YELLOW}{manifests_info['no_collection']}{cf.RESET} aren't associated with any collections. "
                f"Please fill the Collection field in JSTOR so these images are displayed in imagineRio. "
            )

        if manifests_info.get("errors"):
            summary += (
                f"Items {cf.RED}{manifests_info['errors']}{cf.RESET} were skipped, likely due to issues with "
                f"the images or metadata. Inspect the log above (with CTRL+F) for more details."
            )

    if viewcones_info:
        summary += (
            f"Items {cf.YELLOW}{viewcones_info['not_in_arcgis']}{cf.RESET} are marked as ready in JSTOR but have no viewcone. "
            f"Items {cf.YELLOW}{viewcones_info['not_marked_as_ready']}{cf.RESET} have viewcones but aren't marked as ready."
        )

    return summary


def load_xls(xls, index):
    df = pd.read_excel(xls)
    df.rename(columns=lambda x: re.sub(r"\[[0-9]*\]", "", x), inplace=True)
    if "SSID" in df.columns:
        df["SSID"] = df["SSID"].astype(str)
    return df.set_index(index)


def create_collection(label):
    homepage = {
        "id": "https://imaginerio.org",
        "label": {"none": ["imagineRio"]},
        "type": "Text",
        "format": "text/html",
    }

    logo = {
        "id": IC.LOGO_URL,
        "type": "Image",
        "format": "image/png",
        "height": IC.LOGO_HEIGHT,
        "width": IC.LOGO_WIDTH,
    }

    thumb_info = {
        "Views": ("0071824cx001-01/full/295,221/0/default.jpg", 221, 295),
        "Plans": ("10639297/full/259,356/0/default.jpg", 356, 259),
        "Maps": ("10643717/full/512,259/0/default.jpg", 259, 512),
        "Aerials": ("24879867/full/394,260/0/default.jpg", 260, 394),
        "Mare": ("31770323/full/188,125/0/default.jpg", 125, 188),
    }

    thumb_data = thumb_info.get(label)

    if thumb_data:
        thumb_id, h, w = thumb_data
        thumbnail = {
            "id": f"{CLOUDFRONT}/iiif/{thumb_id}/full/{w},{h}/0/default.jpg",
            "type": "Image",
            "format": "image/jpeg",
            "height": h,
            "width": w,
        }
    else:
        thumbnail = None

    collection = Collection(
        id=f"{CLOUDFRONT}/collection/{label.lower()}.json",
        label=label,
        logo=logo,
        homepage=homepage,
        provider={
            "id": "https://imaginerio.org/",
            "type": "Agent",
            "label": {"none": ["imagineRio"]},
            "homepage": homepage,
            "logo": logo,
        },
        requiredStatement={
            "label": {"en": ["Attribution"], "pt-BR": ["Atribuição"]},
            "value": {
                "en": ["Hosted by imagineRio"],
                "pt-BR": ["Hospedado por imagineRio"],
            },
        },
        thumbnail=thumbnail,
    )

    return collection


def file_exists(identifier, type):

    if type == "info" or type == "manifest":
        key = "iiif/{0}/{1}.json".format(identifier, type)
    else:
        key = "iiif/{0}/full/max/0/default.jpg".format(identifier)

    s3 = boto3.resource("s3")
    bucket = s3.Bucket(BUCKET_NAME)
    objs = list(bucket.objects.filter(Prefix=key))
    if any([obj.key == key for obj in objs]):
        return True
    else:
        return False


def invalidate_cache(path):
    cloudfront = boto3.client("cloudfront")
    get_timestamp = lambda x: datetime.timestamp(x)
    cloudfront.create_invalidation(
        DistributionId=DISTRIBUTION_ID,
        InvalidationBatch={
            "Paths": {"Quantity": 1, "Items": ["/" + path]},
            "CallerReference": str(get_timestamp(datetime.now())),
        },
    )


def upload_folder_to_s3(source):
    logger.info(f"{cf.BLUE}Uploading {source} to S3...")
    for root, _, files in os.walk(source):
        for file in files:
            try:
                path = os.path.join(root, file)
                # logger.debug(path)
                s3_client.upload_file(
                    path,
                    "imaginerio-images",
                    path,
                    ExtraArgs={
                        "ContentType": (
                            "image/jpeg"
                            if file.endswith(".jpg")
                            else "application/json"
                        )
                    },
                )
            except:
                logger.error(f"{cf.RED}Failed to upload {path}")
        # invalidate_cache(path)

    # if mode == "test":
    #     return False
    # else:
    #     shutil.rmtree(source)
    #     return True


def upload_object_to_s3(obj, name, key):
    # logger.debug(f"{obj.id} -> {target}")
    try:
        s3_client.put_object(
            Body=obj.json(indent=4),
            Bucket=BUCKET_NAME,
            Key=key,
            ContentType="application/json",
        )
        logger.info(f"{cf.GREEN}Object {name} uploaded successfully")
    except Exception as e:
        logger.error(f"{cf.RED}Failed to upload {name} to {key}: {e}")


def query_wikidata(Q):
    """
    Query Wikidata's SPARQL endpoint for entities' coordinates
    """
    endpoint_url = "https://query.wikidata.org/sparql"

    query = """SELECT ?coordinate
        WHERE
        {
        wd:%s wdt:P625 ?coordinate .
        }""" % (
        Q
    )

    def get_results(endpoint_url, query):
        user_agent = "WDQS-example Python/%s.%s" % (
            sys.version_info[0],
            sys.version_info[1],
        )
        # TODO adjust user agent; see https://w.wiki/CX6
        sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        return sparql.query().convert()

    results = get_results(endpoint_url, query)
    result_list = []
    for result in results["results"]["bindings"]:
        if result:
            result_list.append(result["coordinate"]["value"])
    return result_list


def geo_to_world_coors(coors, inverse=False):
    """
    Transform Rio's geographic to world
    coordinates or vice-versa with inverse=True
    """
    rj = Proj("EPSG:32722")
    origin = Point(coors)
    origin_proj = rj(origin.x, origin.y, inverse=inverse)

    return Point(origin_proj)


def update_metadata(df):
    metadata = pd.read_csv(
        "data/output/faltantes.csv",
        index_col="Document ID",
        converters={
            "First Year": float2str,
            "Last Year": float2str,
            "Width": float2str,
            "Height": float2str,
        },
    )
    print(metadata["Document URL"].dtype)
    print("metadata", len(metadata))
    metadata.update(df)
    print("metadata after update", len(metadata))
    metadata.to_csv("data/output/faltantes.csv")
    # return metadata


def ims2jstor():
    jstor = pd.read_excel("data/input/jstor.xls")
    jstor.set_index("Document ID[19474]", inplace=True)
    ims = pd.read_csv(
        "data/output/faltantes.csv",
        index_col="Document ID",
        converters={
            "First Year": float2str,
            "Last Year": float2str,
            "Width": float2str,
            "Height": float2str,
        },
    )
    print("ims", len(ims))
    # digitized = ims["Media URL"].notna()
    published = ims["Document URL"].notna()
    not_in_jstor = ~(ims.index.isin(jstor.index))
    has_dates = ims["First Year"].notna() & ims["Last Year"].notna()
    ims2jstor = ims.loc[has_dates & published & not_in_jstor].copy()
    print("ims2jstor", len(ims2jstor))
    ims2jstor.loc[ims2jstor["Creator"] == "Autoria não identificada", "Creator"] = (
        "Unknown Authorship"
    )
    ims2jstor["Document URL"] = ims2jstor["Document URL"].astype(str)
    ims2jstor.rename(
        columns={
            "Title": "Title[19462]",
            "Date": "Date[19486]",
            "First Year": "First Year[19466]",
            "Last Year": "Last Year[19467]",
            "Creator": "Creator[1603501]",
            "Description (Portuguese)": "Description (Portuguese)[1612567]",
            "Type": "Type[1604106]",
            "Collection": "Collection[1711006]",
            "Provider": "Provider[1731287]",
            "Material": "Material[1612569]",
            "Fabrication Method": "Fabrication Method[1612568]",
            "Rights": "Rights[1861241]",
            "Required Statement": "Required Statement[19484]",
            "Width": "Width[1604102]",
            "Height": "Height[1604103]",
            "Document URL": "Document URL[796463]",
        },
        inplace=True,
    )
    ims2jstor[
        [column for column in jstor.columns if column not in ims2jstor.columns]
    ] = ""
    ims2jstor["SSID"] = "NEW"
    ims2jstor.index.rename("Document ID[19474]", inplace=True)
    ims2jstor.to_excel("data/output/ims2jstor.xls", engine="openpyxl")
