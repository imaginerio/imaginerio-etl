import logging
import os
import re
import shutil
import sys
from datetime import datetime
from logging import config

import boto3
import boto3.s3.transfer as s3transfer
import botocore
import geojson
import mercantile
import pandas as pd
import requests
from IIIFpres import iiifpapi3
from IIIFpres.utilities import *
from lxml import etree
from pyproj import Proj
from requests.adapters import HTTPAdapter
from shapely.geometry import Point
from SPARQLWrapper import JSON, SPARQLWrapper
from tqdm import tqdm
from turfpy.misc import sector
from urllib3.util import Retry

logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": True,
    }
)
logging.basicConfig(filename="data/output/debug.log", level=logging.DEBUG)
logger = logging.getLogger(__name__)

session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
session.mount("http://", HTTPAdapter(max_retries=retries))
session.mount("https://", HTTPAdapter(max_retries=retries))

float2str = lambda x:x.split(".")[0]

def load_xls(xls, index):
    df = pd.read_excel(xls)
    df.rename(columns=lambda x: re.sub(r"\[[0-9]*\]", "", x), inplace=True)
    if 'SSID' in df.columns:
        df["SSID"] = df["SSID"].astype(str)
    df.set_index(index, inplace=True)
    return df


def create_collection(name, manifest):

    collection_path = "iiif/collection/{0}.json".format(name.lower())

    # Logo
    logo = iiifpapi3.logo()
    logo.set_id(
        "https://aws1.discourse-cdn.com/free1/uploads/imaginerio/original/1X/8c4f71106b4c8191ffdcafb4edeedb6f6f58b482.png"
    )
    logo.set_format("image/png")
    logo.set_hightwidth(164, 708)

    # Homepage
    collection_homepage = iiifpapi3.homepage()
    collection_homepage.set_id("https://imaginerio.org")
    collection_homepage.set_type("Text")
    collection_homepage.add_label("none", "imagineRio")
    collection_homepage.set_format("text/html")

    # Provider
    collection_provider = iiifpapi3.provider()
    collection_provider.set_id("https://imaginerio.org")
    collection_provider.add_label("en", "imagineRio")
    collection_provider.add_label("pt-BR", "imagineRio")
    collection_provider.add_logo(logo)
    collection_provider.add_homepage(collection_homepage)

    # Collection manifest
    collection = iiifpapi3.Collection()
    collection.set_id(extendbase_url="collection/{0}.json".format(name))
    collection.add_label("en", name)
    collection.add_label("pt-BR", name)
    collection.add_requiredStatement(
        label="Attribution",
        value="Hosted by imagineRio",
        language_l="en",
        language_v="en",
    )
    collection.add_requiredStatement(
        label="Attribution",
        value="Hospedado por imagineRio",
        language_l="pt-BR",
        language_v="pt-BR",
    )
    collection.set_rights("http://rightsstatements.org/vocab/CNE/1.0/")

    thumbnailobj = iiifpapi3.thumbnail()

    if name == "views" or name == "all":
        thumb_id = "0071824cx001-01/full/295,221/0/default.jpg"
        h, w = 221, 295
    elif name == "plans":
        thumb_id = "10639297/full/259,356/0/default.jpg"
        h, w = 356, 259
    elif name == "maps":
        thumb_id = "10643717/full/512,259/0/default.jpg"
        h, w = 259, 512
    elif name == "aerials":
        thumb_id = "24879867/full/394,260/0/default.jpg"
        h, w = 260, 394
    elif name == "mare":
        thumb_id = "31770323/full/188,125/0/default.jpg"
        h, w = 125, 188
    else:
        thumb_id = "null"
        h, w = 1, 1

    thumbnailobj.set_id(extendbase_url=thumb_id)
    thumbnailobj.set_hightwidth(h, w)
    collection.add_thumbnail(thumbnailobj=thumbnailobj)
    collection.add_provider(collection_provider)
    collection.add_manifest_to_items(manifest)

    collection.orjson_save(
        collection_path, context="http://iiif.io/api/presentation/3/context.json"
        )

    return collection


def file_exists(identifier, type):

    if type == "info" or type == "manifest":
        key = "iiif/{0}/{1}.json".format(identifier, type)
    else:
        key = "iiif/{0}/full/max/0/default.jpg".format(identifier)

    s3 = boto3.resource("s3")
    bucket = s3.Bucket("imaginerio-images")
    objs = list(bucket.objects.filter(Prefix=key))
    if any([obj.key == key for obj in objs]):
        return True
    else:
        return False


def invalidate_cache(path):
    cloudfront = boto3.client("cloudfront")
    get_timestamp = lambda x: datetime.timestamp(x)
    cloudfront.create_invalidation(
        DistributionId=os.environ["DISTRIBUTION_ID"],
        InvalidationBatch={
            'Paths': {
                'Quantity': 1,
                'Items': ["/"+path]
            },
            'CallerReference': str(get_timestamp(datetime.now()))
        }
    )

def fast_upload(session, bucket_name, files, progress_func, workers=20):
    botocore_config = botocore.config.Config(max_pool_connections=workers)
    s3client = session.client('s3', config=botocore_config)
    transfer_config = s3transfer.TransferConfig(
        use_threads=True,
        max_concurrency=workers,
    )
    s3t = s3transfer.create_transfer_manager(s3client, transfer_config)
    for path in files:
        s3t.upload(
            path, bucket_name, path,
            subscribers=[
                s3transfer.ProgressCallbackInvoker(progress_func),
            ],
            extra_args={
                "ContentType": "image/jpeg"
                if path.endswith(".jpg")
                else "application/json"
            },
        )
    s3t.shutdown()  # wait for all the upload tasks to finish

def upload_folder_to_s3(source, mode="test"):
    s3 = boto3.resource("s3")
    for root, _, files in os.walk(source):
        for file in files:
            path = os.path.join(root, file)
            if mode == "test":
                continue
                #logger.debug("Would be uploading {0}".format(path))
            else:
                s3.meta.client.upload_file(
                    path,
                    "imaginerio-images",
                    path,
                    ExtraArgs={
                        "ContentType": "image/jpeg"
                        if file.endswith(".jpg")
                        else "application/json"
                    },
                )
                #invalidate_cache(path)

    if mode == "test":
        return False
    else:
        shutil.rmtree(source)
        return True


def upload_file_to_s3(source, target, mode="test"):
    s3 = boto3.resource("s3")
    if mode == "test":
        #logger.debug("Would be uploading {0} to {1}".format(source, target))
        return False
    else:
        s3.meta.client.upload_file(
            source,
            "imaginerio-images",
            target,
            ExtraArgs={
                "ContentType": "image/jpeg"
                if source.endswith(".jpg")
                else "application/json"
            },
        
        )
        return True


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
        os.environ["IMS_METADATA"], 
        index_col="Document ID", 
        converters={
            "First Year": float2str, 
            "Last Year":float2str, 
            "Width":float2str, 
            "Height":float2str
            }
        )
    metadata.update(df)
    metadata.to_csv(os.environ["IMS_METADATA"])
    #return metadata


def ims2jstor():
    jstor = pd.read_excel(os.environ["JSTOR"])
    jstor.set_index("Document ID[19474]", inplace=True)
    ims = pd.read_csv(
        os.environ["IMS_METADATA"], 
        index_col="Document ID", 
        converters={
            "First Year": float2str, 
            "Last Year":float2str, 
            "Width":float2str, 
            "Height":float2str
            }
        )
    digitized = ims["Media URL"].notna()
    published = ims["Document URL"].notna()
    not_in_jstor = ~(ims.index.isin(jstor.index))
    has_dates = (ims["First Year"].notna() & ims["Last Year"].notna())
    ims2jstor = ims.loc[has_dates & digitized & published & not_in_jstor].copy()
    ims2jstor.loc[ims2jstor["Creator"] == "Autoria não identificada", "Creator"] = "Unknown Authorship"
    ims2jstor.rename(
        columns={
            "Title":"Title[19462]",
            "Date": "Date[19486]",
            "First Year": "First Year[19466]",
            "Last Year": "Last Year[19467]",
            "Creator": "Creator[1603501]",
            "Description (Portuguese)":"Description (Portuguese)[1612567]",
            "Type":"Type[1604106]",
            "Collections":"Collections[1711006]",
            "Provider":"Provider[1731287]",
            "Material":"Material[1612569]",
            "Fabrication Method":"Fabrication Method[1612568]",
            "Rights":"Rights[1861241]",
            "Required Statement":"Required Statement[19484]",
            "Width":"Width[1604102]",
            "Height":"Height[1604103]",
            "Document URL":"Document URL[796463]",
        }, 
        inplace=True
    )
    ims2jstor[[column for column in jstor.columns if column not in ims2jstor.columns]] = ""
    ims2jstor["SSID"] = "NEW"
    ims2jstor.index.rename("Document ID[19474]", inplace=True)
    ims2jstor.to_excel(os.environ["IMS2JSTOR"])
