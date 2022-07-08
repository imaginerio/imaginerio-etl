import os
import sys
import shutil

import boto3
import geojson
import mercantile
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
    if any([w.key == key for w in objs]):
        return True
    else:
        return False


def upload_folder_to_s3(source, mode="test"):
    s3 = boto3.resource("s3")
    for root, _, files in os.walk(source):
        for file in files:
            path = os.path.join(root, file)
            if mode == "test":
                print("Would be uploading {0}".format(path))
            else:
                s3.meta.client.upload_file(
                    path,
                    os.environ["BUCKET_NAME"],
                    path,
                    ExtraArgs={
                        "ContentType": "image/jpeg"
                        if file.endswith(".jpg")
                        else "application/json"
                    },
                )
    if mode == "test":
        return False
    else:
        shutil.rmtree(source)
        return True


def upload_file_to_s3(source, target, mode="test"):
    s3 = boto3.resource("s3")
    if mode == "test":
        print("Would be uploading {0} to {1}".format(source, target))
        return False
    else:
        s3.meta.client.upload_file(
            source,
            os.environ["BUCKET_NAME"],
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
