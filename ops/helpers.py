from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from IIIFpres import iiifpapi3
from IIIFpres.utilities import *
import requests
import os


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

    with open(collection_path, "w") as f:
        f.write(
            collection.json_dumps(
                dumps_errors=False,
                ensure_ascii=False,
                context="http://iiif.io/api/presentation/3/context.json",
            )
        )

    return collection


def file_exists(identifier, type):

    if type == "info" or type == "manifest":
        endpoint = os.environ["BUCKET"] + "/iiif/{0}/{1}.json".format(identifier, type)
    else:
        endpoint = os.environ["BUCKET"] + "/iiif/{0}/full/max/0/default.jpg".format(
            identifier
        )

    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"],
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    response = http.get(endpoint)

    if response.status_code == 200:
        return True
    else:
        return False
