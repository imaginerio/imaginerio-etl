import json

import os
import shutil
import subprocess
from math import *
from typing import Collection, List

import boto3
from botocore import endpoint
import dagster as dg
import pandas as pd
import requests
from dagster.builtins import Nothing, String
from dotenv import load_dotenv
from requests.api import get
from IIIFpres import iiifpapi3
from IIIFpres.utilities import *
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from tqdm import tqdm
from solids.export import *

load_dotenv(override=True)
iiifpapi3.BASE_URL = "https://imaginerio-images.s3.us-east-1.amazonaws.com/"
iiifpapi3.LANGUAGES = ["pt-BR", "en"]
# iiifpapi3.BASE_URL = "http://127.0.0.1:8000/"


def info_json_exists(identifier):

    endpoint = "https://imaginerio-images.s3.us-east-1.amazonaws.com/iiif/{0}/info.json".format(
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


def tile_image(identifier):

    img_data = requests.get(
        "https://imaginerio-images.s3.us-east-1.amazonaws.com/iiif/{0}/full/max/0/default.jpg".format(
            identifier
        )
    ).content

    img_path = f"tmp/{identifier}.jpg"

    if not os.path.exists("tmp"):
        os.mkdir("tmp")

    with open(img_path, "wb") as handler:
        handler.write(img_data)

    command = ["java", "-jar", "utils/mod-tiler.jar", img_path]

    process = subprocess.Popen(
        command,
        # cwd="src"
        # shell=True,
        # stdout=subprocess.PIPE,
        # stderr=subprocess.PIPE,
    )
    process.communicate()

    os.remove(img_path)
    info = json.load(open("tmp/iiif/{0}/info.json".format(identifier)))
    return info


def write_manifest(info, identifier, item, mapping):

    def get_multilingual_values(en):
        url = "http://wikidata.org/wiki/{0}".format(mapping.loc[en, "Wiki ID"])
        pt = mapping.loc[en, "Label:pt"]
        return{"url":url,"en":str(en),"pt":str(pt)}

    #Values
    title = str(item["Title"])
    description_en = str(item["Description (English)"]) if pd.notna(item["Description (English)"]) else item["Description (Portuguese)"]
    description_pt = str(item["Description (Portuguese)"]) if pd.notna(item["Description (Portuguese)"]) else item["Description (English)"]
    creator = str(item["Creator"])
    date = str(item["Date"])
    type = get_multilingual_values(item["Type"])
    materials = get_multilingual_values(item["Materials"])
    fabrication_method = get_multilingual_values(item["Fabrication Method"])
    width = str(item["Width (mm)"])
    height = str(item["Height (mm)"])
    source = str(item["Source"])
    source_url = str(item["Source URL"])

    # Logo
    logo = iiifpapi3.logo()
    logo.set_id(
        "https://aws1.discourse-cdn.com/free1/uploads/imaginerio/original/1X/8c4f71106b4c8191ffdcafb4edeedb6f6f58b482.png"
    )
    logo.set_format("image/png")
    logo.set_hightwidth(164, 708)

    # Header
    manifest = iiifpapi3.Manifest()
    manifest.set_id(
        extendbase_url="iiif/{0}/manifest.json".format(
            str(identifier).replace(" ", "_"))
    )
    manifest.add_label("pt-BR", title)

    # Metadata
    manifest.add_metadata(
        entry={
            "label": {"en": ["Identifier"], "pt-BR": ["Identificador"]},
            "value": {"en": [identifier], "pt-BR": [identifier]},
        }
    )
    manifest.add_metadata(
        entry={
            "label": {"en": ["Title"], "pt-BR": ["Título"]},
            "value": {
                "en": [title],
                "pt-BR": [title],
            },
        }
    )
    manifest.add_metadata(
        entry={
            "label": {"en": ["Description"], "pt-BR": ["Descrição"]},
            "value": {
                "en": [description_en],
                "pt-BR": [description_pt],
            },
        }
    )
    manifest.add_metadata(
        entry={
            "label": {"en": ["Creator"], "pt-BR": ["Autor"]},
            "value": {
                "en": [creator],
                "pt-BR": [creator],
            },
        }
    )
    manifest.add_metadata(
        entry={
            "label": {"en": ["Date"], "pt-BR": ["Data"]},
            "value": {
                "en": [date],
                "pt-BR": [date],
            },
        }
    )
    manifest.add_metadata(
        entry={
            "label": {"en": ["Type"], "pt-BR": ["Tipo"]},
            "value": {
                "en": [
                    '<a class="uri-value-link" target="_blank" href="{0}">{1}</a>'.format(
                        type["url"], type["en"]
                    )
                ],
                "pt-BR": [
                    '<a class="uri-value-link" target="_blank" href="{0}">{1}</a>'.format(
                        type["url"], type["pt"]
                    )
                ],
            },
        }
    )
    manifest.add_metadata(
        entry={
            "label": {"en": ["Materials"], "pt-BR": ["Materiais"]},
            "value": {
                "en": [
                    '<a class="uri-value-link" target="_blank" href="{0}">{1}</a>'.format(
                        materials["url"], materials["en"]
                    )
                ],
                "pt-BR": [
                    '<a class="uri-value-link" target="_blank" href="{0}">{1}</a>'.format(
                        materials["url"],materials["pt"]
                    )
                ],
            },
        }
    )
    manifest.add_metadata(
        entry={
            "label": {
                "en": ["Fabrication Method"],
                "pt-BR": ["Método de Fabricação"],
            },
            "value": {
                "en": [
                    '<a class="uri-value-link" target="_blank" href="{0}">{1}</a>'.format(
                        fabrication_method["url"], fabrication_method["en"]
                    )
                ],
                "pt-BR": [
                    '<a class="uri-value-link" target="_blank" href="{0}">{1}</a>'.format(
                        fabrication_method["url"], fabrication_method["pt"]
                    )
                ],
            },
        }
    )
    manifest.add_metadata(
        entry={
            "label": {"en": ["Width (mm)"], "pt-BR": ["Largura (mm)"]},
            "value": {
                "en": [width],
                "pt-BR": [width],
            },
        }
    )
    manifest.add_metadata(
        entry={
            "label": {"en": ["Height (mm)"], "pt-BR": ["Altura (mm)"]},
            "value": {
                "en": [height],
                "pt-BR": [height],
            },
        }
    )

    # Rights & Attribution
    manifest.add_summary(
        language="en", text=description_en)
    manifest.add_summary(
        language="pt-BR", text=description_pt)
    manifest.add_requiredStatement(
        label="Attribution",
        value="Hosted by imagineRio",
        language_l="en",
        language_v="en",
    )
    manifest.set_rights("http://rightsstatements.org/vocab/CNE/1.0/")

    # Thumbnail
    thumbnail = iiifpapi3.thumbnail()
    thumb_width = info["sizes"][0]["width"]
    thumb_height = info["sizes"][0]["height"]
    thumbnail.set_id(
        extendbase_url="iiif/{0}/full/{1},{2}/0/default.jpg".format(
            str(identifier).replace(" ", "_"), thumb_width, thumb_height
        )
    )
    thumbnail.set_hightwidth(150, 200)
    manifest.add_thumbnail(thumbnailobj=thumbnail)

    # Homepage
    item_homepage = iiifpapi3.homepage()
    homepage_id = source_url
    homepage_label = source
    try:
        item_homepage.set_id(objid=homepage_id)
        item_homepage.add_label(language="none", text=homepage_label)
    except:
        item_homepage.set_id("https://imaginerio.org/")
        item_homepage.add_label(language="none", text="imagineRio")
    item_homepage.set_type("Text")
    item_homepage.set_format("text/html")

    # Provider
    item_provider = iiifpapi3.provider()
    item_provider.set_id("https://imaginerio.org/")
    item_provider.add_label("en", "imagineRio")
    item_provider.add_label("pt-BR", "imagineRio")
    item_provider.add_logo(logo)
    item_provider.add_homepage(item_homepage)
    manifest.add_provider(item_provider)

    # Get image sizes list
    imgwidth = info["width"]
    imgheight = info["height"]

    # Canvas
    canvas = manifest.add_canvas_to_items()
    canvas.set_id(extendbase_url="canvas/p1")
    canvas.set_height(imgwidth)
    canvas.set_width(imgheight)
    canvas.add_label(language="none", text="Recto")

    annopage = canvas.add_annotationpage_to_items()
    annopage.set_id(extendbase_url="annotation-page/p1")
    annotation = annopage.add_annotation_to_items(target=canvas.id)
    annotation.set_id(extendbase_url="annotation/p1")
    annotation.set_motivation("painting")
    annotation.body.set_id(
        extendbase_url="iiif/{0}/full/max/0/default.jpg".format(identifier)
    )
    annotation.body.set_type("Image")
    annotation.body.set_format("image/jpeg")
    annotation.body.set_width(imgwidth)
    annotation.body.set_height(imgheight)
    s = annotation.body.add_service()
    s.set_id(extendbase_url="iiif/{0}/".format(identifier))
    s.set_type("ImageService3")
    s.set_profile("level0")

    # Save manifest
    if not os.path.exists("tmp/iiif/{0}".format(identifier)):
        os.makedirs("tmp/iiif/{0}".format(identifier))
    file = "tmp/iiif/{0}/manifest.json".format((identifier))
    manifest.json_save(file)
    return manifest


def append_to_collection(manifest, collection_name):
    collection_path = "tmp/iiif/collection/{0}.json".format(collection_name)
    # Logo
    logo = iiifpapi3.logo()
    logo.set_id(
        "https://aws1.discourse-cdn.com/free1/uploads/imaginerio/original/1X/8c4f71106b4c8191ffdcafb4edeedb6f6f58b482.png"
    )
    logo.set_format("image/png")
    logo.set_hightwidth(164, 708)
    try:
        endpoint = "https://imaginerio-images.s3.us-east-1.amazonaws.com/iiif/collection/{0}.json".format(collection_name)
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "OPTIONS"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)
        collection_data = http.get(endpoint).json()
        collection = read_API3_json_dict(collection_data)
    #TO-DO don't wait for json_save to throw exception, check for response code instead
    except Exception as e:
        print(e)
        print("Couldn't find collection. Creating...")
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
        collection.set_id(extendbase_url="iiif/collection/{0}".format(collection_name))
        collection.add_label("en", collection_name)
        collection.add_label("pt-BR", collection_name)
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
        thumbnailobj.set_id(
            extendbase_url="iiif/0071824cx001-01/full/144,95/0/default.jpg"
        )
        thumbnailobj.set_hightwidth(95, 144)
        collection.add_thumbnail(thumbnailobj=thumbnailobj)

        collection.add_provider(collection_provider)

    if not os.path.exists("tmp/iiif/collection"):
        os.mkdir("tmp/iiif/collection")
    collection.add_manifest_to_items(manifest)
    collection.json_save(collection_path)
    print("Collection updated!")


def upload_to_cloud():
    print("Uploading...")
    S3 = boto3.client("s3")
    BUCKET = "imaginerio-images"
    upload_log = []
    for root, dirs, files in os.walk("tmp"):
        for file in files:
            path = root.split("tmp/")[-1] + "/" + file
            content = (
                "image/jpeg" if file.split(
                    ".")[-1] == "jpg" else "application/json"
            )
            try:
                upload = S3.upload_file(
                    os.path.join(root, file),
                    BUCKET,
                    path,
                    ExtraArgs={"ContentType": content},
                )
                print(
                    os.path.join(root, file),
                    BUCKET,
                    path,
                    content,
                )
            except Exception as e:
                upload_log.append(path)
                print("Couldn't upload image {0}.Error: {1}".format(path, e))

    if not upload_log:
        print("Cleaning tmp folder...")
        shutil.rmtree("tmp")


@dg.solid(
    # config_schema={
    #     "debug": dg.BoolSource,
    # },
    input_defs=[
        dg.InputDefinition(
            "metadata", root_manager_key="metadata_root"),
        dg.InputDefinition(
            "mapping", root_manager_key="mapping_root"
        )
    ],
)
def iiify(
    context,
    metadata,
    mapping
):
    metadata.set_index("Source ID", inplace=True)
    mapping.set_index("Label:en", inplace=True)
    metadata.dropna(subset=["Source", "Latitude", "Source URL", "Media URL", "First Year", "Last Year"], inplace=True)
    metadata = metadata.loc[metadata["Source"]=="Instituto Moreira Salles"]
    if context.mode_def.name == "test":
        metadata = metadata[-2:]
    for identifier, item in metadata.iterrows():
        if info_json_exists(identifier) or context.solid_config["manifest_only"]:
            context.log.info("Skipping: {0}".format(str(identifier)))
            continue
        else:
            context.log.info("Processing: {0}".format(str(identifier)))
            info = tile_image(identifier)
            manifest = write_manifest(info, identifier, item, mapping)
            for collection_name in item["Item Set"].lower().split("||"):
                append_to_collection(manifest, collection_name)
            #if not context.solid_config["debug"]:
            if context.mode_def.name == "prod":
                upload_to_cloud()
