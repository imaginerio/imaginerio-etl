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


def tile_image(image):

    if not os.path.exists("tmp"):
        os.mkdir("tmp")

    img_data = requests.get(
        "https://imaginerio-images.s3.us-east-1.amazonaws.com/iiif-img/{0}/full/max/0/default.jpg".format(
            image
        )
    ).content

    img_path = f"tmp/{image}.jpg"

    with open(img_path, "wb") as handler:
        handler.write(img_data)

    # info_path = "tmp/iiif-img/{0}/info.json".format(
    #    image,
    # )
    # if not (os.path.exists(info_path)):
    print("Tiling...")

    command = "python tiler/iiif_static.py -d 'tmp/iiif-img' -t 256 -a 3.0 --osd-version 1.0.0 -e '/full/!400,400/0/default.jpg' -p 'https://imaginerio-images.s3.us-east-1.amazonaws.com/iiif-img' '{0}'".format(
        img_path
    )

    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    process.communicate()

    os.remove(img_path)
    info = json.load(open("tmp/iiif-img/{0}/info.json".format(image)))
    return info


def write_manifest(info, import_omeka):
    print("Write_manifest...")
    id = info["id"].split("iiif-img/")[-1]
    import_omeka = import_omeka.set_index("dcterms:identifier")
    metadata_df = import_omeka.loc[id]

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
            str(id).replace(" ", "_"))
    )
    manifest.add_label("pt-BR", "{0}".format(metadata_df["dcterms:title"]))

    # Metadata
    manifest.add_metadata(
        entry={
            "label": {"en": ["Identifier"], "pt-BR": ["Identificador"]},
            "value": {"en": ["{0}".format(id)], "pt-BR": ["{0}".format(id)]},
        }
    )
    manifest.add_metadata(
        entry={
            "label": {"en": ["Title"], "pt-BR": ["Título"]},
            "value": {
                "en": ["{0}".format(metadata_df["dcterms:title"])],
                "pt-BR": ["{0}".format(metadata_df["dcterms:title"])],
            },
        }
    )
    manifest.add_metadata(
        entry={
            "label": {"en": ["Description"], "pt-BR": ["Descrição"]},
            "value": {
                "en": ["{0}".format(metadata_df["dcterms:description"])],
                "pt-BR": ["{0}".format(metadata_df["dcterms:description"])],
            },
        }
    )
    manifest.add_metadata(
        entry={
            "label": {"en": ["Creator"], "pt-BR": ["Autor"]},
            "value": {
                "en": ["{0}".format(metadata_df["dcterms:creator"])],
                "pt-BR": ["{0}".format(metadata_df["dcterms:creator"])],
            },
        }
    )
    manifest.add_metadata(
        entry={
            "label": {"en": ["Date"], "pt-BR": ["Data"]},
            "value": {
                "en": ["{0}".format(metadata_df["dcterms:date"])],
                "pt-BR": ["{0}".format(metadata_df["dcterms:date"])],
            },
        }
    )
    TYPE_EN = tuple(metadata_df["dcterms:type:en"].split(" ", maxsplit=1))
    TYPE_PT = tuple(metadata_df["dcterms:type:pt"].split(" ", maxsplit=1))
    manifest.add_metadata(
        entry={
            "label": {"en": ["Type"], "pt-BR": ["Tipo"]},
            "value": {
                "en": [
                    '<a class="uri-value-link"\ target="_blank"\ href=\{0}\>{1}</a>'.format(
                        *TYPE_EN
                    )
                ],
                "pt-BR": [
                    '<a class="uri-value-link"\ target="_blank"\ href=\{0}\>{1}</a>'.format(
                        *TYPE_PT
                    )
                ],
            },
        }
    )
    FORMAT_EN = tuple(metadata_df["dcterms:format:en"].split(" ", maxsplit=1))
    FORMAT_PT = tuple(metadata_df["dcterms:format:pt"].split(" ", maxsplit=1))
    manifest.add_metadata(
        entry={
            "label": {"en": ["Format"], "pt-BR": ["Formato"]},
            "value": {
                "en": [
                    '<a class="uri-value-link"\ target="_blank"\ href=\{0}\>{1}</a>'.format(
                        *FORMAT_EN
                    )
                ],
                "pt-BR": [
                    '<a class="uri-value-link"\ target="_blank"\ href=\{0}\>{1}</a>'.format(
                        *FORMAT_PT
                    )
                ],
            },
        }
    )
    FABMETHOD_EN = tuple(
        metadata_df["dcterms:medium:en"].split(" ", maxsplit=1))
    FABMETHOD_PT = tuple(
        metadata_df["dcterms:medium:pt"].split(" ", maxsplit=1))
    manifest.add_metadata(
        entry={
            "label": {
                "en": ["Fabrication Method"],
                "pt-BR": ["Método de Fabricação"],
            },
            "value": {
                "en": [
                    '<a class="uri-value-link"\ target="_blank"\ href=\{0}\>{1}</a>'.format(
                        *FABMETHOD_EN
                    )
                ],
                "pt-BR": [
                    '<a class="uri-value-link"\ target="_blank"\ href=\{0}\>{1}</a>'.format(
                        *FABMETHOD_PT
                    )
                ],
            },
        }
    )
    manifest.add_metadata(
        entry={
            "label": {"en": ["Width (mm)"], "pt-BR": ["Largura (mm)"]},
            "value": {
                "en": ["{0}".format(metadata_df["schema:width"])],
                "pt-BR": ["{0}".format(metadata_df["schema:width"])],
            },
        }
    )
    manifest.add_metadata(
        entry={
            "label": {"en": ["Height (mm)"], "pt-BR": ["Altura (mm)"]},
            "value": {
                "en": ["{0}".format(metadata_df["schema:height"])],
                "pt-BR": ["{0}".format(metadata_df["schema:height"])],
            },
        }
    )

    # Rights & Attribution
    manifest.add_summary(
        language="en", text="{0}".format(metadata_df["dcterms:description"])
    )
    manifest.add_summary(
        language="pt-BR", text="{0}".format(metadata_df["dcterms:description"])
    )
    manifest.add_requiredStatement(
        label="Attribution",
        value="Hosted by imagineRio",
        language_l="en",
        language_v="en",
    )
    manifest.set_rights("http://rightsstatements.org/vocab/CNE/1.0/")

    # Thumbnail
    thumbnail = iiifpapi3.thumbnail()
    thumbnail.set_id(
        extendbase_url="iiif-img/{0}/full/144,95/0/default.jpg".format(
            str(id).replace(" ", "_")
        )
    )
    thumbnail.set_hightwidth(150, 200)
    manifest.add_thumbnail(thumbnailobj=thumbnail)

    # Homepage
    item_homepage = iiifpapi3.homepage()
    homepage_id, homepage_label = metadata_df["dcterms:source"].split(
        " ", maxsplit=1)
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

    # Get image sizeslist

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
        extendbase_url="iiif-img/{0}/full/144,95/0/default.jpg".format(id)
    )
    annotation.body.set_type("Image")
    annotation.body.set_format("image/jpeg")
    annotation.body.set_width(imgwidth)
    annotation.body.set_height(imgheight)
    s = annotation.body.add_service()
    s.set_id(extendbase_url="iiif-img/{0}/".format(id))
    s.set_type("ImageService3")
    s.set_profile("level0")

    # Save manifest
    if not os.path.exists("tmp/iiif/{0}".format(id)):
        os.makedirs("tmp/iiif/{0}".format(id))
    file = "tmp/iiif/{0}/manifest.json".format((id))
    manifest.json_save(file)
    return manifest


def write_collection(manifest):
    print("Collection...")
    collection_path = "tmp/iiif/collection/imaginerio.json"
    # Logo
    logo = iiifpapi3.logo()
    logo.set_id(
        "https://aws1.discourse-cdn.com/free1/uploads/imaginerio/original/1X/8c4f71106b4c8191ffdcafb4edeedb6f6f58b482.png"
    )
    logo.set_format("image/png")
    logo.set_hightwidth(164, 708)
    if not os.path.exists(collection_path):
        try:
            endpoint = "https://imaginerio-images.s3.us-east-1.amazonaws.com/iiif/collection/imaginerio.json"
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
            os.mkdir("tmp/iiif/collection")
            collection = read_API3_json_dict(collection_data)
            collection.json_save(collection_path)
            print("Collection downloaded!")

        except Exception as e:
            print(e)
            print("Couldn't find collection. Writing...")
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
            collection.set_id(extendbase_url="iiif/collection/imaginerio")
            collection.add_label("en", "imagineRio")
            collection.add_label("pt-BR", "imagineRio")
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
                extendbase_url="iiif-img/0071824cx001-01/full/144,95/0/default.jpg"
            )
            thumbnailobj.set_hightwidth(95, 144)
            collection.add_thumbnail(thumbnailobj=thumbnailobj)

            collection.add_provider(collection_provider)

            if not os.path.exists("tmp/iiif/collection"):
                os.makedirs("tmp/iiif/collection")
    else:
        collection = read_API3_json(collection_path)

    collection.add_manifest_to_items(manifest)
    collection.json_save(collection_path)
    print("Collection updated!")
    return collection_path


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


@dg.solid
def set_up(context):
    if not os.path.exists("tmp"):
        os.mkdir("tmp")
        os.mkdir("tmp/iiif")
        os.mkdir("tmp/iiif-img")

    return "ok"


@dg.solid(
    config_schema={"slice_debug": dg.BoolSource},
    input_defs=[
        dg.InputDefinition(
            "import_omeka", root_manager_key="import_omeka_root")
    ],
)
def list_items(context, import_omeka, ok):
    items = import_omeka["dcterms:identifier"].to_list()
    to_do = []

    if context.solid_config["slice_debug"]:
        items = items[:2]

    else:
        pass
    for item in items:
        endpoint = "https://imaginerio-images.s3.us-east-1.amazonaws.com/iiif-img/{0}/info.json".format(
            item
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

        if not response.status_code == 200:
            to_do.append(item)
    return to_do


@dg.solid(
    config_schema={
        "upload": dg.BoolSource,
    },
    input_defs=[
        dg.InputDefinition(
            "import_omeka", root_manager_key="import_omeka_root")
    ],
)
def create_manifest(
    context,
    to_do,
    import_omeka,
):
    upload = context.solid_config["upload"]
    for item in to_do:
        context.log.info("Processing: {0}".format(str(item)))
        info = tile_image(item)
        manifest = write_manifest(info, import_omeka)
        write_collection(manifest)

        if upload:
            upload_to_cloud()
