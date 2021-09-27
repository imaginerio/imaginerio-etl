import collections
import json
import os
import shutil
import subprocess
from math import *
from typing import Collection

import boto3
import dagster as dg
import pandas as pd
import requests
from dagster.builtins import Nothing
from dotenv import load_dotenv
from IIIFpres import iiifpapi3
from IIIFpres.utilities import read_API3_json

from solids.export import *

load_dotenv(override=True)
iiifpapi3.BASE_URL = "https://imaginerio-images.s3.us-east-1.amazonaws.com"
iiifpapi3.LANGUAGES = ["pt-BR", "en"]


@dg.solid(
    input_defs=[
        dg.InputDefinition("import_omeka", root_manager_key="import_omeka_root")
    ],
)
def list_of_items(_, import_omeka):
    items = import_omeka["dcterms:identifier"].to_list()
    to_do = []
    for item in items:
        # colocar retry
        response = requests.get(
            "https://imaginerio-images.s3.us-east-1.amazonaws.com/iiif-img/{0}/info.json".format(
                item
            )
        )

        if not response.status_code == 200:
            to_do.append(item)

    return items[0]


@dg.solid(config_schema=dg.StringSource)
def image_tiling(context, image):
    tmp_path = context.solid_config

    img_data = requests.get(
        "https://imaginerio-images.s3.us-east-1.amazonaws.com/iiif-img/{0}/full/max/0/default.jpg".format(
            image
        )
    ).content

    img_path = f"{tmp_path}/{image}.jpg"
    if not (os.path.exists(tmp_path)):
        os.mkdir(tmp_path)
    with open(img_path, "wb") as handler:
        handler.write(img_data)

    info_path = "{0}/iiif-img/{1}/info.json".format(
        tmp_path,
        image,
    )
    if not (os.path.exists(info_path)):
        command = "python iiif/iiif_static.py -d 'data/output/tmp/iiif-img' -t 256 -a 3.0 --osd-version 1.0.0 -e '/full/!400,400/0/default.jpg' -p 'https://imaginerio-images.s3.us-east-1.amazonaws.com/iiif-img' '{0}'".format(
            img_path
        )

        process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        errors = process.communicate()
        print(errors)

    info = json.load(open(info_path))
    return info


@dg.solid(
    config_schema=dg.StringSource,
    input_defs=[
        dg.InputDefinition("import_omeka", root_manager_key="import_omeka_root")
    ],
)
def write_manifest(context, info, import_omeka):
    id = info["id"].split("iiif-img/")[-1]
    tmp_path = context.solid_config
    item = import_omeka.index[import_omeka["dcterms:identifier"] == id].tolist()[0]
    metadata_df = import_omeka.iloc[item]
    # fazer o loc pelo identificador

    collection_path = os.path.join(
        "{0}/iiif/collection/imaginerio.json".format(tmp_path)
    )
    item = import_omeka.index[import_omeka["dcterms:identifier"] == id].tolist()[0]
    metadata_df = import_omeka.iloc[item]

    # Logo
    logo = iiifpapi3.logo()
    logo.set_id(
        "https://aws1.discourse-cdn.com/free1/uploads/imaginerio/original/1X/8c4f71106b4c8191ffdcafb4edeedb6f6f58b482.png"
    )
    logo.set_format("image/png")
    logo.set_hightwidth(164, 708)

    if not os.path.exists(collection_path):
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
            extendbase_url="iiif-img/tiles/0071824cx001-01/full/144,95/0/default.jpg"
        )
        thumbnailobj.set_hightwidth(95, 144)
        collection.add_thumbnail(thumbnailobj=thumbnailobj)

        collection.add_provider(collection_provider)

    else:
        current_collection = read_API3_json(collection_path)
        # collection = json.load(open(collection_path))

    # try:
    # Header
    manifest = iiifpapi3.Manifest()
    manifest.set_id(
        extendbase_url="iiif/{0}/manifest".format(str(id).replace(" ", "_"))
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
    FABMETHOD_EN = tuple(metadata_df["dcterms:medium:en"].split(" ", maxsplit=1))
    FABMETHOD_PT = tuple(metadata_df["dcterms:medium:pt"].split(" ", maxsplit=1))
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
        extendbase_url="tiles/{0}/full/144,95/0/default.jpg".format(
            str(id).replace(" ", "_")
        )
    )
    thumbnail.set_hightwidth(150, 200)
    manifest.add_thumbnail(thumbnailobj=thumbnail)

    # Homepage
    item_homepage = iiifpapi3.homepage()
    homepage_id, homepage_label = metadata_df["dcterms:source"].split(" ", maxsplit=1)
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

    # Get image sizes

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
        extendbase_url="tiles/{0}/full/144,95/0/default.jpg".format(id)
    )
    annotation.body.set_type("Image")
    annotation.body.set_format("image/jpeg")
    annotation.body.set_width(imgwidth)
    annotation.body.set_height(imgheight)
    s = annotation.body.add_service()
    s.set_id(extendbase_url="tiles/{0}/".format(id))
    s.set_type("ImageService3")
    s.set_profile("level0")

    # Save manifest
    # manifest.inspect()

    if not os.path.exists(tmp_path):
        os.mkdir(tmp_path)

    file = os.path.join("{0}/iiif/manifests/{1}.json".format(tmp_path, id))
    manifest.json_save(file)
    collection.add_manifest_to_items(manifest)
    collection.json_save(collection_path)

    return manifest


@dg.solid(config_schema=dg.StringSource)
def upload_to_cloud(context, manifest):
    tmp_path = context.solid_config

    S3 = boto3.client("s3")
    BUCKET = "imaginerio-images"
    upload_log = [upload_to_cloud]
    for root, dirs, files in os.walk(tmp_path):
        for file in files:
            path = root.split("tmp/")[-1] + "/" + file
            try:
                upload = S3.upload_file(os.path.join(root, file), BUCKET, path)
                upload_log.append(upload)
            except:
                print("Couldn't upload image {0}, skipping".format(id))

    # Delete uploaded files
    if all(upload_log):
        for root, dirs, files in os.walk(tmp_path):
            for file in files:
                os.unlink("{0}/{1}".format(root, file))


@dg.composite_solid
def create_manifest(list):
    for item in list:
        info = image_tiling(item)
        manifest = write_manifest(info)  #
        upload_to_cloud(manifest)
