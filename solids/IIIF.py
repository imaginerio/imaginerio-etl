import os
import shutil
import urllib

import boto3
import dagster as dg
import pandas as pd
import requests
from dagster.builtins import Nothing
from dotenv import load_dotenv
from IIIFpres import iiifpapi3
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from tqdm.notebook import tqdm
from urllib3.util import Retry

import IIIF
from solids.export import *
from math import *
import subprocess

load_dotenv(override=True)
iiifpapi3.BASE_URL = "hthttps://imaginerio-images.s3.us-east-1.amazonaws.com"
iiifpapi3.LANGUAGES = ["pt-BR", "en"]


@dg.solid(
    config_schema=dg.StringSource,
    input_defs=[
        dg.InputDefinition("import_omeka", root_manager_key="import_omeka_root")
    ],
)
def list_of_items(context, import_omeka):
    to_do = import_omeka["dcterms:identifier"].to_list()
    return to_do


@dg.solid(config_schema=dg.StringSource)
def image_tiling(context, image):
    tmp_path = context.solid_config

    img_data = requests.get(
        "https://imaginerio-images.s3.us-east-1.amazonaws.com/iiif-img/{0}.jpg".format(
            image
        )
    ).content
    img_path = f"{tmp_path}/{image}.jpg"
    if not (os.path.exists(tmp_path)):
        os.mkdir(tmp_path)
    with open(img_path, "wb") as handler:
        handler.write(img_data)

    command = f"python iiif/iiif_static.py -d '{tmp_path}/tiles' -t 256 -a 3.0 --osd-version 1.0.0 -e '/full/!600,600/0/default.jpg' -p 'https://imaginerio-images.s3.us-east-1.amazonaws.com/iiif-img' {img_path}"
    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
)
    output, errors = process.communicate()
    print(f"command: {command} \noutput: {output} \nERRO: {errors}")
        # exec(/mnt/d/situated-views-etl/tmp/iiif/iiif_static.py -d "{tmp_path}/tiles" -t 256 -a 3.0 --osd-version 1.0.0 -e "/full/!600,600/0/default.jpg" -p "https://imaginerio-images.s3.us-east-1.amazonaws.com/iiif-img" $img_path)
        os.remove(img_path)

        return (f"{tmp_folder}info.json").json()


@dg.solid(
    config_schema=dg.StringSource,
    input_defs=[
        dg.InputDefinition("import_omeka", root_manager_key="import_omeka_root")
    ],
)
def write_manifest(context, info, import_omeka):
    tmp_folder = context.solid_config
    metadata = import_omeka

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
    collection.set_id(extendbase_url="manifests/imaginerio")
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
        extendbase_url="tiles/0071824cx001-01/full/144,95/0/default.jpg"
    )
    thumbnailobj.set_hightwidth(95, 144)
    collection.add_thumbnail(thumbnailobj=thumbnailobj)

    collection.add_provider(collection_provider)

    pbar = tqdm(
        metadata.iterrows(), total=metadata.shape[0], desc="Creating manifests..."
    )
    for index, row in pbar:
        pbar.set_postfix_str("{0}".format(index))
        # try:
        # Header
        manifest = iiifpapi3.Manifest()
        manifest.set_id(
            extendbase_url="{0}/manifest".format(str(index).replace(" ", "_"))
        )
        manifest.add_label("pt-BR", "{0}".format(row["dcterms:title"]))

        # Metadata
        manifest.add_metadata(
            entry={
                "label": {"en": ["Identifier"], "pt-BR": ["Identificador"]},
                "value": {"en": ["{0}".format(index)], "pt-BR": ["{0}".format(index)]},
            }
        )
        manifest.add_metadata(
            entry={
                "label": {"en": ["Title"], "pt-BR": ["Título"]},
                "value": {
                    "en": ["{0}".format(row["dcterms:title"])],
                    "pt-BR": ["{0}".format(row["dcterms:title"])],
                },
            }
        )
        manifest.add_metadata(
            entry={
                "label": {"en": ["Description"], "pt-BR": ["Descrição"]},
                "value": {
                    "en": ["{0}".format(row["dcterms:description"])],
                    "pt-BR": ["{0}".format(row["dcterms:description"])],
                },
            }
        )
        manifest.add_metadata(
            entry={
                "label": {"en": ["Creator"], "pt-BR": ["Autor"]},
                "value": {
                    "en": ["{0}".format(row["dcterms:creator"])],
                    "pt-BR": ["{0}".format(row["dcterms:creator"])],
                },
            }
        )
        manifest.add_metadata(
            entry={
                "label": {"en": ["Date"], "pt-BR": ["Data"]},
                "value": {
                    "en": ["{0}".format(row["dcterms:date"])],
                    "pt-BR": ["{0}".format(row["dcterms:date"])],
                },
            }
        )
        TYPE_EN = tuple(row["dcterms:type:en"].split(" ", maxsplit=1))
        TYPE_PT = tuple(row["dcterms:type:pt"].split(" ", maxsplit=1))
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
        FORMAT_EN = tuple(row["dcterms:format:en"].split(" ", maxsplit=1))
        FORMAT_PT = tuple(row["dcterms:format:pt"].split(" ", maxsplit=1))
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
        FABMETHOD_EN = tuple(row["dcterms:medium:en"].split(" ", maxsplit=1))
        FABMETHOD_PT = tuple(row["dcterms:medium:pt"].split(" ", maxsplit=1))
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
                    "en": ["{0}".format(row["schema:width"])],
                    "pt-BR": ["{0}".format(row["schema:width"])],
                },
            }
        )
        manifest.add_metadata(
            entry={
                "label": {"en": ["Height (mm)"], "pt-BR": ["Altura (mm)"]},
                "value": {
                    "en": ["{0}".format(row["schema:height"])],
                    "pt-BR": ["{0}".format(row["schema:height"])],
                },
            }
        )

        # Rights & Attribution
        manifest.add_summary(
            language="en", text="{0}".format(row["dcterms:description"])
        )
        manifest.add_summary(
            language="pt-BR", text="{0}".format(row["dcterms:description"])
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
                str(index).replace(" ", "_")
            )
        )
        thumbnail.set_hightwidth(150, 200)
        manifest.add_thumbnail(thumbnailobj=thumbnail)

        # Homepage
        item_homepage = iiifpapi3.homepage()
        homepage_id, homepage_label = row["dcterms:source"].split(" ", maxsplit=1)
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
            extendbase_url="tiles/{0}/full/144,95/0/default.jpg".format(index)
        )
        annotation.body.set_type("Image")
        annotation.body.set_format("image/jpeg")
        annotation.body.set_width(imgwidth)
        annotation.body.set_height(imgheight)
        s = annotation.body.add_service()
        s.set_id(extendbase_url="tiles/{0}/".format(index))
        s.set_type("ImageService3")
        s.set_profile("level0")

        # Save manifest
        # manifest.inspect()
        if not os.path.exists(tmp_folder):
            os.mkdir(tmp_folder)
        manifest.json_save("{tmp_folder}/{0}.json".format(index))
        
        collection.add_manifest_to_items(manifest)

    return collection

@dg.solid(config_schema=dg.StringSource)
def upload_to_cloud(context, to_upload):
    tmp_folder = context.solid_config
    item_path = [f"{tmp_folder}/{item}" for item in to_upload] 

    S3 = boto3.client("s3")
    BUCKET = "imaginerio-images"
    for image_path in tqdm(item_path, "Uploading files..."):
        for root,dirs,files in os.walk(item_path):
            for file in files:      
                try:
                    S3.upload_file(os.path.join(root,file),BUCKET,file)
                    shutil.rmtree(tmp_folder)
                except:
                    print("Couldn't upload image {0}, skipping".format(id))    


@dg.composite_solid
def create_manifest(context, list):
    for item in tqdm(list):
        collection = write_manifest(image_tiling(item))
        upload_to_cloud(item)
    return collection
    

@dg.solid(config_schema=dg.StringSource)
def create_collection(context,collection):
    #collection_path = context.solid_config
    collection_path ="data/output/tmp/iiif/collection"
    collection.json_save("{0}/imaginerio.json".format(collection_path))
    return 
