import io
import os
import subprocess
from math import *
from typing import Collection, List, Mapping

import dagster as dg
import pandas as pd
import requests
from dotenv import load_dotenv
from IIIFpres import iiifpapi3
from IIIFpres.utilities import *
from PIL import Image
from requests.adapters import HTTPAdapter
from requests.api import get
from urllib3.util import Retry

from solids.export import *

load_dotenv(override=True)

iiifpapi3.BASE_URL = "https://imaginerio-images.s3.us-east-1.amazonaws.com/"
iiifpapi3.LANGUAGES = ["pt-BR", "en"]


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


@dg.solid(output_defs=[dg.OutputDefinition(io_manager_key="iiif_manager")])
def tile_image(context, item):
    identifier = item["identifier"]

    if not info_json_exists(identifier) or context.mode_def.name == "test":
        img_data = requests.get(
            "https://imaginerio-images.s3.us-east-1.amazonaws.com/iiif/{0}/full/max/0/default.jpg".format(
                identifier
            )
        ).content

        img_path = f"iiif/{identifier}.jpg"

        if not os.path.exists("iiif"):
            os.mkdir("iiif")

        with open(img_path, "wb") as handler:
            handler.write(img_data)

        command = ["java", "-jar", "utils/mod-tiler.jar", img_path]

        process = subprocess.Popen(
            command,
        )
        process.communicate()
        os.remove(img_path)

        return [
            {"data": "iiif/{0}".format(identifier), "type": "path"},
        ]
    else:
        pass


@dg.solid(
    input_defs=[dg.InputDefinition("mapping", root_manager_key="mapping_root")],
    output_defs=[dg.OutputDefinition(io_manager_key="iiif_manager")],
)
def write_manifests(context, item, mapping):
    identifier = item["identifier"]
    item = item["row"]
    context.log.info("Processing: {0}".format(str(identifier)))

    # Get image sizes
    img = requests.get(
        "https://imaginerio-images.s3.us-east-1.amazonaws.com/iiif/{0}/full/max/0/default.jpg".format(
            identifier
        )
    ).content
    image = Image.open(io.BytesIO(img))
    imgwidth, imgheight = image.size
    mapping.set_index("Label:en", inplace=True)

    def get_multilingual_values(en):
        url = "http://wikidata.org/wiki/{0}".format(mapping.loc[en, "Wiki ID"])
        pt = mapping.loc[en, "Label:pt"]
        return {"url": url, "en": str(en), "pt": str(pt)}

    # Values
    title = str(item["Title"])
    description_en = (
        str(item["Description (English)"])
        if item["Description (English)"] != ""
        else item["Description (Portuguese)"]
    )
    description_pt = (
        str(item["Description (Portuguese)"])
        if item["Description (Portuguese)"] != ""
        else item["Description (English)"]
    )
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
            str(identifier).replace(" ", "_")
        )
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
    if description_en or description_pt:
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
                        materials["url"], materials["pt"]
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
    manifest.add_summary(language="en", text=description_en)
    manifest.add_summary(language="pt-BR", text=description_pt)
    manifest.add_requiredStatement(
        label="Attribution",
        value="Hosted by imagineRio",
        language_l="en",
        language_v="en",
    )
    manifest.set_rights("http://rightsstatements.org/vocab/CNE/1.0/")

    # Thumbnail
    thumbnail = iiifpapi3.thumbnail()
    thumb_width = int(imgwidth / 16)
    thumb_height = int(imgheight / 16)
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

    # setup items to send to io_managers
    manifest_path = "iiif/{0}/manifest.json".format((identifier))
    manifest_obj = manifest.json_dumps(
        dumps_errors=False,
        ensure_ascii=False,
        context="http://iiif.io/api/presentation/3/context.json",
    )
    data = [{"data": manifest_obj, "key": manifest_path, "type": "json"}]

    # Logo
    logo = iiifpapi3.logo()
    logo.set_id(
        "https://aws1.discourse-cdn.com/free1/uploads/imaginerio/original/1X/8c4f71106b4c8191ffdcafb4edeedb6f6f58b482.png"
    )
    logo.set_format("image/png")
    logo.set_hightwidth(164, 708)

    for collection_name in item["Item Set"].lower().split("||"):
        collection_path = "iiif/collection/{0}.json".format(collection_name)

        try:
            if context.mode_def.name == "test":
                collection = read_API3_json(collection_path)
            else:
                endpoint = "https://imaginerio-images.s3.us-east-1.amazonaws.com/iiif/collection/{0}.json".format(
                    collection_name
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
                collection_data = http.get(endpoint).json()
                collection = read_API3_json_dict(collection_data)

            # Deal with duplicates
            for item in collection.items:
                if "{0}/manifest".format(identifier) in item.id:
                    collection.items.remove(item)

        except Exception as e:
            print(e)
            print("Couldn't find collection: {0}. Creating...".format(collection_name))
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
            collection.set_id(
                extendbase_url="iiif/collection/{0}".format(collection_name)
            )
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

        collection.add_manifest_to_items(manifest)

        # setup items to send to io_managers
        collection = collection.json_dumps(
            dumps_errors=False,
            ensure_ascii=False,
            context="http://iiif.io/api/presentation/3/context.json",
        )
        collections = {}
        collections["data"] = collection
        collections["key"] = collection_path
        collections["type"] = "json"
        data.append(collections)

        print("Collection updated: {0}".format(collection_name))

    return data


@dg.solid(
    input_defs=[
        dg.InputDefinition("metadata", root_manager_key="metadata_root"),
    ],
    output_defs=[dg.DynamicOutputDefinition(dict)],
)
def get_items(context, metadata):
    metadata.set_index("Source ID", inplace=True)
    metadata.dropna(
        subset=[
            "Source",
            "Latitude",
            "Source URL",
            "Media URL",
            "First Year",
            "Last Year",
        ],
        inplace=True,
    )
    metadata.fillna("",inplace=True)
    #metadata = metadata.loc[metadata["Source"] == "Instituto Moreira Salles"]
    if context.mode_def.name == "test":
        metadata = metadata[:2]
    for identifier, item in metadata.iterrows():
        yield dg.DynamicOutput(
            value={"identifier": identifier, "row": item},
            mapping_key=identifier.replace("-", "_"),
        )
