import os
import urllib

import dagster as dg
from dagster.builtins import Nothing
import pandas as pd
import requests
from dotenv import load_dotenv
from IIIFpres import iiifpapi3
from tqdm import tqdm
from tqdm.notebook import tqdm
import requests
import IIIF
from solids.export import *
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


load_dotenv(override=True)
iiifpapi3.BASE_URL = "https://rioiconography.sfo2.digitaloceanspaces.com/situatedviews/"
iiifpapi3.LANGUAGES = ["pt-BR", "en"]


@dg.solid(
    config_schema=dg.StringSource,
    input_defs=[
        dg.InputDefinition("import_omeka", root_manager_key="import_omeka_root")
    ],
)
def load_metadata(context, omeka, metadata):
    has_manifest = omeka["dcterms:identifier"].to_list()
    metadata.fillna(value="", inplace=True)
    metadata = metadata[~metadata["Source ID"].isin(has_manifest)]

    has_info = []
    for id in metadata["Source ID"].to_list():
        url = "https://imaginerio-images.s3.us-east-1.amazonaws.com/tiles/{id}/info.json"
        response = requests.get(url).status_code
        if response==200:
            has_info.append(id)      

    metadata = metadata[~metadata["Source ID"].isin(has_info)]
    return metadata



@dg.solid
def image_tiling(context, image):
    img_data = requests.get(
        "https://imaginerio-images.s3.us-east-1.amazonaws.com/originals/{0}.jpg".format(
            image
        )
    ).content
    img_path = "/content/tmp/{image}.jpg"

    if not (os.path.exists("/content/tmp")):
        os.mkdir("/content/tmp")
    with open(img_path, "wb") as handler:
        handler.write(img_data)

    # iiif_static.main()
    os.remove(img_path)

    info = "https://imaginerio-images.s3.us-east-1.amazonaws.com/iiif-img/{IDENTIFICADOR}/info.json"
    return info


@dg.solid
def write_manifest(context, info, df):
    metadata = df
    manifests_folder = context.

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
        iiifimageurl = "https://imaginerio-images.s3.us-east-1.amazonaws.com/iiif-img/{0}/info.json".format(
            index
        )
        
        
        imageinfo = requests.get(iiifimageurl)
        jsoninfo = imageinfo.json()
        imgwidth = jsoninfo["width"]
        imgheight = jsoninfo["height"]

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
        if not os.path.exists(manifests_folder):
            os.mkdir(manifests_folder)
        manifest.json_save("{manifests_folder}{0}.json".format(index))
        collection.add_manifest_to_items(manifest)


@dg.composite_solid
def create_manifest(context, df):
    to_do = df["Source ID"].to_list()
    for item in tqdm(to_do):
        write_manifest(image_tiling(item))
