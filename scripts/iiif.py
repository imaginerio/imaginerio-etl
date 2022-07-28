import argparse
import collections
import logging
import logging.config
import os
import re
import subprocess
from json import JSONDecodeError
from math import *
from urllib.parse import urlsplit, urlunsplit

import jsonschema
import pandas as pd
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from dotenv import load_dotenv
from IIIFpres import iiifpapi3
from IIIFpres.utilities import *
from PIL import Image

from helpers import session, logger, upload_folder_to_s3, create_collection
from item import Item

load_dotenv(override=True)

# logging.config.dictConfig(
#     {
#         "version": 1,
#         "disable_existing_loggers": True,
#     }
# )
# logging.basicConfig(level=logging.DEBUG)
# logger = logging.getLogger(__name__)

BUCKET = os.environ["BUCKET"]
COLLECTIONS = "iiif/collection/"
iiifpapi3.BASE_URL = BUCKET + "iiif/"
iiifpapi3.LANGUAGES = ["pt-BR", "en"]
Image.MAX_IMAGE_PIXELS = None

def get_items(metadata, vocabulary, mode):

    os.makedirs(COLLECTIONS, exist_ok=True)

    # list all collection names
    collections = metadata["Collections"].dropna().str.split("\|").explode().unique()
    # download collection(s)
    for name in collections:
        collection_path = f"{COLLECTIONS}{name.lower()}.json"
        response = session.get(BUCKET + collection_path)
        if response.status_code == 200:
            with open(collection_path, "w") as f:
                json.dump(response.json(), f, indent=4)

    # save current and try to load previous data
    if mode == "prod":
        try:
            last_run = pd.read_pickle("data/input/last_run.pickle")
        except:
            last_run = None

        # pickle current data to compare against
        pd.to_pickle(metadata, "data/input/last_run.pickle")

        # process only new or modified items
        if last_run is not None:
            new_items = metadata.loc[~metadata.index.isin(last_run.index)]
            previous_items = metadata.loc[last_run.index]
            modified_items = last_run.compare(previous_items).index
            to_process = metadata.loc[modified_items].append(new_items)
        else:
            to_process = metadata
    else:
        to_process = metadata
        
    logger.debug(f"Processing {len(to_process)} items")

    return [Item(id, row, vocabulary) for id, row in to_process.fillna("").iterrows()]
    

def write_manifest(item):

    id = str(item._id)
    logger.debug("Creating manifest {0}".format(str(id)))

    # Get image sizes
    try:
        img_sizes = session.get(BUCKET + item._info_path).json()["sizes"]
    except JSONDecodeError:
        try:
            logger.debug("Attempting to download remote image")
            item.download_full_image()
            item.tile_image()
            img_sizes = json.load(open(item._info_path))["sizes"]
        except:
            logger.error(f"Image {id} unavailable")
            return False

    full_width, full_height = img_sizes[-1].values()
    # Pick size closer to 600px (long edge) for thumbnail
    thumb_width, thumb_height = min(
        img_sizes, key=lambda x: abs(min(x.values()) - 600)
    ).values()

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
        extendbase_url="{0}/manifest.json".format(str(id).replace(" ", "_"))
    )
    manifest.add_label("pt-BR", item._title)

    for field in item._metadata:
        item.get_metadata_entry(manifest, field)

    # Rights & Attribution
    if item._description["value_en"][0]:
        manifest.add_summary(language="en", text=item._description["value_en"][0])
        manifest.add_summary(language="pt-BR", text=item._description["value_pt"][0])

    required_statement = manifest.add_requiredStatement()
    required_statement.add_label("Attribution", "en")
    required_statement.add_label("Atribuição", "pt-BR")

    if item._attribution:
        required_statement.add_value(item._attribution + " Hosted by imagineRio.", "en")
        required_statement.add_value(
            item._attribution + " Hospedado por imagineRio.", "pt-BR"
        )
    else:
        required_statement.add_value("Hosted by imagineRio.", "en")
        required_statement.add_value("Hospedado por imagineRio.", "pt-BR")

    if item._rights.startswith("http"):
        manifest.set_rights(item._rights)
    else:
        manifest.set_rights("http://rightsstatements.org/vocab/CNE/1.0/")

    # Thumbnail
    thumbnail = iiifpapi3.thumbnail()
    thumbnail.set_id(
        extendbase_url="{0}/full/{1},{2}/0/default.jpg".format(
            str(id).replace(" ", "_"), thumb_width, thumb_height
        )
    )
    thumbnail.set_hightwidth(thumb_height, thumb_width)
    manifest.add_thumbnail(thumbnailobj=thumbnail)

    # Homepage
    item_homepage = iiifpapi3.homepage()

    try:
        item_homepage.set_id(objid=item._document_url)
    except:
        url_parts = list(urlsplit(item._document_url))
        url_parts[2:5] = ["", "", ""]
        new_url = urlunsplit(url_parts)
        item_homepage.set_id(objid=new_url)

    # homepage_label = item["Provider"] if item["Provider"] else "imagineRio"
    item_homepage.add_label(language="none", text=item._provider)
    item_homepage.set_type("Text")
    item_homepage.set_format("text/html")
    manifest.add_homepage(item_homepage)

    # See Also
    imaginerio_seealso = iiifpapi3.seeAlso()
    imaginerio_seealso.set_id(objid="https://www.imaginerio.org/map#{0}".format(id))
    imaginerio_seealso.add_label(language="none", text="imagineRio")
    imaginerio_seealso.set_type("Text")
    manifest.add_seeAlso(imaginerio_seealso)

    if item._wikidata_id:
        wikidata_seealso = iiifpapi3.seeAlso()
        wikidata_seealso.set_id(
            objid="https://www.wikidata.org/wiki/{0}".format(item._wikidata_id)
        )
        wikidata_seealso.add_label(language="none", text="Wikidata")
        wikidata_seealso.set_type("Text")
        # wikidata_seealso.set_format("text/html")
        manifest.add_seeAlso(wikidata_seealso)

    if item._smapshot_id:  # ["Smapshot ID"]
        smapshot_seealso = iiifpapi3.seeAlso()
        smapshot_seealso.set_id(
            objid="https://smapshot.heig-vd.ch/visit/{0}".format(
                item._smapshot_id
            )  # ["Smapshot ID"]
        )
        smapshot_seealso.add_label(language="none", text="Smapshot")
        smapshot_seealso.set_type("Text")
        # smapshot_seealso.set_format("text/html")
        manifest.add_seeAlso(smapshot_seealso)

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
    canvas.set_width(full_width)
    canvas.set_height(full_height)
    canvas.add_label(language="none", text=id)

    annopage = canvas.add_annotationpage_to_items()
    annopage.set_id(extendbase_url="annotation-page/p1")
    annotation = annopage.add_annotation_to_items(target=canvas.id)
    annotation.set_id(extendbase_url="annotation/p1")
    annotation.set_motivation("painting")
    annotation.body.set_id(extendbase_url="{0}/full/max/0/default.jpg".format(id))
    annotation.body.set_type("Image")
    annotation.body.set_format("image/jpeg")
    annotation.body.set_width(full_width)
    annotation.body.set_height(full_height)
    s = annotation.body.add_service()
    s.set_id(extendbase_url=id)
    s.set_type("ImageService3")
    s.set_profile("level0")

    # validate manifest
    with open("data/input/iiif_3_0_schema.json") as schema:
        try:
            jsonschema.validate(
                instance=json.loads(
                    manifest.orjson_dumps(
                        context="http://iiif.io/api/presentation/3/context.json"
                    )
                ),
                schema=json.load(schema),
            )
        except jsonschema.exceptions.ValidationError as e:
            logger.error("Manifest {0} is invalid\n".format(id), e)
            return False

    # Add to collection
    for collection_name in item._collections.split("|"):  # ["Collections"]
        if collection_name:
            try:
                collection = read_API3_json(
                    f"{COLLECTIONS}/{collection_name.lower()}.json"
                )
                for current_item in collection.items:
                    if f"/{id}/" in current_item.id:
                        logger.debug(f"Updating manifest {id} in {collection_name}")
                        collection.items.remove(current_item)
                collection.add_manifest_to_items(manifest)
                collection.orjson_save(f"{COLLECTIONS}/{collection_name}.json")
            except FileNotFoundError:
                logger.debug("Collection doesn't exist, creating...")
                create_collection(collection_name, manifest)
        else:
            continue

    os.makedirs(item._base_path, exist_ok=True)

    manifest.orjson_save(
        item._manifest_path, context="http://iiif.io/api/presentation/3/context.json"
    )
    logger.debug(f"Saved manifest at {item._manifest_path}")

    return True


if __name__ == "__main__":

    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", "-m", help="run mode", choices=["test", "prod"], default="test"
    )
    args = parser.parse_args()

    # open files and rename columns
    vocabulary = pd.read_excel(os.environ["VOCABULARY"])
    vocabulary.rename(columns=lambda x: re.sub(r"\[[0-9]*\]", "", x), inplace=True)
    vocabulary.set_index("Label (en)", inplace=True)

    metadata = pd.read_excel(os.environ["JSTOR"])
    metadata.rename(columns=lambda x: re.sub(r"\[[0-9]*\]", "", x), inplace=True)
    metadata.set_index("SSID", inplace=True)

    # filter rows
    if args.mode == "test":
        metadata = pd.DataFrame(metadata.iloc[0]).T
    else:
        metadata = metadata.loc[metadata["Status"] == "In imagineRio"]

    # filter columns
    metadata = metadata[[
        "Document ID",
        "Rights",
        "Provider",
        "Collections",
        "Type",
        "Material",
        "Fabrication Method",
        "Media URL",
        "Creator",
        "Title",
        "Description (Portuguese)",
        "Description (English)",
        "Date",
        "First Year",
        "Last Year",
        "Document URL",
        "Required Statement",
        "Wikidata ID",
        "Smapshot ID",
        "Depicts",
        "Width",
        "Height"
    ]]
    
    items = get_items(metadata, vocabulary, args.mode)

    with logging_redirect_tqdm():
        for item in tqdm(items, desc="Creating IIIF assets"):
            success = write_manifest(item)
            if success:
                upload_folder_to_s3(item._base_path, mode=args.mode)
            else:
                continue
        upload_folder_to_s3(COLLECTIONS, mode=args.mode)
