import os
import subprocess
from math import *

from dagster import op, In, Out, DynamicOut, DynamicOutput, ExperimentalWarning, Nothing
import pandas as pd
import requests
from dotenv import load_dotenv
from IIIFpres import iiifpapi3
from IIIFpres.utilities import *
import jsonschema
from PIL import Image
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from urllib.parse import urlsplit, urlunsplit

import ops.helpers as h

BUCKET = os.environ["BUCKET"]

load_dotenv(override=True)

iiifpapi3.BASE_URL = BUCKET + "iiif/"
iiifpapi3.LANGUAGES = ["pt-BR", "en"]
Image.MAX_IMAGE_PIXELS = None


class Item:
    def __init__(self, id, row, mapping):
        self._id = id
        self._title = row["Title"]
        self._description = {
            "label_en": ["Description"],
            "label_pt": ["Descrição"],
            "value_en": [row["Description (English)"]]
            if row["Description (English)"]
            else [row["Description (Portuguese)"]],
            "value_pt": [row["Description (Portuguese)"]]
            if row["Description (Portuguese)"]
            else [row["Description (English)"]],
        }
        if row["Creator"] == "Autoria não identificada":
            self._creator = {
                "label_en": ["Creator"],
                "label_pt": ["Autor"],
                "value_en": ["Unknown"],
                "value_pt": [row["Creator"]],
            }
        else:
            self._creator = {
                "label_en": ["Creator"],
                "label_pt": ["Autor"],
                "value": [row["Creator"]],
            }
        self._date = {
            "label_en": ["Date"],
            "label_pt": ["Data"],
            "value": [str(row["Date"])],
        }
        self._depicts = (
            {
                "label_en": ["Depicts"],
                "label_pt": ["Retrata"],
                "value": [
                    '<a class="uri-value-link" target="_blank" href="{0}">{1}</a>'.format(
                        *depicts.split(" ", maxsplit=1)
                    )
                    for depicts in row["Depicts"].split("||")
                ],
            }
            if row["Depicts"]
            else None
        )
        self._type = self.map_wikidata(row["Type"], "Type", "Tipo", mapping)
        self._fabrication_method = self.map_wikidata(
            row["Fabrication Method"],
            "Fabrication Method",
            "Método de Fabricação",
            mapping,
        )
        self._materials = self.map_wikidata(
            row["Materials"], "Materials", "Materiais", mapping
        )
        self._width = {
            "label_en": ["Width (mm)"],
            "label_pt": ["Largura (mm)"],
            "value": [str(row["Width (mm)"])],
        }
        self._height = {
            "label_en": ["Height (mm)"],
            "label_pt": ["Altura (mm)"],
            "value": [str(row["Height (mm)"])],
        }
        self._metadata = [
            self._description,
            self._creator,
            self._date,
            self._depicts,
            self._type,
            self._materials,
            self._fabrication_method,
            self._width,
            self._height,
        ]
        self._attribution = row["Attribution"]
        self._license = row["License"]
        self._rights = row["Rights"]
        self._document_url = (
            row["Document URL"] if row["Document URL"] else "https://null"
        )
        self._provider = row["Provider"] if row["Provider"] else "imagineRio"
        self._wikidata_id = row["Wikidata ID"]
        self._smapshot_id = row["Smapshot ID"]
        self._collections = row["Collections"]
        self._is_tiled = h.file_exists(id, "info")
        self._base_path = "iiif/{0}".format(id)
        self._remote_img_path = self._base_path + "/full/max/0/default.jpg"
        self._local_img_path = self._base_path + ".jpg"
        self._info_path = self._base_path + "/info.json"
        self._manifest_path = self._base_path + "/manifest.json"

    def download_full_image(self):
        img_data = requests.get(BUCKET + self._remote_img_path).content
        with open(self._local_img_path, "wb") as handler:
            handler.write(img_data)

    def tile_image(self):
        command = [
            "java",
            "-jar",
            "resources/iiif-tiler.jar",
            self._local_img_path,
            "-tile_size",
            "256",
            "-version",
            "3",
        ]
        process = subprocess.Popen(command)
        process.communicate()
        os.remove(self._local_img_path)

    def map_wikidata(self, values_en, label_en, label_pt, mapping):
        en = []
        pt = []
        if not values_en:
            return None
        else:
            for value_en in values_en.split("||"):
                try:
                    url = "http://wikidata.org/wiki/{0}".format(
                        mapping.loc[value_en, "Wiki ID"]
                    )
                    value_pt = mapping.loc[value_en, "Label:pt"]
                    en.append(
                        '<a class="uri-value-link" target="_blank" href="{0}">{1}</a>'.format(
                            url, value_en
                        )
                    )
                    pt.append(
                        '<a class="uri-value-link" target="_blank" href="{0}">{1}</a>'.format(
                            url, value_pt
                        )
                    )
                except:
                    en.append(value_en)
                    pt.append(value_en)
                d = {"en": en, "pt": pt}

            return {
                "label_en": [label_en],
                "label_pt": [label_pt],
                "value_en": d["en"],
                "value_pt": d["pt"],
            }

    def get_metadata_entry(self, field):
        if field:
            if "value" in field:
                value = {"none": field["value"]} if any(field["value"]) else None
            else:
                value = (
                    {"en": field["value_en"], "pt-BR": field["value_pt"]}
                    if any(field["value_pt"])
                    else None
                )
            if value:
                return {
                    "label": {
                        "en": field["label_en"],
                        "pt-BR": field["label_pt"],
                    },
                    "value": value,
                }
        else:
            return None


@op(
    ins={
        "metadata": In(root_manager_key="metadata_root"),
        "mapping": In(root_manager_key="mapping_root"),
    },
    out=DynamicOut(),
)
def get_items(context, metadata, mapping):

    os.makedirs("iiif/collection", exist_ok=True)
    mapping.set_index("Label:en", inplace=True)
    metadata.set_index("Document ID", inplace=True)

    # filter ready to go items
    ims = (
        (metadata["Latitude"].notna())
        & (metadata["Document URL"].notna())
        & (metadata["Media URL"].notna())
        & (metadata["First Year"].notna())
        & (metadata["Last Year"].notna())
    )
    jstor = metadata["Provider"] != "Instituto Moreira Salles"
    metadata = metadata.loc[ims | jstor]

    # download existing collections
    for name in metadata["Collections"].dropna().str.split("\|\|").explode().unique():
        collection_path = "iiif/collection/{0}.json".format(name.lower())
        response = requests.get(BUCKET + collection_path)
        if response.status_code == 200:
            with open(collection_path, "w") as f:
                json.dump(response.json(), f, indent=4)

    # save current and try to load previous data
    if context.get_tag("mode") != "test":
        try:
            last_run = pd.read_pickle("data/input/last_run.pickle")
        except:
            last_run = None

        # pickle current data to compare against
        pd.to_pickle(metadata, "data/input/last_run.pickle")

        # process only new or modified items
        if last_run is not None:
            new_items = metadata.loc[~metadata.index.isin(last_run.index)]
            modified_items = last_run.compare(metadata.loc[last_run.index]).index
            to_process = metadata.loc[modified_items].append(new_items)
        else:
            to_process = metadata

    # test with dataframe's first item
    else:
        to_process = metadata.fillna("")  # pd.DataFrame(metadata.iloc[0]).T

    context.log.info(f"Processing {len(to_process)} items")

    for id, row in to_process.iterrows():
        yield DynamicOutput(
            value=Item(id, row, mapping),
            mapping_key=id.replace("-", "_"),
        )


@op
def tile_image(context, item):

    if not item._is_tiled or context.get_tag("mode") == "test":
        item.download_full_image()
        item.tile_image()

        # TO-DO remove when java tiler gets updated with id as CLI argument
        with open("iiif/{0}/info.json".format(item._id), "r+") as f:
            info = json.load(f)
            info["id"] = info["id"].replace("http://localhost:8887", BUCKET)
            f.seek(0)  # rewind
            json.dump(info, f, indent=4)
            f.truncate()
    else:
        context.log.info("Image is already tiled")
    return item


@op(
    out=Out(io_manager_key="iiif_manager"),
)
def write_manifest(context, item):

    id = item._id
    context.log.info("Processing: {0}".format(str(id)))

    # Get image sizes
    img_sizes = requests.get(BUCKET + item._info_path).json()["sizes"]
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
        manifest.add_metadata(entry=item.get_metadata_entry(field))

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

    if item._license.startswith("http"):
        manifest.set_rights(item._license)
    elif item._rights.startswith("http"):
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
                    manifest.json_dumps(
                        context="http://iiif.io/api/presentation/3/context.json"
                    )
                ),
                schema=json.load(schema),
            )
        except jsonschema.exceptions.ValidationError as e:
            print(e)
            context.log.warning("Manifest {0} is invalid".format(id))

    # Add to collection
    for name in item._collections.split("||"):  # ["Collections"]
        if name:
            try:
                collection = read_API3_json(
                    "iiif/collection/{0}.json".format(name.lower())
                )
                for object in collection.items:
                    if "{0}/manifest".format(id) in object.id:
                        collection.items.remove(object)
                collection.add_manifest_to_items(manifest)
            except FileNotFoundError:
                h.create_collection(name, manifest)
        else:
            continue

    manifest.json_save(
        item._manifest_path, context="http://iiif.io/api/presentation/3/context.json"
    )

    return item._base_path


@op(out=Out(io_manager_key="iiif_manager"))
def upload_collections(start):
    return "iiif/collection"
