import json
import os
import subprocess
import time
from json import JSONDecodeError
from subprocess import PIPE, Popen
from urllib.parse import urlsplit, urlunsplit

import jsonschema
import pandas as pd
from dotenv import load_dotenv
from helpers import create_collection, logger, session
from IIIFpres import iiifpapi3
from IIIFpres.utilities import *
from PIL import Image
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

load_dotenv(override=True)

BUCKET = os.environ["BUCKET"]
CLOUDFRONT = os.environ["CLOUDFRONT"]
COLLECTIONS = "iiif/collection/"
iiifpapi3.BASE_URL = CLOUDFRONT + "/iiif/"
iiifpapi3.LANGUAGES = ["pt-BR", "en"]
Image.MAX_IMAGE_PIXELS = None


class Item:
    def __init__(self, id, row, vocabulary):
        logger.debug(f"Parsing item {id}")
        self._id = id
        self._title = row["Title"]
        self._document_id = {
            "label_en": ["Document ID"],
            "label_pt": ["Identificador"],
            "value": [str(row["Document ID"])],
        }
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
        self._creator = self.map_wikidata(
            {"en": "Creator", "pt": "Criador"}, row["Creator"], vocabulary
        )
        self._date = {
            "label_en": ["Date"],
            "label_pt": ["Data"],
            "value": [str(row["Date"])],
        }
        self._depicts = self.map_wikidata(
            {"en": "Depicts", "pt": "Retrata"}, row["Depicts"], vocabulary
        )
        self._type = self.map_wikidata(
            {"en": "Type", "pt": "Tipo"}, row["Type"], vocabulary
        )
        self._fabrication_method = self.map_wikidata(
            {"en": "Fabrication Method", "pt": "Método de Fabricação"},
            row["Fabrication Method"],
            vocabulary,
        )
        self._material = self.map_wikidata(
            {"en": "Material", "pt": "Material"}, row["Material"], vocabulary
        )
        self._width = {
            "label_en": ["Width (mm)"],
            "label_pt": ["Largura (mm)"],
            "value": [str(row["Width"])],
        }
        self._height = {
            "label_en": ["Height (mm)"],
            "label_pt": ["Altura (mm)"],
            "value": [str(row["Height"])],
        }
        self._metadata = [
            self._document_id,
            self._description,
            self._creator,
            self._date,
            self._depicts,
            self._type,
            self._material,
            self._fabrication_method,
            self._width,
            self._height,
        ]
        self._attribution = row["Required Statement"]
        self._rights = row["Rights"]
        self._document_url = (
            row["Document URL"] if row["Document URL"] else "https://null"
        )
        self._provider = row["Provider"] if row["Provider"] else "imagineRio"
        self._wikidata_id = row["Wikidata ID"]
        self._smapshot_id = row["Smapshot ID"]
        self._collection = row["Collection"]
        self._remote_img_path = row["Media URL"]
        self._base_path = f"iiif/{id}"
        self._local_img_path = self._base_path + "/full/max/0/default.jpg"
        self._info_path = self._base_path + "/info.json"
        self._manifest_path = self._base_path + "/manifest.json"

    def download_full_image(self):
        start = time.time()
        response = session.get(self._remote_img_path)
        if response.status_code == 200:
            os.makedirs(os.path.dirname(self._local_img_path), exist_ok=True)
            with open(self._local_img_path, "wb") as handler:
                handler.write(response.content)
            end = time.time()
            tqdm.write(f"Image downloaded in {end - start} seconds")

    def tile_image(self):
        command = [
            "vips",
            "dzsave",
            "--layout",
            "iiif3",
            "--id",
            CLOUDFRONT + "/iiif",
            "--tile-size",
            "256",
            self._local_img_path,
            f"iiif/{self._id}",
        ]
        start = time.time()
        subprocess.run(command)
        end = time.time()
        tqdm.write(f"Tiling done in {end - start} seconds")
        factors = [16, 8, 4, 2, 1]
        for factor in factors:
            with Image.open(self._local_img_path) as im:
                full_width, full_height = im.size
                width, height = full_width // factor, full_height // factor
                if factor != 1:
                    path = f"{self._base_path}/full/{width},{height}/0"
                else:
                    path = f"{self._base_path}/full/max/0"

                os.makedirs(path, exist_ok=True)
                im.resize((width, height)).save(
                    f"{path}/default.jpg",
                    quality=95,
                    icc_profile=im.info.get("icc_profile"),
                )
            with open(self._info_path, "r+") as f:
                info = json.load(f)
                try:
                    info["sizes"].append({"width": width, "height": height})
                except KeyError:
                    info["sizes"] = [{"width": width, "height": height}]
                f.seek(0)  # rewind
                json.dump(info, f, indent=4)
                f.truncate()
        # os.remove(os.path.abspath(self._local_img_path))

    def map_wikidata(self, labels, value_en, vocabulary):
        if pd.isna(value_en):
            return None
        else:
            en = []
            pt = []
            values_en = value_en.split("|")
            for value_en in values_en:
                if value_en:
                    wikidata_id = vocabulary.loc[value_en, "Wikidata ID"]
                    value_pt = vocabulary.loc[value_en, "Label (pt)"]
                    url = "http://wikidata.org/wiki/{0}".format(wikidata_id) if pd.notna(wikidata_id) else None
                    if url and pd.notna(value_pt):
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
                    elif url and pd.isna(value_pt):
                        value = '<a class="uri-value-link" target="_blank" href="{0}">{1}</a>'.format(
                            url, value_en
                        )
                        en.append(value)
                        pt.append(value)
                    else:
                        en.append(value_en)
                        pt.append(value_en)
            return {
                "label_en": [labels["en"]],
                "label_pt": [labels["pt"]],
                "value_en": en,
                "value_pt": pt,
            }

    def get_metadata_entry(self, manifest, field):
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
                manifest.add_metadata(
                    entry={
                        "label": {
                            "en": field["label_en"],
                            "pt-BR": field["label_pt"],
                        },
                        "value": value,
                    }
                )
        else:
            return None

    def write_manifest(self):

        id = str(self._id)
        # logger.debug("Creating manifest {0}".format(str(id)))

        # Get image sizes
        try:
            img_sizes = session.get(BUCKET + self._info_path).json()["sizes"]
        except JSONDecodeError:
            # try:
            logger.debug("Attempting to download remote image")
            self.download_full_image()
            try:
                self.tile_image()
            except FileNotFoundError:
                tqdm.write("Image unavailable, skipping")
                return False
            except OSError:
                tqdm.write("Image corrupted, skipping")
                return False
            img_sizes = json.load(open(self._info_path))["sizes"]

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
        manifest.add_label("pt-BR", self._title)

        for field in self._metadata:
            self.get_metadata_entry(manifest, field)

        # Rights & Attribution
        if self._description["value_en"][0]:
            manifest.add_summary(language="en", text=self._description["value_en"][0])
            manifest.add_summary(
                language="pt-BR", text=self._description["value_pt"][0]
            )

        required_statement = manifest.add_requiredStatement()
        required_statement.add_label("Attribution", "en")
        required_statement.add_label("Atribuição", "pt-BR")

        if self._attribution:
            required_statement.add_value(
                self._attribution + ". Hosted by imagineRio.", "en"
            )
            required_statement.add_value(
                self._attribution.replace("Provided by", "Disponibilizado por")
                + ". Hospedado por imagineRio.",
                "pt-BR",
            )
        else:
            required_statement.add_value("Hosted by imagineRio.", "en")
            required_statement.add_value("Hospedado por imagineRio.", "pt-BR")

        if self._rights.startswith("http"):
            manifest.set_rights(self._rights)
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
            item_homepage.set_id(objid=self._document_url)
        except:
            url_parts = list(urlsplit(self._document_url))
            url_parts[2:5] = ["", "", ""]
            new_url = urlunsplit(url_parts)
            item_homepage.set_id(objid=new_url)

        # homepage_label = item["Provider"] if item["Provider"] else "imagineRio"
        item_homepage.add_label(language="none", text=self._provider)
        item_homepage.set_type("Text")
        item_homepage.set_format("text/html")
        manifest.add_homepage(item_homepage)

        # See Also
        imaginerio_seealso = iiifpapi3.seeAlso()
        imaginerio_seealso.set_id(objid="https://www.imaginerio.org/map#{0}".format(id))
        imaginerio_seealso.add_label(language="none", text="imagineRio")
        imaginerio_seealso.set_type("Text")
        manifest.add_seeAlso(imaginerio_seealso)

        if self._wikidata_id:
            wikidata_seealso = iiifpapi3.seeAlso()
            wikidata_seealso.set_id(
                objid="https://www.wikidata.org/wiki/{0}".format(self._wikidata_id)
            )
            wikidata_seealso.add_label(language="none", text="Wikidata")
            wikidata_seealso.set_type("Text")
            # wikidata_seealso.set_format("text/html")
            manifest.add_seeAlso(wikidata_seealso)

        if self._smapshot_id:  # ["Smapshot ID"]
            smapshot_seealso = iiifpapi3.seeAlso()
            smapshot_seealso.set_id(
                objid="https://smapshot.heig-vd.ch/visit/{0}".format(
                    self._smapshot_id
                )  # ["Smapshot ID"]
            )
            smapshot_seealso.add_label(language="none", text="Smapshot")
            smapshot_seealso.set_type("Text")
            # smapshot_seealso.set_format("text/html")
            manifest.add_seeAlso(smapshot_seealso)

        if self._provider == "Instituto Moreira Salles":
            download_seealso = iiifpapi3.seeAlso()
            download_seealso.set_id(
                objid="{0}/{1}".format(CLOUDFRONT, self._local_img_path)
            )
            download_seealso.add_label("en", "Download image")
            download_seealso.add_label("pt-BR", "Baixar imagem")
            download_seealso.set_type("Text")
            manifest.add_seeAlso(download_seealso)

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
        for collection_name in self._collection.split("|"):  # ["Collections"]
            if collection_name:
                try:
                    collection = read_API3_json(
                        COLLECTIONS + collection_name.lower() + ".json"
                    )
                    for current_item in collection.items:
                        if f"/{id}/" in current_item.id:
                            # logger.debug(f"Updating manifest {id} in {collection_name}")
                            collection.items.remove(current_item)
                    collection.add_manifest_to_items(manifest)
                    collection.orjson_save(f"{COLLECTIONS}/{collection_name}.json")
                except FileNotFoundError:
                    logger.debug("Collection doesn't exist, creating...")
                    create_collection(collection_name, manifest)
            else:
                continue

        os.makedirs(self._base_path, exist_ok=True)

        manifest.orjson_save(
            self._manifest_path,
            context="http://iiif.io/api/presentation/3/context.json",
        )
        # logger.debug(f"Saved manifest at {self._manifest_path}")

        return True
