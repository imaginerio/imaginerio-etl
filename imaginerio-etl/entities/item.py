import json
import os
import subprocess
import time
from json import JSONDecodeError

import pandas as pd
from iiif_prezi3 import KeyValueString, Manifest
from PIL import Image
from tqdm import tqdm

from ..config import *
from ..utils.helpers import logger, session

Image.MAX_IMAGE_PIXELS = None


class Item:
    def __init__(self, id, row, vocabulary):
        logger.info(f"Parsing item {id}")
        self._id = id
        self._title = row["Title"]
        self._document_id = KeyValueString(
            label={"en": ["Document ID"], "pt-BR": ["Identificador"]},
            value=str(row.get("Document ID")),
        )
        self._description = (
            {
                "en": [
                    row.get("Description (English)")
                    or row.get("Description (Portuguese)")
                ],
                "pt-BR": [
                    row.get("Description (Portuguese)")
                    or row.get("Description (English)")
                ],
            }
            if row.get("Description (English)") or row.get("Description (Portuguese)")
            else None
        )
        self._date = (
            KeyValueString(
                label={"en": ["Date"], "pt-BR": ["Data"]}, value=str(row.get("Date"))
            )
            if row.get("Date")
            else None
        )
        self._width = (
            KeyValueString(
                label={"en": ["Width (mm)"], "pt-BR": ["Largura (mm)"]},
                value=str(row.get("Width")),
            )
            if row.get("Width")
            else None
        )
        self._height = (
            KeyValueString(
                label={"en": ["Height (mm)"], "pt-BR": ["Altura (mm)"]},
                value=str(row.get("Height")),
            )
            if row.get("Height")
            else None
        )
        self._creator = self.map_wikidata(
            {"en": ["Creator"], "pt-BR": ["Criador"]}, row["Creator"], vocabulary
        )
        self._depicts = (
            self.map_wikidata(
                {"en": ["Depicts"], "pt-BR": ["Retrata"]}, row["Depicts"], vocabulary
            )
            if row.get("Depicts")
            else None
        )
        self._type = self.map_wikidata(
            {"en": ["Type"], "pt-BR": ["Tipo"]}, row["Type"], vocabulary
        )
        self._fabrication_method = self.map_wikidata(
            {"en": ["Fabrication Method"], "pt-BR": ["Método de Fabricação"]},
            row["Fabrication Method"],
            vocabulary,
        )
        self._material = self.map_wikidata(
            {"en": ["Material"], "pt-BR": ["Material"]}, row["Material"], vocabulary
        )
        self._metadata = [
            field
            for field in [
                self._document_id,
                self._creator,
                self._date,
                self._depicts,
                self._type,
                self._material,
                self._fabrication_method,
                self._width,
                self._height,
            ]
            if field
        ]
        # logger.debug(f"{self._id} ### {"\n".join([str(field) for field in self._metadata])}")
        if row.get("Required Statement"):
            attribution_en = row.get("Required Statement") + ". Hosted by imagineRio."
            attribution_pt = (
                row.get("Required Statement").replace(
                    "Provided by", "Disponibilizado por"
                )
                + ". Hospedado por imagineRio."
            )
        else:
            attribution_en = "Hosted by imagineRio."
            attribution_pt = "Hospedado por imagineRio."
        self._attribution = KeyValueString(
            label={"en": ["Attribution"], "pt-BR": ["Atribuição"]},
            value={"en": [attribution_en], "pt-BR": [attribution_pt]},
        )
        self._rights = row.get("Rights")
        self._document_url = row.get("Document URL")
        self._provider = row.get("Provider") or "imagineRio"
        self._wikidata_id = row.get("Wikidata ID")
        self._smapshot_id = row.get("Smapshot ID")
        self._collection = row.get("Collection")
        self._remote_img_path = row.get("Media URL")
        self._base_path = f"{CLOUDFRONT}/{id}"
        self._local_img_path = f"iiif/{id}/full/max/0/default.jpg"
        self._img_path = f"{self._base_path}/full/max/0/default.jpg"
        self._local_info_path = f"iiif/{id}/info.json"
        self._info_path = f"{self._base_path}/info.json"
        self._manifest_path = f"{self._base_path}/manifest.json"

    def get_collections(self):
        return self._collection.split("|")

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
            f"{CLOUDFRONT}/iiif",
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
            with open(self._local_info_path, "r+") as f:
                info = json.load(f)
                try:
                    info["sizes"].append({"width": width, "height": height})
                except KeyError:
                    info["sizes"] = [{"width": width, "height": height}]
                f.seek(0)  # rewind
                json.dump(info, f, indent=4)
                f.truncate()
        # os.remove(os.path.abspath(self._local_img_path))

    def map_wikidata(self, label, values, vocabulary):
        # Initialize results
        result = {"en": [], "pt-BR": []}

        for value in values.split("|"):
            if not value:
                continue
            # Find the row with the matching label
            try:
                row = vocabulary[vocabulary["Label (en)"] == value].iloc[0]
            except KeyError:
                row = vocabulary[vocabulary["Label (pt)"] == value].iloc[0]

            if pd.isna(row["Label (en)"]) or pd.isna(row["Label (pt)"]):
                logger.warning(
                    f"No translation found for {value}. Will use available language."
                )
                en_label = pt_label = value
            else:
                en_label = row["Label (en)"]
                pt_label = row["Label (pt)"]

            # Check if Wikidata Id is present
            if pd.isna(row["Wikidata ID"]):
                logger.warning(
                    f"No Wikidata ID found for {value}. Will display labels instead of link"
                )
                # Return only the values if identifier is missing
                result["en"].append(en_label)
                result["pt-BR"].append(pt_label)
            else:
                # Build URLs
                wikidata_id = row["Wikidata ID"]
                result["en"].append(
                    f'<a class="uri-value-link" target="_blank" href="https://wikidata.org/wiki/{wikidata_id}">{en_label}</a>'
                )
                result["pt-BR"].append(
                    f'<a class="uri-value-link" target="_blank" href="https://wikidata.org/wiki/{wikidata_id}">{pt_label}</a>'
                )

        return KeyValueString(
            label=label,
            value=result,
        )

    def create_manifest(self):
        # Get image sizes
        try:
            logger.debug("Attempting to get image sizes")
            img_sizes = session.get(self._info_path).json()["sizes"]
        except JSONDecodeError:
            return False
            # try:
            # logger.debug("Attempting to download remote image")
            # self.download_full_image()
            # try:
            # self.tile_image()
            # except FileNotFoundError:
            #     logger.warning(
            #         f"Image at {self._remote_img_path} unavailable, skipping"
            #     )
            #     return False
            # except OSError:
            #     logger.warning(f"Image {self._id} corrupted, skipping")
            #     return False
            # img_sizes = json.load(open(self._local_info_path))["sizes"]

        # Pick size closer to 600px (long edge) for thumbnail
        thumb_width, thumb_height = min(
            img_sizes, key=lambda x: abs(min(x.values()) - 600)
        ).values()

        homepage = {
            "id": self._document_url,
            "label": {"none": [self._provider]},
            "type": "Text",
            "format": "text/html",
        }

        logo = {
            "id": "https://aws1.discourse-cdn.com/free1/uploads/imaginerio/original/1X/8c4f71106b4c8191ffdcafb4edeedb6f6f58b482.png",
            "type": "Image",
            "format": "image/png",
            "height": 164,
            "width": 708,
        }

        logger.debug(f"Creating manifest {self._id}")
        manifest = Manifest(
            id=self._manifest_path,
            label=self._title,
            summary=self._description,
            metadata=self._metadata,
            requiredStatement=self._attribution,
            rights=(
                self._rights
                if self._rights.startswith("http")
                else "http://rightsstatements.org/vocab/CNE/1.0/"
            ),
            homepage=homepage,
            thumbnail={
                "id": f"{self._base_path}/full/{thumb_width},{thumb_height}/0/default.jpg",
                "type": "Image",
                "format": "image/jpeg",
                "height": thumb_height,
                "width": thumb_width,
            },
            provider={
                "id": "https://imaginerio.org/",
                "type": "Agent",
                "label": {"none": ["imagineRio"]},
                "homepage": homepage,
                "logo": logo,
            },
        )

        manifest.make_canvas_from_iiif(
            self._info_path,
            id=f"{self._base_path}/canvas/1",
            anno_page_id=f"{self._base_path}/annopage/1",
            anno_id=f"{self._base_path}/anno/1",
        )

        seeAlso = [
            {
                "id": "https://www.imaginerio.org/map#35103808",
                "type": "Text",
                "format": "text/html",
                "label": {"none": ["imagineRio"]},
            },
        ]
        if self._wikidata_id:
            seeAlso.append(
                {
                    "id": "https://www.wikidata.org/wiki/{0}".format(self._wikidata_id),
                    "type": "Text",
                    "format": "text/html",
                    "label": {"none": ["Wikidata"]},
                }
            )
        if self._smapshot_id:
            seeAlso.append(
                {
                    "id": "https://smapshot.heig-vd.ch/visit/{0}".format(
                        self._smapshot_id
                    ),
                    "type": "Text",
                    "format": "text/html",
                    "label": {"none": ["Smapshot"]},
                }
            )
        if self._provider == "Instituto Moreira Salles":
            seeAlso.append(
                {
                    "id": self._img_path,
                    "type": "Text",
                    "format": "text/html",
                    "label": {"en": ["Download image"], "pt-BR": ["Baixar imagem"]},
                }
            )
        manifest.seeAlso = seeAlso

        return manifest
