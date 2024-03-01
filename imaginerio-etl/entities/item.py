import json
import logging
import os
import subprocess
from json import JSONDecodeError

from iiif_prezi3 import KeyValueString, Manifest
from PIL import Image

logging.getLogger("PIL").setLevel(logging.WARNING)

from ..config import *
from ..utils.helpers import session, upload_folder_to_s3
from ..utils.logger import CustomFormatter as cf
from ..utils.logger import logger

Image.MAX_IMAGE_PIXELS = None


class Item:
    def __init__(self, id, row, vocabulary):
        self._id = id
        self._vocabulary = vocabulary
        self._title = row["Title"]
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
        self._metadata = list(
            filter(
                None,
                [
                    (
                        KeyValueString(
                            label={"en": ["Document ID"], "pt-BR": ["Identificador"]},
                            value=str(row.get("Document ID")),
                        )
                        if row.get("Document ID")
                        else None
                    ),
                    self.map_wikidata(
                        {"en": ["Creator"], "pt-BR": ["Criador"]},
                        row.get("Creator"),
                    ),
                    (
                        KeyValueString(
                            label={"en": ["Date"], "pt-BR": ["Data"]},
                            value=str(row.get("Date")),
                        )
                        if row.get("Date")
                        else None
                    ),
                    self.map_wikidata(
                        {"en": ["Depicts"], "pt-BR": ["Retrata"]},
                        row.get("Depicts"),
                    ),
                    self.map_wikidata(
                        {"en": ["Type"], "pt-BR": ["Tipo"]}, row.get("Type")
                    ),
                    self.map_wikidata(
                        {"en": ["Material"], "pt-BR": ["Material"]},
                        row.get("Material"),
                    ),
                    self.map_wikidata(
                        {
                            "en": ["Fabrication Method"],
                            "pt-BR": ["Método de Fabricação"],
                        },
                        row.get("Fabrication Method"),
                    ),
                    (
                        KeyValueString(
                            label={"en": ["Width (mm)"], "pt-BR": ["Largura (mm)"]},
                            value=str(row.get("Width")),
                        )
                        if row.get("Width")
                        else None
                    ),
                    (
                        KeyValueString(
                            label={"en": ["Height (mm)"], "pt-BR": ["Altura (mm)"]},
                            value=str(row.get("Height")),
                        )
                        if row.get("Height")
                        else None
                    ),
                ],
            )
        )

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
        self._rights = RIGHTS.get(
            row["Rights"], "http://rightsstatements.org/vocab/CNE/1.0/"
        )
        self._document_url = row.get("Document URL")
        self._provider = row.get("Provider") or "imagineRio"
        self._wikidata_id = row.get("Wikidata ID")
        self._smapshot_id = row.get("Smapshot ID")
        self._collection = row.get("Collection")
        self._jstor_img_path = row.get("Media URL")
        self._base_path = f"{CLOUDFRONT}/{id}"
        self._local_img_path = f"iiif/{id}/full/max/0/default.jpg"
        self._img_path = f"{self._base_path}/full/max/0/default.jpg"
        self._local_info_path = f"iiif/{id}/info.json"
        self._info_path = f"{self._base_path}/info.json"
        self._manifest_path = f"{self._base_path}/manifest.json"

    def get_collections(self):
        if self._collection:
            return self._collection.lower().split("|")
        else:
            logger.warning(f"Item {self._id} isn't associated with any collections")
            return []

    def get_sizes(self):
        try:
            img_sizes = session.get(self._info_path).json()["sizes"]
            return img_sizes
        except JSONDecodeError:
            return None

    def download_image(self):
        logger.info(f"{cf.BLUE}Downloading image...{cf.RESET}")
        response = session.get(self._jstor_img_path)
        if response.status_code == 200:
            os.makedirs(os.path.dirname(self._local_img_path), exist_ok=True)
            with open(self._local_img_path, "wb") as handler:
                handler.write(response.content)
        else:
            logger.error(
                f"{cf.RED}Failed to download image {self._id} at {self._jstor_img_path}{cf.RESET}"
            )
            raise Exception(IOError)

    def tile_image(self):
        self.download_image()
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
        logger.info(f"{cf.BLUE}Tiling image...")
        subprocess.run(command)
        sizes = self.create_derivatives([16, 8, 4, 2, 1])
        upload_folder_to_s3(f"iiif/{self._id}")
        return sizes
        # os.remove(os.path.abspath(self._local_img_path))

    def create_derivatives(self, factors):
        logger.info(f"{cf.BLUE}Creating derivatives...")
        sizes = []
        for factor in factors:
            with Image.open(self._local_img_path) as im:
                full_width, full_height = im.size
                width, height = full_width // factor, full_height // factor
                sizes.append({"width": width, "height": height})
                if factor != 1:
                    path = f"iiif/{self._id}/full/{width},{height}/0"
                    os.makedirs(path, exist_ok=True)
                    im.resize((width, height)).save(
                        f"{path}/default.jpg",
                        quality=95,
                        icc_profile=im.info.get("icc_profile"),
                    )
                # else:
                #     path = f"iiif/{self._id}/full/max/0"
            with open(self._local_info_path, "r+") as f:
                info = json.load(f)
                info["sizes"] = sizes
                f.seek(0)  # rewind
                json.dump(info, f, indent=4)
                f.truncate()
        return sizes

    def map_wikidata(self, label, values_en):
        if not values_en:
            return None

        value = {"en": [], "pt-BR": []}
        for value_en in values_en.split("|"):
            wikidata_id = self._vocabulary[value_en].get("Wikidata ID")
            value_pt = self._vocabulary[value_en].get("Label (pt)") or value_en

        if not wikidata_id:
            logger.warning(
                f"{cf.YELLOW}No Wikidata ID found for {value_en}, will display text instead of link{cf.RESET}"
            )
            value["en"].append(value_en)
            value["pt-BR"].append(value_pt)
        else:
            value["en"].append(
                f'<a class="uri-value-link" target="_blank" href="https://wikidata.org/wiki/{wikidata_id}">{value_en}</a>'
            )
            value["pt-BR"].append(
                f'<a class="uri-value-link" target="_blank" href="https://wikidata.org/wiki/{wikidata_id}">{value_pt}</a>'
            )
        return KeyValueString(label=label, value=value)

    def create_manifest(self, sizes):
        if not sizes:
            return None
        # Pick size closer to 600px (long edge) for thumbnail
        thumb_width, thumb_height = min(
            sizes, key=lambda x: abs(min(x.values()) - 600)
        ).values()

        thumbnail = {
            "id": f"{self._base_path}/full/{thumb_width},{thumb_height}/0/default.jpg",
            "type": "Image",
            "format": "image/jpeg",
            "height": thumb_height,
            "width": thumb_width,
        }

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
            thumbnail=thumbnail,
            provider={
                "id": "https://imaginerio.org/",
                "type": "Agent",
                "label": {"none": ["imagineRio"]},
                "homepage": homepage,
                "logo": logo,
            },
        )

        canvas = manifest.make_canvas(
            label={"none": [self._id]},
            id=f"{self._base_path}/canvas/1",
            height=sizes[-1]["height"],
            width=sizes[-1]["width"],
        )
        anno_page = canvas.add_image(
            image_url=self._img_path,
            anno_page_id=f"{self._base_path}/annotation-page/1",
            anno_id=f"{self._base_path}/annotation/1",
            format="image/png",
            height=canvas.height,
            width=canvas.width,
        )
        anno_page.items[0].body.make_service(
            id=self._base_path, type="ImageService3", profile="level0"
        )

        # manifest.make_canvas_from_iiif(
        #     self._info_path,
        #     id=f"{self._base_path}/canvas/1",
        #     anno_page_id=f"{self._base_path}/annopage/1",
        #     anno_id=f"{self._base_path}/anno/1",
        # )

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
