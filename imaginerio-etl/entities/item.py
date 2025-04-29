import json
import logging
import os
import subprocess
from json import JSONDecodeError
import pandas as pd

from iiif_prezi3 import KeyValueString, Manifest, Canvas
from PIL import Image

logging.getLogger("PIL").setLevel(logging.WARNING)

from ..config import (
    CLOUDFRONT,
    RIGHTS,
    MetadataFields as MF,
    VocabularyFields as VF,
    Languages as L,
    IIIFConfig as IC,
)
from ..utils.helpers import session, upload_folder_to_s3
from ..utils.logger import CustomFormatter as cf
from ..utils.logger import logger

Image.MAX_IMAGE_PIXELS = None


class Item:
    def __init__(self, id, row, vocabulary):
        self._id = id
        self._vocabulary = vocabulary
        self._title = row[MF.TITLE]
        self._description = (
            {
                L.EN: [row.get(MF.DESC_EN) or row.get(MF.DESC_PT)],
                L.PT_BR: [row.get(MF.DESC_PT) or row.get(MF.DESC_EN)],
            }
            if row.get(MF.DESC_EN) or row.get(MF.DESC_PT)
            else None
        )
        self._metadata = list(
            filter(
                None,
                [
                    (
                        KeyValueString(
                            label={L.EN: ["Document ID"], L.PT_BR: ["Identificador"]},
                            value=str(row.get(MF.DOCUMENT_ID)),
                        )
                        if row.get(MF.DOCUMENT_ID)
                        else None
                    ),
                    self.map_wikidata(
                        {L.EN: ["Creator"], L.PT_BR: ["Criador"]},
                        row.get(MF.CREATOR),
                    ),
                    (
                        KeyValueString(
                            label={L.EN: ["Date"], L.PT_BR: ["Data"]},
                            value=str(row.get(MF.DATE)),
                        )
                        if row.get(MF.DATE)
                        else None
                    ),
                    self.map_wikidata(
                        {L.EN: ["Depicts"], L.PT_BR: ["Retrata"]},
                        row.get(MF.DEPICTS),
                    ),
                    self.map_wikidata(
                        {L.EN: ["Type"], L.PT_BR: ["Tipo"]}, row.get(MF.TYPE)
                    ),
                    self.map_wikidata(
                        {L.EN: ["Material"], L.PT_BR: ["Material"]},
                        row.get(MF.MATERIAL),
                    ),
                    self.map_wikidata(
                        {
                            L.EN: ["Fabrication Method"],
                            L.PT_BR: ["Método de Fabricação"],
                        },
                        row.get(MF.FABRICATION_METHOD),
                    ),
                    (
                        KeyValueString(
                            label={L.EN: ["Width"], L.PT_BR: ["Largura"]},
                            value=self._format_dimension(row.get(MF.WIDTH)),
                        )
                        if row.get(MF.WIDTH)
                        else None
                    ),
                    (
                        KeyValueString(
                            label={L.EN: ["Height"], L.PT_BR: ["Altura"]},
                            value=self._format_dimension(row.get(MF.HEIGHT)),
                        )
                        if row.get(MF.HEIGHT)
                        else None
                    ),
                ],
            )
        )

        if row.get(MF.REQUIRED_STATEMENT):
            attribution_en = (
                row.get(MF.REQUIRED_STATEMENT) + f". Hosted by {IC.PROVIDER_LABEL}."
            )
            attribution_pt = (
                row.get(MF.REQUIRED_STATEMENT).replace(
                    "Provided by", "Disponibilizado por"
                )
                + f". Hospedado por {IC.PROVIDER_LABEL}."
            )
        else:
            attribution_en = f"Hosted by {IC.PROVIDER_LABEL}."
            attribution_pt = f"Hospedado por {IC.PROVIDER_LABEL}."
        self._attribution = KeyValueString(
            label={L.EN: ["Attribution"], L.PT_BR: ["Atribuição"]},
            value={L.EN: [attribution_en], L.PT_BR: [attribution_pt]},
        )
        self._rights = RIGHTS.get(row[MF.RIGHTS], IC.DEFAULT_RIGHTS)
        self._transcription = row.get(MF.TRANSCRIPTION)
        self._transcription_en = row.get(MF.TRANSCRIPTION_EN)
        self._transcription_pt = row.get(MF.TRANSCRIPTION_PT)
        self._document_url = row.get(MF.DOCUMENT_URL)
        self._provider = row.get(MF.PROVIDER) or IC.PROVIDER_LABEL
        self._wikidata_id = row.get(MF.WIKIDATA_ID)
        self._smapshot_id = row.get(MF.SMAPSHOT_ID)
        self._collection = row.get(MF.COLLECTION)
        self._jstor_img_path = row.get(MF.MEDIA_URL)
        self._base_path = f"{CLOUDFRONT}/{id}"
        self._local_img_path = f"iiif/{id}/full/max/0/default.jpg"
        self._img_path = f"{self._base_path}/full/max/0/default.jpg"
        self._local_info_path = f"iiif/{id}/info.json"
        self._info_path = f"{self._base_path}/info.json"
        self._manifest_path = f"{self._base_path}/manifest.json"

    def get_collections(self):
        if self._collection:
            return self._collection.split("|")
        else:
            logger.warning(f"Item {self._id} isn't associated with any collections")
            return []

    def get_sizes(self):
        try:
            img_sizes = session.get(self._info_path).json()["sizes"]
            return img_sizes
        except JSONDecodeError:
            return None

    def _format_dimension(self, value: str | None) -> str | None:
        """Convert dimension from millimeters to centimeters and format with unit.

        Args:
            value: Dimension value in millimeters as string, or None

        Returns:
            Formatted string with value in centimeters and unit, or None if input is None
        """
        if not value:
            return None

        try:
            # Convert from string to float, divide by 10 to convert mm to cm
            cm_value = float(value) / 10
            # Format with 1 decimal place if needed, remove trailing zeros
            formatted = f"{cm_value:.1f}".rstrip("0").rstrip(".")
            return f"{formatted} cm"
        except (ValueError, TypeError):
            logger.warning(
                f"{cf.YELLOW}Invalid dimension value: {value}, using as is{cf.RESET}"
            )
            return str(value)

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
            CLOUDFRONT,
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

    def _format_wikidata_term(self, value_en: str) -> tuple[str, str] | None:
        """
        Processes a single English term, looking up Wikidata info and translation.

        Args:
            value_en: The single English term.

        Returns:
            A tuple (formatted_en, formatted_pt) or None if the term is invalid
            or not found in the vocabulary. Formatted strings include HTML links
            if a Wikidata ID is found.
        """
        if value_en not in self._vocabulary:
            logger.warning(
                f"{cf.YELLOW}Value '{value_en}' not found in vocabulary, skipping{cf.RESET}"
            )
            return None

        term_data = self._vocabulary[value_en]
        wikidata_id = term_data.get(VF.WIKIDATA_ID)
        value_pt = term_data.get(VF.LABEL_PT)
        value_pt = value_pt if pd.notna(value_pt) else value_en

        if not wikidata_id:
            logger.warning(
                f"{cf.YELLOW}No Wikidata ID found for '{value_en}', will display text instead of link{cf.RESET}"
            )
            return value_en, value_pt
        else:
            link_template = '<a class="uri-value-link" target="_blank" href="https://wikidata.org/wiki/{wikidata_id}">{text}</a>'
            formatted_en = link_template.format(wikidata_id=wikidata_id, text=value_en)
            formatted_pt = link_template.format(wikidata_id=wikidata_id, text=value_pt)
            return formatted_en, formatted_pt

    def map_wikidata(self, label, values_en):
        if not values_en:
            return None

        processed_en = []
        processed_pt = []

        for value_en in values_en.split("|"):
            formatted_pair = self._format_wikidata_term(value_en)
            if formatted_pair:
                processed_en.append(formatted_pair[0])
                processed_pt.append(formatted_pair[1])

        if not processed_en:
            return None

        final_value = {L.EN: processed_en, L.PT_BR: processed_pt}
        return KeyValueString(label=label, value=final_value)

    def _create_thumbnail(self, sizes: list[dict]) -> dict:
        """Create thumbnail for the manifest.

        Args:
            sizes: List of available image sizes.

        Returns:
            Dictionary with thumbnail configuration.
        """
        # Pick size closer to 200px (long edge) for thumbnail
        thumb_width, thumb_height = min(
            sizes, key=lambda x: abs(max(x.values()) - 200)
        ).values()

        return {
            "id": f"{self._base_path}/full/{thumb_width},{thumb_height}/0/default.jpg",
            "type": "Image",
            "format": IC.IMAGE_FORMAT,
            "height": thumb_height,
            "width": thumb_width,
        }

    def _create_homepage(self) -> dict:
        """Create homepage for the manifest."""
        url = self._document_url
        if not url and self._provider in self._vocabulary:
            url = self._vocabulary[self._provider].get(VF.URL)

        return {
            "id": url,
            "label": {L.NONE: [self._provider]},
            "type": "Text",
            "format": IC.TEXT_FORMAT,
        }

    def _create_logo(self) -> dict:
        """Create logo for the manifest."""
        return {
            "id": IC.LOGO_URL,
            "type": "Image",
            "format": "image/png",
            "height": IC.LOGO_HEIGHT,
            "width": IC.LOGO_WIDTH,
        }

    def _create_provider(self, logo: dict) -> dict:
        """Create provider for the manifest."""
        return {
            "id": IC.PROVIDER_ID,
            "type": "Agent",
            "label": {L.NONE: [IC.PROVIDER_LABEL]},
            "logo": logo,
        }

    def _create_canvas(self, sizes: list[dict]) -> Canvas:
        """Create canvas and annotation page for the manifest.

        Returns:
            Tuple of (canvas, annotation_page)
        """
        canvas = Canvas(
            label={L.NONE: [self._id]},
            id=f"{self._base_path}/canvas/1",
            height=sizes[-1]["height"],
            width=sizes[-1]["width"],
        )

        anno_page = canvas.add_image(
            image_url=self._img_path,
            anno_page_id=f"{self._base_path}/annotation-page/1",
            anno_id=f"{self._base_path}/annotation/1",
            format=IC.IMAGE_FORMAT,
            height=canvas.height,
            width=canvas.width,
        )

        anno_page.items[0].body.make_service(
            id=self._base_path, type="ImageService3", profile=IC.IMAGE_SERVICE_PROFILE
        )

        canvas = self._create_annotations(canvas)

        return canvas

    def _create_annotations(self, canvas: Canvas):
        """Create annotations for the canvas"""
        if self._transcription:
            canvas.make_annotation(
                anno_page_id=f"{self._base_path}/annotation-page/2",
                id=f"{self._base_path}/annotation/2",
                motivation="supplementing",
                body={
                    "type": "TextualBody",
                    "value": self._transcription,
                    "format": "text/plain",
                },
                target=canvas.id,
            )
        if self._transcription_en or self._transcription_pt:
            canvas.make_annotation(
                anno_page_id=f"{self._base_path}/annotation-page/2",
                id=f"{self._base_path}/annotation/3",
                motivation="supplementing",
                body={
                    "type": "Choice",
                    "items": [
                        {
                            "type": "TextualBody",
                            "value": self._transcription_en,
                            "language": L.EN,
                            "format": "text/plain",
                        },
                        {
                            "type": "TextualBody",
                            "value": self._transcription_pt,
                            "language": L.PT_BR,
                            "format": "text/plain",
                        },
                    ],
                },
                target=canvas.id,
            )

        return canvas

    def _create_see_also(self) -> list[dict]:
        """Create seeAlso links for the manifest."""
        see_also = [
            {
                "id": "https://www.imaginerio.org/map#35103808",
                "type": "Text",
                "format": "text/html",
                "label": {L.NONE: ["imagineRio"]},
            }
        ]

        if self._wikidata_id:
            see_also.append(
                {
                    "id": f"https://www.wikidata.org/wiki/{self._wikidata_id}",
                    "type": "Text",
                    "format": "text/html",
                    "label": {L.NONE: ["Wikidata"]},
                }
            )

        if self._smapshot_id:
            see_also.append(
                {
                    "id": f"https://smapshot.heig-vd.ch/visit/{self._smapshot_id}",
                    "type": "Text",
                    "format": "text/html",
                    "label": {L.NONE: ["Smapshot"]},
                }
            )

        if self._provider == "Instituto Moreira Salles":
            see_also.append(
                {
                    "id": self._img_path,
                    "type": "Text",
                    "format": "text/html",
                    "label": {L.EN: ["Download image"], L.PT_BR: ["Baixar imagem"]},
                }
            )

        return see_also

    def create_manifest(self, sizes: list[dict]) -> Manifest | None:
        """Create IIIF manifest for the item.

        Args:
            sizes: List of available image sizes.

        Returns:
            IIIF Manifest object or None if no sizes available.
        """
        if not sizes:
            return None

        # Create manifest components
        thumbnail = self._create_thumbnail(sizes)
        homepage = self._create_homepage()
        logo = self._create_logo()
        provider = self._create_provider(logo)
        see_also = self._create_see_also()

        # Create the manifest
        manifest = Manifest(
            id=self._manifest_path,
            label=self._title,
            summary=self._description,
            metadata=self._metadata,
            requiredStatement=self._attribution,
            rights=self._rights,
            homepage=homepage,
            thumbnail=thumbnail,
            provider=provider,
            seeAlso=see_also,
        )

        # Add canvas and annotation page
        canvas = self._create_canvas(sizes)
        manifest.add_item(canvas)

        return manifest
