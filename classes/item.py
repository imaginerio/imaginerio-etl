import json
import os
import subprocess

import pandas as pd
from dotenv import load_dotenv
from scripts.helpers import logger, session

load_dotenv(override=True)


class Item:
    def __init__(self, id, row, vocabulary):
        logger.debug(f"Parsing item {id}")
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
        self._creator = self.map_wikidata(
                {"en":"Creator","pt":"Criador"},
                row["Creator"], 
                vocabulary
            )
        self._date = {
            "label_en": ["Date"],
            "label_pt": ["Data"],
            "value": [str(row["Date"])],
        }
        self._depicts = self.map_wikidata(
            {"en":"Depicts","pt":"Retrata"},
            row["Depicts"],
            vocabulary
        )
        self._type = self.map_wikidata(
            {"en":"Type","pt":"Tipo"}, 
            row["Type"], 
            vocabulary
        )
        self._fabrication_method = self.map_wikidata(
            {"en":"Fabrication Method", "pt":"Método de Fabricação"},
            row["Fabrication Method"],
            vocabulary,
        )
        self._material = self.map_wikidata(
            {"en":"Material", "pt":"Material"}, 
            row["Material"], 
            vocabulary
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
        self._collections = row["Collections"]
        self._remote_img_path = row["Media URL"]
        self._base_path = f"iiif/{id}"
        self._local_img_path = self._base_path + ".jpg"
        self._info_path = self._base_path + "/info.json"
        self._manifest_path = self._base_path + "/manifest.json"

    def download_full_image(self):
        response = session.get(self._remote_img_path)
        if response.status_code == 200:
            print(self._local_img_path)
            with open(self._local_img_path, "wb") as handler:
                handler.write(response.content)

    def tile_image(self):
        command = [
            "java",
            "-jar",
            "scripts/iiif-tiler.jar",
            self._local_img_path,
            "-tile_size",
            "256",
            "-version",
            "3",
        ]
        process = subprocess.Popen(command)
        process.communicate()
        os.makedirs(self._base_path, exist_ok=True)
        with open(self._info_path, "r+") as f:
            info = json.load(f)
            info["id"] = info["id"].replace("http://localhost:8887", os.environ["BUCKET"][:-1])
            f.seek(0)  # rewind
            json.dump(info, f, indent=4)
            f.truncate()
        os.remove(self._local_img_path)

    def map_wikidata(self, labels, value_en, vocabulary):
        if pd.isna(value_en):
            return None
        else:
            en = []
            pt = []
            values_en = value_en.split("|")
            # try:
            #     label_pt = vocabulary.loc[labels["pt"], "Label (pt)"]
            # except KeyError:
            #     logger.error(f"{labels['pt']} missing Portuguese label in vocabulary")
            #     label_pt = labels["en"]
            for value_en in values_en:
                try:
                    url = "http://wikidata.org/wiki/{0}".format(
                        vocabulary.loc[value_en, "Wikidata ID"]
                    )
                    en.append(
                        '<a class="uri-value-link" target="_blank" href="{0}">{1}</a>'.format(
                            url, value_en
                        )
                    )
                    value_pt = vocabulary.loc[value_en, "Label (pt)"]
                    pt.append(
                        '<a class="uri-value-link" target="_blank" href="{0}">{1}</a>'.format(
                            url, value_pt
                        )
                    )
                except:
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
