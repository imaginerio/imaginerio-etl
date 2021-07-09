import os
import shutil
from datetime import datetime as dt
from pathlib import Path

import dagster as dg
import geojson
import numpy as np
import pandas as pd

from dagster.core.definitions import solid


@dg.solid(input_defs=[dg.InputDefinition("metadata", root_manager_key="metadata_root")])
def load_metadata(_, metadata):

    metadata["date"] = pd.to_datetime(metadata["date"])
    metadata["first_year"] = pd.to_datetime(metadata["first_year"])
    metadata["last_year"] = pd.to_datetime(metadata["last_year"])
    export_df = metadata

    # filter items
    export_df = export_df.dropna(
        subset=["geometry", "first_year", "last_year", "portals_url", "img_hd"]
    )

    return export_df


@dg.solid
def organize_columns_to_omeka(_, df):
    # format data
    omeka_df = df
    omeka_df["portals_url"] = omeka_df["portals_url"] + " Instituto Moreira Salles"
    omeka_df["wikidata_id"] = omeka_df["wikidata_id"] + " Wikidata"
    omeka_df["image_width"] = omeka_df["image_width"].str.replace(",", ".")
    omeka_df["image_height"] = omeka_df["image_height"].str.replace(",", ".")

    # create columns
    omeka_df["rights"] = ""
    omeka_df["citation"] = ""
    omeka_df["item_sets"] = "all||views"
    smapshot = pd.read_csv("data/output/log/smapshot.csv")
    include = omeka_df["id"].isin(smapshot["id"])
    omeka_df.loc[include, "item_sets"] = omeka_df["item_sets"] + "||smapshot"
    omeka_df["dcterms:available"] = df["date_circa"]
    omeka_df.loc[~(df["date_accuracy"] == "circa"), "dcterms:available"] = (
        df["first_year"].astype(str) + "/" + df["last_year"].astype(str)
    )

    # rename columns
    omeka_df = omeka_df.rename(
        columns={
            "id": "dcterms:identifier",
            "title": "dcterms:title",
            "description": "dcterms:description",
            "creator": "dcterms:creator",
            "date_created": "dcterms:created",
            "date_circa": "dcterms:temporal",
            "type": "dcterms:type",
            "image_width": "schema:width",
            "image_height": "schema:height",
            "rights": "dcterms:rights",
            "citation": "dcterms:bibliographicCitation",
            "portals_url": "dcterms:source",
            "wikidata_id": "dcterms:hasVersion",
            "geometry": "schema:polygon",
            "wikidata_depict": "foaf:depicts",
            "img_hd": "media",
        }
    )

    # select columns
    omeka_df = omeka_df[
        [
            "dcterms:identifier",
            "dcterms:title",
            "dcterms:description",
            "dcterms:creator",
            "dcterms:created",
            "dcterms:temporal",
            "dcterms:available",
            "dcterms:type",
            "dcterms:rights",
            "dcterms:bibliographicCitation",
            "dcterms:source",
            "dcterms:hasVersion",
            "latitude",
            "longitude",
            "schema:polygon",
            "foaf:depicts",
            "schema:width",
            "schema:height",
            "media",
            "item_sets",
        ]
    ]

    return omeka_df


@dg.solid(
    input_defs=[dg.InputDefinition("jstor", root_manager_key="jstor_root")],
    output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="import_omeka")],
)
def import_omeka_dataframe(_, df, jstor):
    # append JSTOR migration
    omeka_df = df.append(jstor)
    omeka_df.name = "import_omeka"

    return omeka_df.set_index("dcterms:identifier")


@dg.solid
def make_df_to_wikidata(_, df):
    quickstate = pd.DataFrame(
        columns=[
            "Qid",
            "P31",
            "Lpt-br",
            "Dpt-br",
            "Den",
            "P571",
            "qal1319",
            "qal1326",
            "P17",
            "P1259",
            "qal2044",
            "qal7787",
            "qal8208",
            "P170",
            "P186",
            "P195",
            "P217",
            "P2079",
            "P4036",
            "P2049",
            "P2048",
            "P7835",
        ]
    )

    # date_accuracy
    quickstate["date_accuracy"] = df["date_accuracy"]
    circa = quickstate["date_accuracy"] == "circa"
    year = quickstate["date_accuracy"] == "year"
    month = quickstate["date_accuracy"] == "month"
    day = quickstate["date_accuracy"] == "day"

    quickstate["P571"] = df["date"].apply(dt.isoformat)
    quickstate.loc[circa, "P571"] = quickstate["P571"] + "Z/8"
    quickstate.loc[year, "P571"] = quickstate["P571"] + "Z/9"
    quickstate.loc[month, "P571"] = quickstate["P571"] + "Z/10"
    quickstate.loc[day, "P571"] = quickstate["P571"] + "Z/11"
    # earliest date
    quickstate.loc[circa, "qal1319"] = df["first_year"].apply(dt.isoformat) + "Z/9"
    # latest date
    quickstate.loc[circa, "qal1326"] = df["last_year"].apply(dt.isoformat) + "Z/9"
    # pt-br label
    quickstate["Lpt-br"] = df["title"]
    # pt-br description
    quickstate["Dpt-br"] = "Fotografia de " + df["creator"]
    # en description
    quickstate["Den"] = "Photograph by " + df["creator"]
    # Instance of
    quickstate["P31"] = "Q125191"
    # country
    quickstate["P17"] = "Q155"
    # coordinate of POV
    quickstate["P1259"] = (
        "@" + df["latitude"].astype(str) + "/" + df["longitude"].astype(str)
    )
    # altitude
    quickstate["qal2044"] = df["altitude"].astype(str) + "U11573"
    # heading
    quickstate["qal7787"] = df["heading"].astype(str) + "U28390"
    # tilt
    quickstate["qal8208"] = df["tilt"].astype(str) + "U28390"
    # creator
    quickstate["P170"] = df["creator"]
    # material used
    quickstate["P186"] = df["type"]
    # collection
    quickstate["P195"] = "Q71989864"
    # inventory number
    quickstate["P217"] = df["id"]
    # fabrication method
    quickstate["P2079"] = df["fabrication_method"]
    # field of view
    quickstate["P4036"] = df["fov"].astype(str) + "U28390"
    # width
    quickstate["P2049"] = df["image_width"].str.replace(",", ".") + "U174728"
    # height
    quickstate["P2048"] = df["image_height"].str.replace(",", ".") + "U174728"
    # IMS ID
    quickstate["P7835"] = df["portals_id"].astype(int)
    # qid
    quickstate["qid"] = df["wikidata_id"].str.split("/").str[-1]
    # Copyright status
    # quickstate["P6216"]

    paper = quickstate["P186"].str.contains("Papel", na=False)
    glass = quickstate["P186"].str.contains("Vidro", na=False)
    quickstate.loc[paper, "P186"] = "Q11472"
    quickstate.loc[glass, "P186"] = "Q11469"

    # fabrication method
    gelatin = quickstate["P2079"].str.contains("GELATINA", na=False)
    albumin = quickstate["P2079"].str.contains("ALBUMINA", na=False)
    quickstate.loc[gelatin, "P2079"] = "Q172984"
    quickstate.loc[albumin, "P2079"] = "Q580807"

    return quickstate


@solid(
    output_defs=[
        dg.OutputDefinition(io_manager_key="pandas_csv", name="import_wikidata")
    ]
)
def organise_creator(_, quickstate):
    creators = {
        "Augusto Malta": "Q16495239",
        "Anônimo": "Q4233718",
        "Marc Ferrez": "Q3180571",
        "Georges Leuzinger": "Q5877879",
        "José dos Santos Affonso": "Q63993961",
        "N. Viggiani": "Q65619909",
        "Archanjo Sobrinho": "Q64009665",
        "F. Basto": "Q55089601",
        "J. Faria de Azevedo": "Q97570600",
        "S. H. Holland": "Q65619918",
        "Augusto Monteiro": "Q65619921",
        "Jorge Kfuri": "Q63166336",
        "Camillo Vedani": "Q63109123",
        "Fritz Büsch": "Q63109492",
        "Armando Pittigliani": "Q19607834",
        "Braz": "Q97487621",
        "Stahl & Wahnschaffe": "Q63109157",
        "Gomes Junior": "Q86942676",
        "A. Ruelle": "Q97570551",
        "Guilherme Santos": "Q55088608",
        "Albert Frisch": "Q21288396",
        "José Baptista Barreira Vianna": "Q63166517",
        "Alfredo Krausz": "Q63166405",
        "Therezio Mascarenhas": "Q97570728",
        "Torres": "Q65619905",
        "Theodor Preising": "Q63109140",
        "Augusto Stahl": "Q4821327",
        "Luiz Musso": "Q89538832",
        "Carlos Bippus": "Q63109147",
        "Thiele": "Q64825643",
        "Revert Henrique Klumb": "Q3791061",
        "Juan Gutierrez": "Q10312614",
        "F. Manzière": "Q65619915",
        "Antonio Luiz Ferreira": "Q97570558",
        "Etienne Farnier": "Q97570575",
        "José Francisco Corrêa": "Q10309433",
        "Chapelin": "Q97570376",
        "J. Teixeira": "Q89642578",
        "F. Garcia": "Q97570588",
        "A. de Barros Lobo": "Q97570363",
        "Bloch": "Q61041099",
    }

    def name2qid(name):
        try:
            qid = creators[f"{name}"]
        except KeyError:
            qid = ""
        return qid

    quickstate["P170"] = quickstate["P170"].apply(name2qid)
    quickstate = quickstate.drop(columns="date_accuracy")
    quickstate.name = "mport_wikidata"

    return quickstate.set_index("qid")
