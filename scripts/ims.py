import argparse
import os
import re
from xml.etree import ElementTree

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import distinct

from helpers import ims2jstor
from portals import main as query_portals
from pull_images import main as pull_images

load_dotenv(override=True)

def xml_to_df(path):
    """
    Build Pandas DataFrame from XML file
    """
    # Find the uids

    with open(path, encoding="utf8") as f:
      tree = ElementTree.parse(f)
      root = tree.getroot()

    uids = {}
    for thing in root[0][0]:
        uids[thing.attrib["uid"]] = thing[0].text

    table = {}
    for field in uids.values():
        table[field] = []

    outDict = {"table": table, "uids": uids}

    # Fill the records
    ns = {"cumulus": "http://www.canto.com/ns/Export/1.0"}
    for thing in root[1]:
        added = set()
        for field_value in thing.findall("cumulus:FieldValue", ns):
            try:
                if len(field_value) == 0:
                    value = field_value.text.strip()
                else:
                    value = field_value[0].text.strip().split(":")
                    value = str(value).strip("[']")

                outDict["table"][outDict["uids"][field_value.attrib["uid"]]].append(
                    value
                )
                added.add(field_value.attrib["uid"])
            except KeyError:
                continue
        for missing in outDict["uids"].keys() - added:
            try:
                outDict["table"][outDict["uids"][missing]].append(None)
            except KeyError:
                continue
    formated_table = outDict["table"]
    cumulus_df = pd.DataFrame(formated_table).drop_duplicates()

    # load
    cumulus_df = cumulus_df.astype(
        {"DATA": str, "DATA LIMITE INFERIOR": str, "DATA LIMITE SUPERIOR": str},
        copy=False,
    )
    cumulus_df[["DATA LIMITE SUPERIOR", "DATA LIMITE INFERIOR"]] = cumulus_df[
        ["DATA LIMITE SUPERIOR", "DATA LIMITE INFERIOR"]
    ].applymap(lambda x: x.split(".")[0])

    return cumulus_df
 

def extract_dimensions(df):
    """
    Infer width and height in mm from dimensions field
    """
    dimensions = df["dimensions"].str.extract(
        r"[.:] (?P<height>\d+,?\d?) [Xx] (?P<width>\d+,?\d?)"
    )

    df["Width"] = dimensions["width"].str.replace(",", ".").astype(float) * 10
    df["Height"] = dimensions["height"].str.replace(",", ".").astype(float) * 10

    return df


def format_dates(df):
    """
    Infer circa dates and format date string according to accuracy
    """

    # infer accuracy
    accuracy_conditions = [
        (df["Date"].str.count(r"[-\/^a-z]") == 0),
        (df["Date"].str.count(r"[\/-]") == 1),
        (df["Date"].str.count(r"[\/-]") == 2),
        (df["Date"].str.contains(r"[a-z]", na=False)),
    ]
    accuracy_choices = ["year", "month", "day", "circa"]
    df["date_accuracy"] = np.select(accuracy_conditions, accuracy_choices)

    # format dates
    
    df["First Year"] = df["First Year"].str.extract(r"([\d\/-]*\d{4}[-\/\d]*)")
    df["Last Year"] = df["Last Year"].str.extract(r"([\d\/-]*\d{4}[-\/\d]*)")
    df["datetime"] = df["Date"].str.extract(r"([\d\/-]*\d{4}[-\/\d]*)") 
    df[["First Year","Last Year","datetime"]] = df[["First Year","Last Year","datetime"]].applymap(
        lambda x: pd.to_datetime(x, errors="coerce", yearfirst=True)
        )

    circa = df["date_accuracy"] == "circa"
    year = df["date_accuracy"] == "year"
    month = df["date_accuracy"] == "month"
    day = df["date_accuracy"] == "day"

    # infer first and last year when unavailable
    df.loc[circa & df["First Year"].isna(), "First Year"] = df[
        "datetime"
    ] - pd.DateOffset(years=5)
    df.loc[circa & df["Last Year"].isna(), "Last Year"] = df[
        "datetime"
    ] + pd.DateOffset(years=5)
    df.loc[df["First Year"].isna(), "First Year"] = df["datetime"]
    df.loc[df["Last Year"].isna(), "Last Year"] = df["datetime"]

    # datetime to string according to date accuracy
    df["First Year"] = df["First Year"].dt.strftime("%Y")
    df["Last Year"] = df["Last Year"].dt.strftime("%Y")

    format_conditions = [circa, year, month, day]
    format_choices = [
        ("circa " + (df["datetime"].dt.strftime("%Y"))),
        df["datetime"].dt.strftime("%Y"),
        df["datetime"].dt.strftime("%m/%Y"),
        df["datetime"].dt.strftime("%d/%m/%Y"),
    ]
    df["Date"] = np.select(format_conditions, format_choices)


def format_data(df):
    """
    Rename columns and values, remove file extension from identifiers,
    normalize creator names, infer dimensions and format dates
    """
    # rename columns
    df.rename(
        columns={
            "Record Name": "Document ID",
            "CÓDIGO DE IDENTIFICAÇÃO PRELIMINAR": "preliminary id",
            "TÍTULO": "Title",
            "RESUMO": "Description (Portuguese)",
            "AUTORIA": "Creator",
            "DATA": "Date",
            "DATA LIMITE INFERIOR": "First Year",
            "DATA LIMITE SUPERIOR": "Last Year",
            "DIMENSÃO": "dimensions",
            "PROCESSO FORMADOR DA IMAGEM": "Fabrication Method",
            "DESIGNAÇÃO GENÉRICA": "Material",
            "FORMATO PADRÃO": "format",
        },
        inplace=True
    )

    # remove file extension
    df["Document ID"] = df["Document ID"].str.split(
        ".", n=1, expand=True
    )[0]

    # replace newlines
    df["Description (Portuguese)"] = df["Description (Portuguese)"].str.replace("\n", "")

    # reverse creator name
    df["Creator"] = df["Creator"].str.replace(r"(.+),\s+(.+)", r"\2 \1")

    # create columns
    df["Type"] = "Photograph"
    df["Collections"] = "Views"
    df["Provider"] = "Instituto Moreira Salles"
    df["Rights"] = np.nan
    df["Required Statement"] = "Provided by Instituto Moreira Salles"
    df["Smapshot ID"] = np.nan
    df["Document URL"] = np.nan
    df["Media URL"] = np.nan

    # map materials, types and methods
    material_map = {
        "FOTOGRAFIA/PAPEL": "Photographic Print",
        "REPRODUÇÃO FOTOMECÂNICA/ Papel": "Photomechanical Print",
        "NEGATIVO/ Vidro": "Glass Plate Negative",
        "DIAPOSITIVO/ Vidro": "Glass Diapositive",
    }

    fabrication_method_map = {
        "AUTOCHROME / Corante e prata": "Autochrome",
        "ALBUMINA/ Prata": "Albumine",
        "GELATINA/ Prata": "Gelatin Silver",
        "COLÓDIO/ Prata": "Collodion",
        "LANTERN SLIDE / Prata": "Lantern Slide",
        "AMBROTIPIA/ Prata": "Ambrotype",
        "COLOTIPIA/ Pigmento": "Collotype Print",
        "FOTOGRAVURA/ Pigmento": "Photogravure",
        "MEIO-TOM/ Pigmento": "Photogravure"
    }
    
    df["Material"] = df["Material"].map(material_map)
    df["Fabrication Method"] = df["Fabrication Method"].map(fabrication_method_map)
    df.loc[df["format"] == "Estereoscopia", "Type"] = (
        df["Type"] + "|Stereoscopy"
    )

    format_dates(df)
    extract_dimensions(df)

    return df.filter(items=[
        "Document ID",
        "Title",
        "Creator",
        "Description (Portuguese)",
        "Date",
        "First Year",
        "Last Year",
        "Type",
        "Collections",
        "Provider",
        "Material",
        "Fabrication Method",
        "Rights",
        "Required Statement",
        "Width",
        "Height",
        "Document URL",
        "Media URL",
    ]).set_index("Document ID")

def main():
    ims = xml_to_df(os.environ["CUMULUS_XML"])
    ims = format_data(ims)
    ims.to_csv(os.environ["IMS_METADATA"])

    if args.mode == "portals" or args.mode == "all":
        query_portals()
    if args.mode == "images" or args.mode == "all":
        pull_images()
    ims2jstor()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", "-m", 
        help="Which operations to run (portals, images or all)", 
        choices=["portals", "images", "all"]
    )
    args = parser.parse_args()

    main()