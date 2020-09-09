import os
from pprint import pprint
from xml.etree import ElementTree

import numpy as np
import pandas as pd


def xml_to_df(path):

    with open(path) as f:
        tree = ElementTree.parse(f)
    root = tree.getroot()

    ns = {"cumulus": "http://www.canto.com/ns/Export/1.0"}

    # Find the uids
    uids = {}
    for thing in root[0][0]:
        uids[thing.attrib["uid"]] = thing[0].text

    table = {}
    for field in uids.values():
        table[field] = []

    # Fill the records
    for thing in root[1]:
        added = set()
        for field_value in thing.findall("cumulus:FieldValue", ns):
            try:
                if len(field_value) == 0:
                    value = field_value.text.strip()
                else:
                    value = field_value[0].text.strip().split(":")
                    value = str(value).strip("[']")

                table[uids[field_value.attrib["uid"]]].append(value)
                added.add(field_value.attrib["uid"])
            except KeyError:
                continue
        for missing in uids.keys() - added:
            try:
                table[uids[missing]].append(None)
            except KeyError:
                continue

    # Create the actual DataFrame
    cumulus_df = pd.DataFrame(table)

    return cumulus_df


def find_dates(date):
    """
    Find dates between 1500 and 2022 and parse to datetime
    """
    result = date.str.extract(r"([\d\/-]*\d{4}[-\/\d]*)")
    result = pd.to_datetime(date, errors="coerce", yearfirst=True)
    return result


def load(path):
    catalog_df = xml_to_df(path)
    catalog_df = catalog_df.astype(
        {"DATA": str, "DATA LIMITE INFERIOR": str, "DATA LIMITE SUPERIOR": str}
    )

    # rename columns
    catalog_df = catalog_df.rename(
        columns={
            "Record Name": "id",
            "TÍTULO": "title",
            "RESUMO": "description",
            "AUTORIA": "creator",
            "DATA": "date",
            "DATA LIMITE INFERIOR": "start_date",
            "DATA LIMITE SUPERIOR": "end_date",
            "DIMENSÃO": "dimensions",
            "PROCESSO FORMADOR DA IMAGEM": "fabrication_method",
            "LOCAL": "place",
            "DESIGNAÇÃO GENÉRICA": "type",
        },
    )

    # select columns
    catalog_df = catalog_df[
        [
            "id",
            "title",
            "description",
            "creator",
            "date",
            "start_date",
            "end_date",
            "type",
            "fabrication_method",
            "dimensions",
            "place",
        ]
    ]

    # remove file extension
    catalog_df["id"] = catalog_df["id"].str.split(".", n=1, expand=True)

    # remove duplicates
    catalog_df = catalog_df.drop_duplicates(subset="id", keep="last")

    # check dates accuracy
    circa = catalog_df["date"].str.contains(r"[a-z]", na=False,)
    year = catalog_df["date"].str.count(r"[\/-]") == 0
    month = catalog_df["date"].str.count(r"[\/-]") == 1
    day = catalog_df["date"].str.count(r"[\/-]") == 2

    catalog_df.loc[year, "date_accuracy"] = "year"
    catalog_df.loc[month, "date_accuracy"] = "month"
    catalog_df.loc[day, "date_accuracy"] = "day"
    catalog_df.loc[circa, "date_accuracy"] = "circa"

    # dates to datetime
    catalog_df["date"] = find_dates(catalog_df["date"])
    catalog_df["start_date"] = find_dates(catalog_df["start_date"])
    catalog_df["end_date"] = find_dates(catalog_df["end_date"])

    # reverse cretor name
    catalog_df["creator"] = catalog_df["creator"].str.replace(r"(.+),\s+(.+)", r"\2 \1")

    # save list of creators for rights assessment
    creators_df = catalog_df["creator"].unique()
    pd.DataFrame(creators_df).to_csv(os.environ["CREATORS"], index=False)

    # fill empty start/end dates
    catalog_df.loc[
        (catalog_df["date_accuracy"] == "circa") & (catalog_df["start_date"].isna()),
        "start_date",
    ] = catalog_df["date"] - pd.DateOffset(years=5)

    catalog_df.loc[
        (catalog_df["date_accuracy"] == "circa") & (catalog_df["end_date"].isna()),
        "end_date",
    ] = catalog_df["date"] + pd.DateOffset(years=5)

    # extract dimensions
    dimensions_df = catalog_df["dimensions"].str.extract(
        r"[.:] (?P<height>\d+,?\d?) [Xx] (?P<width>\d+,?\d?)"
    )
    catalog_df["image_width"] = dimensions_df["width"]
    catalog_df["image_height"] = dimensions_df["height"]

    return catalog_df


if __name__ == "__main__":
    load(os.environ["CUMULUS_XML"])
