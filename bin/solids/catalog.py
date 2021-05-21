import os
from pprint import pprint
from xml.etree import ElementTree

import dagster as dg
import numpy as np
import pandas as pd


#solids catalog
@dg.solid(input_defs=[dg.InputDefinition("root", root_manager_key="xml")]) 
def xml_to_df(context, root): 
    # Find the uids 
    
    uids = {}
    for thing in root[0][0]:
        uids[thing.attrib["uid"]] = thing[0].text   

    table = {}
    for field in uids.values():
        table[field] = []

    
    outDict = {"table": table, "uids":uids}   
    

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

                outDict['table'][outDict['uids'][field_value.attrib["uid"]]].append(value)
                added.add(field_value.attrib["uid"])
            except KeyError:
                continue
        for missing in outDict['uids'].keys() - added:
            try:
                outDict['table'][outDict['uids'][missing]].append(None)
            except KeyError:
                continue
    formated_table = outDict['table']
    catalog_df = pd.DataFrame(formated_table)
   
    #load
    catalog_df = catalog_df.astype(
        {"DATA": str, "DATA LIMITE INFERIOR": str, "DATA LIMITE SUPERIOR": str}
    )
    catalog_df[["DATA LIMITE SUPERIOR", "DATA LIMITE INFERIOR"]] = catalog_df[
        ["DATA LIMITE SUPERIOR", "DATA LIMITE INFERIOR"]
    ].applymap(lambda x: x.split(".")[0])

    return catalog_df

@dg.solid 
def organize_columns(context,df): 
    # rename columns  
    catalog_df = df.rename(
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

    # reverse cretor name
    catalog_df["creator"] = catalog_df["creator"].str.replace(r"(.+),\s+(.+)", r"\2 \1")
    
    return catalog_df


@dg.solid(output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="creators")])  # save list of creators for rights assessment
def creators_list(context,df):
    creators_df = df["creator"].unique()
    listed_creators = pd.DataFrame(creators_df)

    return listed_creators


@dg.solid # check dates accuracy
def dates_accuracy(context,df):
    circa = df["date"].str.contains(r"[a-z]", na=False,)
    year = df["date"].str.count(r"[\/-]") == 0
    month = df["date"].str.count(r"[\/-]") == 1
    day = df["date"].str.count(r"[\/-]") == 2
    startna = df["start_date"].isna()
    endna = df["end_date"].isna()

    df.loc[year, "date_accuracy"] = "year"
    df.loc[month, "date_accuracy"] = "month"
    df.loc[day, "date_accuracy"] = "day"
    df.loc[circa, "date_accuracy"] = "circa"

    #format date
    df["date"] = df["date"].str.extract(r"([\d\/-]*\d{4}[-\/\d]*)")
    df["start_date"] = df["start_date"].str.extract(
        r"([\d\/-]*\d{4}[-\/\d]*)"
    )
    df["end_date"] = df["end_date"].str.extract(
        r"([\d\/-]*\d{4}[-\/\d]*)"
    )
    df[["date", "start_date", "end_date"]] = df[
        ["date", "start_date", "end_date"]
    ].applymap(lambda x: pd.to_datetime(x, errors="coerce", yearfirst=True))

    #fill dates
    df.loc[circa & startna, "start_date"] = df["date"] - pd.DateOffset(
        years=5
    )
    df.loc[circa & endna, "end_date"] = df["date"] + pd.DateOffset(
        years=5
    )
    df.loc[startna, "start_date"] = df["date"]
    df.loc[endna, "end_date"] = df["date"]

    catalog_df = df

    return catalog_df

  
@dg.solid(output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="catalog")]) # extract dimensions
def extract_dimensions(context,df):
    dimensions = df["dimensions"].str.extract(
        r"[.:] (?P<height>\d+,?\d?) [Xx] (?P<width>\d+,?\d?)"
    )
    df["image_width"] = dimensions["width"]
    df["image_height"] = dimensions["height"]

    catalog = df
    catalog.name="catalog"

    return catalog


