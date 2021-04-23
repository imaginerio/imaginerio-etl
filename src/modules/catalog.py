import os
from pprint import pprint
from xml.etree import ElementTree

import numpy as np
import pandas as pd
#from dagster import execute_pipeline, pipeline, solid, OutputDefinition, Output
import dagster as dg


#def xml_to_df(path):
@dg.solid
def read_xml(context,path):    
    with open(path, encoding="utf8") as f:
        tree = ElementTree.parse(f)
    root = tree.getroot()

    return root   


@dg.solid  # Find the uids
def find_uids(context, root):   
    uids = {}
    for thing in root[0][0]:
        uids[thing.attrib["uid"]] = thing[0].text   

    table = {}
    for field in uids.values():
        table[field] = []

    
    a = {"table": table, "uids":uids}

    return a
    
    

@dg.solid # Fill the records
def fill_records(context,root,a): 
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

                a['table'][a['uids'][field_value.attrib["uid"]]].append(value)
                added.add(field_value.attrib["uid"])
            except KeyError:
                continue
        for missing in a['uids'].keys() - added:
            try:
                a['table'][a['uids'][missing]].append(None)
            except KeyError:
                continue
    formated_table = a['table']
    return formated_table


@dg.solid # Create the actual DataFrame
def create_actual_df(context,formated_table):
    catalog_df = pd.DataFrame(formated_table)
    return catalog_df

@dg.solid # load
def load(context,catalog_df):   
    catalog_df = catalog_df.astype(
        {"DATA": str, "DATA LIMITE INFERIOR": str, "DATA LIMITE SUPERIOR": str}
    )
    catalog_df[["DATA LIMITE SUPERIOR", "DATA LIMITE INFERIOR"]] = catalog_df[
        ["DATA LIMITE SUPERIOR", "DATA LIMITE INFERIOR"]
    ].applymap(lambda x: x.split(".")[0])

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
    startna = catalog_df["start_date"].isna()
    endna = catalog_df["end_date"].isna()

    catalog_df.loc[year, "date_accuracy"] = "year"
    catalog_df.loc[month, "date_accuracy"] = "month"
    catalog_df.loc[day, "date_accuracy"] = "day"
    catalog_df.loc[circa, "date_accuracy"] = "circa"

    # dates to datetime
    catalog_df["date"] = catalog_df["date"].str.extract(r"([\d\/-]*\d{4}[-\/\d]*)")
    catalog_df["start_date"] = catalog_df["start_date"].str.extract(
        r"([\d\/-]*\d{4}[-\/\d]*)"
    )
    catalog_df["end_date"] = catalog_df["end_date"].str.extract(
        r"([\d\/-]*\d{4}[-\/\d]*)"
    )
    catalog_df[["date", "start_date", "end_date"]] = catalog_df[
        ["date", "start_date", "end_date"]
    ].applymap(lambda x: pd.to_datetime(x, errors="coerce", yearfirst=True))

    # reverse cretor name
    catalog_df["creator"] = catalog_df["creator"].str.replace(r"(.+),\s+(.+)", r"\2 \1")

    # save list of creators for rights assessment
    creators_df = catalog_df["creator"].unique()
    #pd.DataFrame(creators_df).to_csv(os.environ["CREATORS"], index=False)

    # fill empty start/end dates
    catalog_df.loc[circa & startna, "start_date"] = catalog_df["date"] - pd.DateOffset(
        years=5
    )
    catalog_df.loc[circa & endna, "end_date"] = catalog_df["date"] + pd.DateOffset(
        years=5
    )
    catalog_df.loc[startna, "start_date"] = catalog_df["date"]
    catalog_df.loc[endna, "end_date"] = catalog_df["date"]

    # extract dimensions
    dimensions_df = catalog_df["dimensions"].str.extract(
        r"[.:] (?P<height>\d+,?\d?) [Xx] (?P<width>\d+,?\d?)"
    )
    catalog_df["image_width"] = dimensions_df["width"]
    catalog_df["image_height"] = dimensions_df["height"]

    return catalog_df



@dg.pipeline
def catalog_main():     
    root = read_xml()
    a = find_uids(root)
    formated_table = fill_records(root, a)
    cumulus_df = create_actual_df(formated_table)
    load(cumulus_df)



''' if __name__ == "__main__":
    load(os.environ["CUMULUS_XML"]) '''
