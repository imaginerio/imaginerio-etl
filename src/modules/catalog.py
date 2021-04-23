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
    created_df = pd.DataFrame(formated_table)
    return created_df

@dg.solid # load
def load(context,df):   
    loaded_df = df.astype(
        {"DATA": str, "DATA LIMITE INFERIOR": str, "DATA LIMITE SUPERIOR": str}
    )
    catalog_df[["DATA LIMITE SUPERIOR", "DATA LIMITE INFERIOR"]] = catalog_df[
        ["DATA LIMITE SUPERIOR", "DATA LIMITE INFERIOR"]
    ].applymap(lambda x: x.split(".")[0])
    return loaded_df

@dg.solid # rename columns
def rename_columns(df):    
    renamed_columns = df.rename(
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
    return renamed_columns


@dg.solid # select columns from renamed coluns of catalog df
def select_columns(df):    
    selected_columns = df[
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
    return selected_columns


@dg.solid # remove file extension    
def remove_extension(df):
    df["id"] = df["id"].str.split(".", n=1, expand=True)
    return removed_extension_df

@dg.solid # remove duplicates    
def remove_duplicates(df): 
    df = df.drop_duplicates(subset="id", keep="last")
    return removed_duplicates_df


@dg.solid# check dates accuracy
def check_dates(df):
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

    return df

@dg.solid # dates to datetime
def dates_datetime(df):    
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

@dg.solid   # reverse cretor name
    catalog_df["creator"] = catalog_df["creator"].str.replace(r"(.+),\s+(.+)", r"\2 \1")

@dg.solid   # save list of creators for rights assessment
    creators_df = catalog_df["creator"].unique()
    #pd.DataFrame(creators_df).to_csv(os.environ["CREATORS"], index=False)

@dg.solid    # fill empty start/end dates
def 
    catalog_df.loc[circa & startna, "start_date"] = catalog_df["date"] - pd.DateOffset(
        years=5
    )
    catalog_df.loc[circa & endna, "end_date"] = catalog_df["date"] + pd.DateOffset(
        years=5
    )
    catalog_df.loc[startna, "start_date"] = catalog_df["date"]
    catalog_df.loc[endna, "end_date"] = catalog_df["date"]

@dg.solid    # extract dimensions
def extrac_dimensions(df):
    dimensions_df = df["dimensions"].str.extract(
        r"[.:] (?P<height>\d+,?\d?) [Xx] (?P<width>\d+,?\d?)"
    )
    df["image_width"] = dimensions_df["width"]
    df["image_height"] = dimensions_df["height"]

    return df



@dg.pipeline
def catalog_main():     
    root = read_xml()
    a = find_uids(root)
    formated_table = fill_records(root, a)
    cumulus_df = create_actual_df(formated_table)
    load(cumulus_df)



''' if __name__ == "__main__":
    load(os.environ["CUMULUS_XML"]) '''
