import os
from pprint import pprint
from xml.etree import ElementTree

import numpy as np
import pandas as pd
#from dagster import execute_pipeline, pipeline, solid, OutputDefinition, Output
import dagster as dg

#solids catalog
#def xml_to_df(path):
@dg.solid
def read_xml(context):    
    path = context.solid_config
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

    
    outDict = {"table": table, "uids":uids}

    return outDict
    
    

@dg.solid # Fill the records
def fill_records(context,root,outDict): 
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
    loaded_df[["DATA LIMITE SUPERIOR", "DATA LIMITE INFERIOR"]] = loaded_df[
        ["DATA LIMITE SUPERIOR", "DATA LIMITE INFERIOR"]
    ].applymap(lambda x: x.split(".")[0])

    return loaded_df

@dg.solid # rename columns
def rename_columns(context,df):    
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
def select_columns(context,df):    
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
def remove_extension(context,df):
    df["id"] = df["id"].str.split(".", n=1, expand=True)
    removed_extension_df = df

    return removed_extension_df

@dg.solid # remove duplicates    
def remove_duplicates(context,df): 
    removed_duplicates_df = df.drop_duplicates(subset="id", keep="last")

    return removed_duplicates_df


@dg.solid# check dates accuracy
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

    accuraced_dates = df

    return accuraced_dates

@dg.solid   # reverse cretor name
def reverse_creators_name(context,df):
    df["creator"] = df["creator"].str.replace(r"(.+),\s+(.+)", r"\2 \1")
    organized_names = df

    return organized_names
 
@dg.solid   # save list of creators for rights assessment
def creators_list(context,df):
    listed_creators = df["creator"].unique()
    #pd.DataFrame(creators_df).to_csv(os.environ["CREATORS"], index=False)

    return listed_creators

@dg.solid    # extract dimensions
def extract_dimensions(context,df):
    dimensions = df["dimensions"].str.extract(
        r"[.:] (?P<height>\d+,?\d?) [Xx] (?P<width>\d+,?\d?)"
    )
    df["image_width"] = dimensions["width"]
    df["image_height"] = dimensions["height"]

    extracted_dimensions = df

    return extracted_dimensions

#pipeline catalog
#@dg.pipeline  
@dg.composite_solid
def catalog_main():
    root = read_xml()   
    outDict = find_uids(root)
    formated_table = fill_records(root,outDict)
    created_df = create_actual_df(formated_table)
    loaded_df = load(created_df)
    renamed_columns = rename_columns(loaded_df)
    selected_columns = select_columns(renamed_columns)
    removed_extension_df = remove_extension(selected_columns)
    removed_duplicates_df = remove_duplicates(removed_extension_df)
    organized_names = reverse_creators_name(removed_duplicates_df)
    accuraced_dates = dates_accuracy(organized_names)
    catalog = extract_dimensions(accuraced_dates)
    listed_creators = creators_list(organized_names)
    
    return catalog


''' if __name__ == "__main__":
    load(os.environ["CUMULUS_XML"]) '''
