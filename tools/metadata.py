import sys
import os
import argparse

import pandas as pd
import numpy as np
import geopandas as gpd


def create_camera_df(path):
    """
    Return dataframe from camera.csv
    """

    # read csv
    camera = pd.read_csv(path)

    # remove spaces
    camera.columns = camera.columns.str.strip().str.lower().str.replace(" ", "")

    # rename columns
    camera = camera.rename(columns={"name": "identifier", "long": "lng",})

    # drop duplicates
    camera = camera.drop_duplicates(subset="identifier", keep="last")

    return camera


def create_viewcone_df(path):
    """
    Return geodataframe from camera.geojson
    """

    # read geojson
    viewcone = gpd.read_file(path)

    # rename columns
    viewcone = viewcone.rename(columns={"name": "identifier",})

    # remove record_name file extension
    viewcone["identifier"] = viewcone["identifier"].str.split(".", n=1, expand=True)

    # subset by columns
    viewcone = viewcone[["identifier", "geometry"]]

    # remove duplicates
    viewcone = viewcone.drop_duplicates(subset="identifier", keep="last")

    return viewcone


def create_catalog_df(path):
    """
    Return dataframe from cumulus.xlsx
    """

    catalog = pd.read_excel(
        path,
        dtype={"DATA": str, "DATA LIMITE INFERIOR": str, "DATA LIMITE SUPERIOR": str},
    )

    # rename columns
    catalog = catalog.rename(
        columns={
            "Record Name": "identifier",
            "TÍTULO": "title",
            "RESUMO": "description",
            "AUTORIA": "author",
            "DATA": "date",
            "DATA LIMITE INFERIOR": "start_date",
            "DATA LIMITE SUPERIOR": "end_date",
            "DIMENSÃO": "dimensions",
            "PROCESSO FORMADOR DA IMAGEM": "fabrication_method",
            "LOCAL": "place",
        },
    )

    catalog = catalog[
        [
            "identifier",
            "title",
            "description",
            "author",
            "date",
            "start_date",
            "end_date",
            "dimensions",
            "fabrication_method",
            "place",
        ]
    ]

    # remove file extension
    catalog["identifier"] = catalog["identifier"].str.split(".", n=1, expand=True)

    # remove duplicates
    catalog = catalog.drop_duplicates(subset="identifier", keep="last")

    # check dates accuracy
    catalog["accurate_date"] = np.where(
        catalog.date.astype(str).str.contains(r"[a-z]"), False, True
    )
    catalog["accurate_date"] = np.where(
        catalog.date.isnull(), False, catalog["accurate_date"]
    )

    return catalog


def create_images_df(path):
    """
    List all files in directory, write github paths, check size ratio
    """
    pass


def wikidentifierata_query(df):
    """
    SPARQL query by identifierentifier to return items Qidentifiers
    """
    pass


def join_dataframes():
    """
    Join all dataframes by identifierentifier
    """
    pass


def build_omeka_csv(df):
    """
    Prepare and save csv
    """
    pass


def main():
    """
    Execute all functions
    """

    # path variables

    CAMERA_PATH = "../metadata/camera/camera.csv"
    CONES_PATH = "../metadata/camera/camera.geojson"
    CATALOG_PATH = "../metadata/catalog/cumulus.xlsx"
    IMAGES_PATH = "../images/ "

    try:

        print("Creating camera dataframe...")
        camera = create_camera_df(CAMERA_PATH)
        print(camera.head())

        print("Creating viewcone dataframe...")
        viewcone = create_viewcone_df(CONES_PATH)
        print(viewcone.head())

        print("Creating catalog dataframe...")
        catalog = create_catalog_df(CATALOG_PATH)
        print(catalog.head())

    except Exception as e:

        print(str(e))


if __name__ == "__main__":
    main()
    # pandas saves all data regarding metadata in metadata/metadata.csv
