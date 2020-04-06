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
    camera = camera.rename(columns={"name": "id", "long": "lng",})

    # drop duplicates
    camera = camera.drop_duplicates(subset="id", keep="last")

    return camera


def create_cones_df(path):
    """
    Return geodataframe from camera.geojson
    """
    pass


def create_catalog_df(path):
    """
    Return dataframe from cumulus.xlsx
    """

    catalog = pd.read_excel(
        path,
        dtype={"DATA": str, "DATA LIMITE INFERIOR": str, "DATA LIMITE SUPERIOR": str},
    )

    return catalog


def create_images_df(path):
    """
    List all files in directory, write github paths, check size ratio
    """
    pass


def wikidata_query(df):
    """
    SPARQL query by identifier to return items QIDs
    """
    pass


def join_dataframes():
    """
    Join all dataframes by identifier
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

        print("Creating catalog dataframe...")
        catalog = create_catalog_df(CATALOG_PATH)
        print(f"Catalog DataFrame contains {len(catalog.columns)} columns")

    except Exception as e:

        print(str(e))


if __name__ == "__main__":
    main()
