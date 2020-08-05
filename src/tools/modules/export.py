from pathlib import Path
from datetime import datetime
import os, shutil

import pandas as pd
import numpy as np
from bokeh.plotting import output_file, show
from bokeh.layouts import column, layout

from . import report, maps


def omeka_csv(df):
    """
    Export omeka.csv
    """

    # read final dataframe
    omeka_df = df

    # datetime to year strings
    omeka_df["date"] = omeka_df["date"].dt.strftime("%d/%m/%Y")
    omeka_df["start_date"] = omeka_df["start_date"].dt.strftime("%Y")
    omeka_df["end_date"] = omeka_df["end_date"].dt.strftime("%Y")

    # join years into interval
    omeka_df["interval"] = omeka_df["start_date"] + "/" + omeka_df["end_date"]
    # omeka_df = omeka_df.drop(columns=["start_date", "end_date"])

    # pick date to be displayed (precise or range)
    omeka_df.loc[(omeka_df["accurate_date"] == False), "date"] = np.nan
    omeka_df.loc[(omeka_df["accurate_date"] == True), "interval"] = np.nan

    # format data
    omeka_df["portals_url"] = omeka_df["portals_url"] + " Instituto Moreira Salles"
    omeka_df["wikidata_id"] = omeka_df["wikidata_id"] + " Wikidata"
    omeka_df["image_width"] = omeka_df["image_width"].str.replace(",", ".")
    omeka_df["image_height"] = omeka_df["image_height"].str.replace(",", ".")

    # create columns
    omeka_df["rights"] = ""
    omeka_df["citation"] = ""

    # filter items
    omeka_df = omeka_df.copy().dropna(
        subset=["geometry", "start_date", "end_date", "portals_url", "img_hd"]
    )

    # rename columns
    omeka_df = omeka_df.rename(
        columns={
            "id": "dcterms:identifier",
            "title": "dcterms:title",
            "description": "dcterms:description",
            "creator": "dcterms:creator",
            "date": "dcterms:date",
            "interval": "dcterms:temporal",
            "type": "dcterms:type",
            "image_width": "schema:width",
            "image_height": "schema:height",
            "rights": "dcterms:rights",
            "citation": "dcterms:bibliographicCitation",
            "portals_url": "dcterms:source",
            "wikidata_id": "dcterms:hasVersion",
            "lat": "latitude",
            "lng": "longitude",
            "geometry": "dcterms:spatial",
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
            "dcterms:date",
            "dcterms:temporal",
            "dcterms:type",
            "dcterms:rights",
            "dcterms:bibliographicCitation",
            "dcterms:source",
            "dcterms:hasVersion",
            "latitude",
            "longitude",
            "foaf:depicts",
            "schema:width",
            "schema:height",
            "media",
        ]
    ]

    # save csv
    omeka_df.to_csv(os.environ["OMEKA_PATH"], index=False)

    # print dataframe
    print(omeka_df.head())


def gis_csv(df):
    """
    Export gis.csv
    """

    gis_df = df

    # drop items
    gis_df = gis_df.copy().dropna(
        subset=["geometry", "start_date", "end_date", "portals_url", "img_hd"]
    )

    # rename columns
    gis_df = gis_df.rename(
        columns={"start_date": "first_year", "end_date": "last_year"}
    )

    # select columns
    gis_df = gis_df[["id", "first_year", "last_year", "geometry"]]

    # save csv
    gis_df.to_csv(os.environ["GIS_PATH"], index=False)

    # print dataframe
    print(gis_df.head())


def load(METADATA_PATH):

    # load items for dashboard
    dashboard_plot = report.update(METADATA_PATH)
    map_plot = maps.update(METADATA_PATH)

    # read metadata.csv
    export_df = pd.read_csv(
        METADATA_PATH, parse_dates=["date", "start_date", "end_date"]
    )

    # checking dates
    l = []
    for i in range(len(export_df)):
        if export_df["start_date"][i] > export_df["end_date"][i]:
            l.append(export_df["id"][i])
    # print(l)

    # export omeka-import.csv
    omeka_csv(export_df)

    # export gis-import.csv
    gis_csv(export_df)

    # export index.html
    output_file(os.environ["INDEX_PATH"], title="Situated Views")
    show(
        layout(
            [[dashboard_plot["hbar"], dashboard_plot["pie"]], [map_plot]],
            sizing_mode="stretch_both",
        )
    )


def img_to_commons(METADATA_PATH, IMAGES_PATH):

    # Get unplubished geolocated images
    final_df = pd.read_csv(METADATA_PATH)
    commons_df = pd.DataFrame(
        final_df[
            final_df["geometry"].notna()
            & final_df["img_hd"].notna()
            & final_df["wikidata_image"].isna()
        ]
    )

    # Create folder with images to be sent
    today = datetime.now()

    new_folder = IMAGES_PATH + "commons_" + today.strftime("%Y%m%d")

    Path(new_folder).mkdir(parents=True, exist_ok=True)

    for id in commons_df["id"]:
        shutil.copy2(f"./images/jpeg-hd/{id}.jpg", new_folder)


if __name__ == "__main__":
    load(os.environ["METADATA_PATH"])
