import os
import shutil
from datetime import datetime as dt
from pathlib import Path

import geojson
import numpy as np
import pandas as pd
from bokeh.layouts import column, layout
from bokeh.plotting import output_file, show
from geomet import wkt

from maps import update as maps_update
from report import update as report_update


def img_to_commons(METADATA, IMAGES):

    # Get unplubished geolocated images
    final_df = pd.read_csv(METADATA)
    commons_df = pd.DataFrame(
        final_df[
            final_df["geometry"].notna()
            & final_df["img_hd"].notna()
            & final_df["wikidata_image"].isna()
        ]
    )

    # Create folder with images to be sent
    today = dt.now()

    new_folder = IMAGES + "commons_" + today.strftime("%Y%m%d")

    Path(new_folder).mkdir(parents=True, exist_ok=True)

    for id in commons_df["id"]:
        shutil.copy2(f"./images/jpeg-hd/{id}.jpg", new_folder)


def omeka_csv(df):
    """
    Export omeka.csv
    """

    # read final dataframe
    omeka_df = df.copy()

    # datetime to string according to date accuracy
    omeka_df.loc[omeka_df["date_accuracy"] == "day", "dcterms:created"] = omeka_df[
        "date"
    ].dt.strftime("%Y-%m-%d")
    omeka_df.loc[omeka_df["date_accuracy"] == "month", "dcterms:created"] = omeka_df[
        "date"
    ].dt.strftime("%Y-%m")
    omeka_df.loc[omeka_df["date_accuracy"] == "year", "dcterms:created"] = omeka_df[
        "date"
    ].dt.strftime("%Y")
    # omeka_df.loc[omeka_df["date_accuracy"] == "circa", "dcterms:created"] = np.nan
    omeka_df["start_date"] = omeka_df["start_date"].dt.strftime("%Y")
    omeka_df["end_date"] = omeka_df["end_date"].dt.strftime("%Y")
    omeka_df.loc[omeka_df["date_accuracy"] == "circa", "interval"] = (
        omeka_df["start_date"] + "/" + omeka_df["end_date"]
    )

    #
    omeka_df["dcterms:available"] = omeka_df["interval"]
    omeka_df.loc[~(omeka_df["date_accuracy"] == "circa"), "dcterms:available"] = (
        omeka_df["start_date"] + "/" + omeka_df["end_date"]
    )

    # format data
    omeka_df["portals_url"] = omeka_df["portals_url"] + " Instituto Moreira Salles"
    omeka_df["wikidata_id"] = omeka_df["wikidata_id"] + " Wikidata"
    omeka_df["image_width"] = omeka_df["image_width"].str.replace(",", ".")
    omeka_df["image_height"] = omeka_df["image_height"].str.replace(",", ".")

    # create columns
    omeka_df["rights"] = ""
    omeka_df["citation"] = ""
    omeka_df["item_sets"] = "all||views"
    smapshot = pd.read_csv("data-out/smapshot.csv")
    include = omeka_df["id"].isin(smapshot["id"])
    omeka_df.loc[include, "item_sets"] = omeka_df["item_sets"] + "||smapshot"

    # rename columns
    omeka_df = omeka_df.rename(
        columns={
            "id": "dcterms:identifier",
            "title": "dcterms:title",
            "description": "dcterms:description",
            "creator": "dcterms:creator",
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

    # save csv
    omeka_df.to_csv(os.environ["IMPORT_OMEKA"], index=False)

    # print dataframe
    print(omeka_df.head())


def gis_csv(df):
    """
    Export gis.csv
    """

    gis_df = df.copy()

    # rename columns
    gis_df = gis_df.rename(
        columns={"start_date": "first_year", "end_date": "last_year"}
    )

    # select columns
    gis_df = gis_df[["id", "first_year", "last_year", "geometry"]]

    # date formatting
    gis_df["first_year"] = gis_df["first_year"].dt.strftime("%Y")
    gis_df["last_year"] = gis_df["last_year"].dt.strftime("%Y")

    # to geojson
    feature_list = []

    for _, row in gis_df.iterrows():
        wkt_string = row["geometry"]
        geojson_string = wkt.loads(wkt_string)
        feature = geojson.Feature(
            id=row["id"],
            geometry=geojson_string,
            properties={
                "id": row["id"],
                "first_year": row["first_year"],
                "last_year": row["last_year"],
            },
        )
        feature_list.append(feature)

    feature_collection = geojson.FeatureCollection(feature_list)

    # save geojson
    with open("data-out/import-gis.geojson", "w", encoding="utf-8") as f:
        geojson.dump(feature_collection, f, ensure_ascii=False, indent=4)

    # print dataframe
    print(gis_df.head())


def quickstate_csv(df):
    """
    Export CSV file for QuickStatements import on Wikidata
    """

    quickstate = pd.DataFrame(
        columns=[
            "qid",
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

    # pt-br label
    quickstate["Lpt-br"] = df["title"]
    # pt-br description
    quickstate["Dpt-br"] = "Fotografia de " + df["creator"]
    # en description
    quickstate["Den"] = "Photograph by " + df["creator"]
    # Instance of
    quickstate["P31"] = "Q125191"
    # inception
    quickstate["P571"] = df["date"].apply(dt.isoformat)
    quickstate.loc[circa, "P571"] = quickstate["P571"] + "Z/8"
    quickstate.loc[year, "P571"] = quickstate["P571"] + "Z/9"
    quickstate.loc[month, "P571"] = quickstate["P571"] + "Z/10"
    quickstate.loc[day, "P571"] = quickstate["P571"] + "Z/11"
    # earliest date
    quickstate.loc[circa, "qal1319"] = df["start_date"].apply(dt.isoformat) + "Z/9"
    # latest date
    quickstate.loc[circa, "qal1326"] = df["end_date"].apply(dt.isoformat) + "Z/9"
    # country
    quickstate["P17"] = "Q155"
    # coordinate of POV
    quickstate["P1259"] = "@" + df["lat"].astype(str) + "/" + df["lng"].astype(str)
    # altitude
    quickstate["qal2044"] = df["height"].astype(str) + "U11573"
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

    # material
    paper = quickstate["P186"].str.contains("Papel", na=False)
    glass = quickstate["P186"].str.contains("Vidro", na=False)
    quickstate.loc[paper, "P186"] = "Q11472"
    quickstate.loc[glass, "P186"] = "Q11469"

    # fabrication method
    gelatin = quickstate["P2079"].str.contains("GELATINA", na=False)
    albumin = quickstate["P2079"].str.contains("ALBUMINA", na=False)
    quickstate.loc[gelatin, "P2079"] = "Q172984"
    quickstate.loc[albumin, "P2079"] = "Q580807"

    # creator
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

    quickstate.to_csv((os.environ["IMPORT_WIKI"]), index=False)

    print(quickstate.head())


def vikus_csv(df):
    """
    Export csv for Vikus Viewer
    """

    vikus_df = df.copy()

    vikus_df["portals_url"] = (
        '<a href="' + vikus_df["portals_url"] + ">Instituto Moreira Salles</a>"
    )
    vikus_df.loc[vikus_df["date_accuracy"] == "circa", "_date"] = (
        vikus_df["start_date"].dt.strftime("%Y")
        + "/"
        + vikus_df["end_date"].dt.strftime("%Y")
    )
    vikus_df.loc[~(vikus_df["date_accuracy"] == "circa"), "_date"] = vikus_df[
        "date"
    ].dt.strftime("%d-%m-%Y")
    vikus_df["date"] = vikus_df["date"].dt.strftime("%Y")
    vikus_df["creator"] = vikus_df["creator"] + "," + vikus_df["type"]
    vikus_df["wikidata_depict"] = vikus_df["wikidata_depict"].str.split(r"\|\|")
    vikus_df = vikus_df.explode(column="wikidata_depict")
    vikus_df["wikidata_depict"] = vikus_df["wikidata_depict"].str.split(" ", 1)
    has_depicts = vikus_df["wikidata_depict"].notna()

    def href(valuelist):
        return '<a href="' + valuelist[0] + ">" + valuelist[1] + "</a>"

    vikus_df.loc[has_depicts, "wikidata_depict"] = vikus_df.loc[
        has_depicts, "wikidata_depict"
    ].apply(href)
    vikus_df = vikus_df.groupby("id", as_index=False).agg(lambda x: set(x))
    vikus_df = vikus_df.applymap(lambda x: str(x).strip("{'}"))

    vikus_df = vikus_df[
        [
            "id",
            "title",
            "description",
            "creator",
            "date",
            "_date",
            "portals_url",
            "wikidata_depict",
            "image_width",
            "image_height",
        ]
    ]

    vikus_df = vikus_df.rename(
        columns={
            "date": "year",
            "creator": "keywords",
            "title": "_title",
            "description": "_description",
            "portals_url": "_source",
            "wikidata_depict": "_depicts",
            "image_width": "_width",
            "image_height": "_height",
        }
    )

    vikus_df.to_csv(os.environ["IMPORT_VIKUS"])
    print(vikus_df.head())


def img_to_commons(METADATA, IMAGES):

    # Get unplubished geolocated images
    final_df = pd.read_csv(METADATA)
    commons_df = pd.DataFrame(
        final_df[
            final_df["geometry"].notna()
            & final_df["img_hd"].notna()
            & final_df["wikidata_image"].isna()
        ]
    )

    # Create folder with images to be sent
    today = datetime.now()

    new_folder = IMAGES + "commons_" + today.strftime("%Y%m%d")

    Path(new_folder).mkdir(parents=True, exist_ok=True)

    for id in commons_df["id"]:
        shutil.copy2(f"./images/jpeg-hd/{id}.jpg", new_folder)


def load(METADATA):

    # read metadata.csv
    export_df = pd.read_csv(METADATA, parse_dates=["date", "start_date", "end_date"])

    # checking dates
    l = []
    for i in range(len(export_df)):
        if export_df["start_date"][i] > export_df["end_date"][i]:
            l.append(export_df["id"][i])
    # print(l)

    # filter items
    export_df = export_df.copy().dropna(
        subset=["geometry", "start_date", "end_date", "portals_url", "img_hd"]
    )

    # export import-omeka.csv
    omeka_csv(export_df)

    # export import-gis.csv
    gis_csv(export_df)

    # export import-wiki.csv
    quickstate_csv(export_df)

    # export import-vikus.csv
    vikus_csv(export_df)

    # load items for dashboard
    dashboard_plot = report_update(METADATA)
    map_plot = maps_update(METADATA)

    # export graphs.html
    output_file(os.environ["INDEX"], title="Situated Views - Progress Dashboard")
    show(
        layout(
            [[dashboard_plot["hbar"], dashboard_plot["pie"]], [map_plot["map"]]],
            sizing_mode="stretch_both",
        )
    )

    # export tiles.html
    output_file(os.environ["TILES"], title="Situated Views - Item heatmap")
    show(dashboard_plot["tiles"])

    # export index.html
    output_file(os.environ["SEARCH"], title="Situated Views - Search by ID")
    show(map_plot["search"])


if __name__ == "__main__":
    load(os.environ["METADATA"])

