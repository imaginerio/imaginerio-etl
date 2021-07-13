import os
import shutil
from datetime import datetime as dt
from pathlib import Path
from copy import deepcopy

import dagster as dg
import geojson
import numpy as np
import pandas as pd

from dagster.core.definitions import solid
from bokeh.layouts import layout
from bokeh.transform import cumsum
from bokeh.plotting import output_file, show, figure
from bokeh.models import (
    HoverTool,
    Span
)


@dg.solid(input_defs=[dg.InputDefinition("metadata", root_manager_key="metadata_root")])
def load_metadata(_, metadata):

    metadata["date"] = pd.to_datetime(metadata["date"])
    metadata["first_year"] = pd.to_datetime(metadata["first_year"])
    metadata["last_year"] = pd.to_datetime(metadata["last_year"])
    export_df = metadata

    return export_df

@dg.solid(input_defs=[dg.InputDefinition("smapshot", root_manager_key="smapshot_root")])
def organize_columns_to_omeka(_, df, smapshot):
    # filter items
    df = df.dropna(
        subset=["geometry", "first_year", "last_year", "portals_url", "img_hd"]
    )

    # format data
    omeka_df = df
    omeka_df["portals_url"] = omeka_df["portals_url"] + " Instituto Moreira Salles"
    omeka_df["wikidata_id"] = omeka_df["wikidata_id"] + " Wikidata"
    omeka_df["image_width"] = omeka_df["image_width"].str.replace(",", ".")
    omeka_df["image_height"] = omeka_df["image_height"].str.replace(",", ".")

    # create columns
    omeka_df["rights"] = ""
    omeka_df["citation"] = ""
    omeka_df["item_sets"] = "all||views"
    include = omeka_df["id"].isin(smapshot["id"])
    omeka_df.loc[include, "item_sets"] = omeka_df["item_sets"] + "||smapshot"
    omeka_df["dcterms:available"] = df["date_circa"]
    omeka_df.loc[~(df["date_accuracy"] == "circa"), "dcterms:available"] = (
        df["first_year"].astype(str) + "/" + df["last_year"].astype(str)
    )

    # rename columns
    omeka_df = omeka_df.rename(
        columns={
            "id": "dcterms:identifier",
            "title": "dcterms:title",
            "description": "dcterms:description",
            "creator": "dcterms:creator",
            "date_created": "dcterms:created",
            "date_circa": "dcterms:temporal",
            "type": "dcterms:type",
            "image_width": "schema:width",
            "image_height": "schema:height",
            "rights": "dcterms:rights",
            "citation": "dcterms:bibliographicCitation",
            "portals_url": "dcterms:source",
            "wikidata_id": "dcterms:hasVersion",
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

    return omeka_df


@dg.solid(
    input_defs=[dg.InputDefinition("jstor", root_manager_key="jstor_root")],
    output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="import_omeka")],
)
def import_omeka_dataframe(_, df, jstor):
    # append JSTOR migration
    omeka_df = df.append(jstor)
    omeka_df.name = "import_omeka"

    return omeka_df.set_index("dcterms:identifier")


@dg.solid
def make_df_to_wikidata(_, df):

    # filter items
    df = df.dropna(
        subset=["geometry", "first_year", "last_year", "portals_url", "img_hd"]
    )

    quickstate = pd.DataFrame(
        columns=[
            "Qid",
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

    quickstate["P571"] = df["date"].apply(dt.isoformat)
    quickstate.loc[circa, "P571"] = quickstate["P571"] + "Z/8"
    quickstate.loc[year, "P571"] = quickstate["P571"] + "Z/9"
    quickstate.loc[month, "P571"] = quickstate["P571"] + "Z/10"
    quickstate.loc[day, "P571"] = quickstate["P571"] + "Z/11"
    # earliest date
    quickstate.loc[circa, "qal1319"] = df["first_year"].apply(dt.isoformat) + "Z/9"
    # latest date
    quickstate.loc[circa, "qal1326"] = df["last_year"].apply(dt.isoformat) + "Z/9"
    # pt-br label
    quickstate["Lpt-br"] = df["title"]
    # pt-br description
    quickstate["Dpt-br"] = "Fotografia de " + df["creator"]
    # en description
    quickstate["Den"] = "Photograph by " + df["creator"]
    # Instance of
    quickstate["P31"] = "Q125191"
    # country
    quickstate["P17"] = "Q155"
    # coordinate of POV
    quickstate["P1259"] = (
        "@" + df["latitude"].astype(str) + "/" + df["longitude"].astype(str)
    )
    # altitude
    quickstate["qal2044"] = df["altitude"].astype(str) + "U11573"
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

    paper = quickstate["P186"].str.contains("Papel", na=False)
    glass = quickstate["P186"].str.contains("Vidro", na=False)
    quickstate.loc[paper, "P186"] = "Q11472"
    quickstate.loc[glass, "P186"] = "Q11469"

    # fabrication method
    gelatin = quickstate["P2079"].str.contains("GELATINA", na=False)
    albumin = quickstate["P2079"].str.contains("ALBUMINA", na=False)
    quickstate.loc[gelatin, "P2079"] = "Q172984"
    quickstate.loc[albumin, "P2079"] = "Q580807"

    return quickstate


@dg.solid(
    output_defs=[
        dg.OutputDefinition(io_manager_key="pandas_csv", name="import_wikidata")
    ]
)
def organise_creator(_, quickstate):
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
    quickstate.name = "mport_wikidata"

    return quickstate.set_index("qid")

@dg.solid
def format_values_chart(context, DF):
    
    DF["img_sd"] = DF["img_sd"].str.strip('"')

    # create a new df
    DF_AUX = pd.DataFrame(columns=["A", "B", "C", "D", "E"])

    # add items in global variables and new dataframe
    # kml finished
    val_kml = len(DF[DF["geometry"].notna()])
    DF_AUX["A"] = DF["geometry"].notna().astype(int)
    # kml total
    val_kml_total = 0

    # image finished
    val_img = len(DF[DF["img_hd"].notna()])
    DF_AUX["B"] = DF["img_sd"].notna().astype(int)
    # image total
    val_img_total = len(DF[DF["img_sd"].notna()])

    # cumulus published
    val_meta = len(DF[DF["portals_id"].notna()])
    DF_AUX["C"] = DF["portals_id"].notna().astype(int)
    # cumulus total
    val_meta_total = len(DF)

    # wiki published
    val_wiki = len(DF[DF["wikidata_image"].notna()])
    # wiki total
    val_wiki_total = len(DF[DF["wikidata_id"].notna()])
    DF_AUX["D"] = DF["wikidata_id"].notna().astype(int)

    # omeka published
    val_omeka = len(DF[DF["omeka_url"].notna()])
    DF_AUX["E"] = DF["omeka_url"].notna().astype(int)
    # omeka total
    val_omeka_total = 0

    values_hbar = {
        "Done_orange": [ 
            0,
            val_wiki, 
            val_meta,
            0,
            0
            ],
        "Done_blue": [
            val_omeka,
            0,
            0, 
            val_img, 
            val_kml
            ],
        "To do": [
            val_omeka_total,
            val_wiki_total - val_wiki,
            val_meta_total - val_meta,
            val_img_total - val_img,
            val_kml_total,
        ],
        "y": ["Omeka-S", "Wikimedia", "Cumulus", "HiRes Images", "KML"],
    }

    values_pie = {
        "Done": [
            val_omeka,
            val_wiki, 
            val_meta, 
            val_img, 
            val_kml
            ],
        "To do": [
            val_omeka_total,
            val_wiki_total - val_wiki,
            val_meta_total - val_meta,
            val_img_total - val_img,
            val_kml_total,
        ],
        "y": ["Omeka-S", "Wikimedia", "Cumulus", "HiRes Images", "KML"],
    }

    values = [values_hbar,values_pie]
    
    return values 

@dg.solid
def create_hbar(context, values_hbar):

    # construct a data source
    list1 = ["Done_orange","Done_blue", "To do"]
    data = values_hbar
    # deepcopy the data for later use
    data1 = deepcopy(data)

    data1.update(
        {
            "tooltip_grey": [
                "Not on Omeka",
                "Wikidata only",
                "Potential items",
                "To geolocate",
                "Not geolocated",
            ],
            "tooltip_b-o": [
                "Published",
                "Commons and Wikidata",
                "On IMS' Cumulus Portals",
                "Geolocated",
                "Total geolocated items",
            ],
        }
    )

    # base dashboard
    for i in range(1, len(list1)):
        data[list1[i]] = [sum(x) for x in zip(data[list1[i]], data[list1[i - 1]])]

    plot_hbar = figure(
        y_range=data["y"],
        x_range=(0, 6000),
        plot_height=300,
        plot_width=900,
        toolbar_location=None,
    )

    # construct bars with differents colors
    hbar_1 = plot_hbar.hbar(
        y=data["y"],
        right=data["Done_orange"],
        left=0,
        height=0.8,
        color="orange"
    )
    hbar_1.data_source.add(data1["tooltip_b-o"], "data")
    hbar_1.data_source.add(data1["Done_orange"], "value")

    hbar_2 = plot_hbar.hbar(
        y=data["y"],
        right=data["Done_blue"],
        left=data["Done_orange"],
        height=0.8,
        color="royalblue"
    )
    hbar_2.data_source.add(data1["tooltip_b-o"], "data")
    hbar_2.data_source.add(data1["Done_blue"], "value")

    hbar_3 = plot_hbar.hbar(
        y=data["y"],
        right=data["To do"],
        left=data["Done_blue"],
        height=0.8,
        color="lightgrey",
    )
    hbar_3.data_source.add(data1["tooltip_grey"], "data")
    hbar_3.data_source.add(data1["To do"], "value")

    # add hover tool for each bar chart
    TOOLTIPS = "@data: @value"
    h1 = HoverTool(
        renderers=[hbar_1], tooltips=TOOLTIPS, mode="mouse", show_arrow=False
    )
    h2 = HoverTool(
        renderers=[hbar_2], tooltips=TOOLTIPS, mode="mouse", show_arrow=False
    )
    h3 = HoverTool(
        renderers=[hbar_3], tooltips=TOOLTIPS, mode="mouse", show_arrow=False
    )

    plot_hbar.add_tools(h1, h2, h3)

    # data goal line
    hline = Span(
        location=4000,
        dimension="height",
        line_color="grey",
        line_dash="dashed",
        line_width=3,
    )

    plot_hbar.add_layout(hline)
    plot_hbar.ygrid.grid_line_color = None
    plot_hbar.toolbar.active_drag = None
    plot_hbar.background_fill_color = "ghostwhite"

    return plot_hbar

@dg.solid
def create_pie(context, values_pie):
    # construct a data source
    total = 20000
    s = sum(values["Done"])
    x = round((100 * s) / total, 1)

    a = {"To do": 100 - x, "Done": x}

    datas = pd.Series(a).reset_index(name="value").rename(columns={"index": "data"})
    datas["angle"] = (360 * datas["value"]) / 100
    datas["color"] = ["lightgrey", "orange"]

    # create a legend label
    sep = []
    for i in range(len(datas.index)):
        sep.append(": ")
    datas["legend"] = datas["data"] + sep + datas["value"].astype(str) + "%"

    # base pie chart
    plot_pie = figure(plot_height=100, toolbar_location=None)

    # construct wedges
    plot_pie.wedge(
        x=0,
        y=1,
        radius=0.5,
        start_angle=cumsum("angle"),
        end_angle=cumsum("angle", include_zero=True),
        start_angle_units="deg",
        end_angle_units="deg",
        legend_field="legend",
        line_color=None,
        fill_color="color",
        direction="clock",
        source=datas,
    )

    plot_pie.axis.axis_label = None
    plot_pie.axis.visible = False
    plot_pie.grid.grid_line_color = None
    plot_pie.background_fill_color = "ghostwhite"

    return plot_pie

@dg.solid(config_schema=dg.StringSource)
def export_html(context,plot_hbar,plot_pie):
    path = context.config

    output_file(path, title="Situated Views - Progress Dashboard")
    show(layout(
        [[plot_hbar,plot_pie]],
        sizing_mode="stretch_both"
    ))

    print("Done!")