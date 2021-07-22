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
    CheckboxButtonGroup,
    CustomJS,
    LinearColorMapper,
    Row,
    HoverTool,
    Span,
)


@dg.solid(input_defs=[dg.InputDefinition("metadata", root_manager_key="metadata_root")])
def load_metadata(_, metadata):

    metadata["Date"] = pd.to_datetime(metadata["Date"])
    metadata["First Year"] = pd.to_datetime(metadata["First Year"])
    metadata["Last Year"] = pd.to_datetime(metadata["Last Year"])
    export_df = metadata

    return export_df


@dg.solid(input_defs=[dg.InputDefinition("smapshot", root_manager_key="smapshot_root")])
def organize_columns_to_omeka(_, df, smapshot):
    # filter items
    df = df.dropna(
        subset=["geometry", "First Year", "Last Year", "Source URL", "Media URL"]
    )

    # format data
    omeka_df = df
    omeka_df["Source URL"] = omeka_df["Source URL"] + " Instituto Moreira Salles"
    omeka_df["Wikidata ID"] = omeka_df["Wikidata ID"] + " Wikidata"
    omeka_df["image_width"] = omeka_df["image_width"].str.replace(",", ".")
    omeka_df["image_height"] = omeka_df["image_height"].str.replace(",", ".")

    # create columns
    omeka_df["dcterms:type:pt"] = ""
    omeka_df["dcterms:type:en"] = ""
    omeka_df["dcterms:medium:pt"] = ""
    omeka_df["dcterms:medium:en"] = ""
    omeka_df["rights"] = ""
    omeka_df["citation"] = ""
    omeka_df["item_sets"] = "all||views"
    include = omeka_df["Source ID"].isin(smapshot["Source ID"])
    omeka_df.loc[include, "item_sets"] = omeka_df["item_sets"] + "||smapshot"
    omeka_df["dcterms:available"] = df["date_circa"]
    omeka_df.loc[~(df["date_accuracy"] == "circa"), "dcterms:available"] = (
        df["First Year"].astype(str) + "/" + df["Last Year"].astype(str)
    )

    # format data
    omeka_df["Source URL"] = omeka_df["Source URL"] + " Instituto Moreira Salles"
    omeka_df["Wikidata ID"] = omeka_df["Wikidata ID"] + " Wikidata"
    omeka_df["image_width"] = omeka_df["image_width"].str.replace(",", ".")
    omeka_df["image_height"] = omeka_df["image_height"].str.replace(",", ".")

    # omeka_df.loc[
    #     omeka_df["Materials"] == "FOTOGRAFIA/ Papel",
    #     ("dcterms:type:pt", "dcterms:type:en"),
    # ] = (
    #     "wikidata.org/wiki/Q56055236 Fotografia em papel",
    #     "wikidata.org/wiki/Q56055236 Photographic print",
    # )
    # omeka_df.loc[
    #     omeka_df["Materials"] == "REPRODUÇÃO FOTOMECÂNICA/ Papel",
    #     ("dcterms:type:pt", "dcterms:type:en"),
    # ] = (
    #     "wikidata.org/wiki/Q100575647 Impressão fotomecânica",
    #     "wikidata.org/wiki/Q100575647 Photomechanical print",
    # )
    # omeka_df.loc[
    #     omeka_df["Materials"] == "NEGATIVO/ Vidro",
    #     ("dcterms:type:pt", "dcterms:type:en"),
    # ] = (
    #     "wikidata.org/wiki/Q85621807 Negativo de vidro",
    #     "wikidata.org/wiki/Q85621807 Glass plate negative",
    # )
    # omeka_df.loc[
    #     omeka_df["Materials"] == "DIAPOSITIVO/ Vidro",
    #     ("dcterms:type:pt", "dcterms:type:en"),
    # ] = (
    #     "wikidata.org/wiki/Q97570383 Diapositivo de vidro",
    #     "wikidata.org/wiki/Q97570383 Glass diapositive",
    # )

    # filter = (omeka_df["format"] == "Estereoscopia") & (
    #     omeka_df["Materials"] == "DIAPOSITIVO/ Vidro"
    # )
    # omeka_df.loc[filter, ("dcterms:type:pt", "dcterms:type:en")] = (
    #     "wikidata.org/wiki/Q97570383 Diapositivo de vidro||wikidata.org/wiki/Q35158 Estereoscopia",
    #     "wikidata.org/wiki/Q97570383 Glass diapositive||wikidata.org/wiki/Q35158 Stereoscopy",
    # )

    # omeka_df.loc[
    #     omeka_df["Fabrication Method"] == "AUTOCHROME / Corante e prata",
    #     ("dcterms:medium:pt", "dcterms:medium:en"),
    # ] = ("wikidata.org/wiki/Q355674 Autocromo", "wikidata.org/wiki/Q355674 Autochrome")
    # omeka_df.loc[
    #     omeka_df["Fabrication Method"] == "ALBUMINA/ Prata",
    #     ("dcterms:medium:pt", "dcterms:medium:en"),
    # ] = (
    #     "wikidata.org/wiki/Q107387614 Albumina",
    #     "wikidata.org/wiki/Q107387614 Albumine",
    # )
    # omeka_df.loc[
    #     omeka_df["Fabrication Method"] == "GELATINA/ Prata",
    #     ("dcterms:medium:pt", "dcterms:medium:en"),
    # ] = (
    #     "wikidata.org/wiki/Q172984 Gelatina e prata",
    #     "wikidata.org/wiki/Q172984 Silver gelatin",
    # )
    # omeka_df.loc[
    #     omeka_df["Fabrication Method"] == "COLÓDIO/ Prata",
    #     ("dcterms:medium:pt", "dcterms:medium:en"),
    # ] = ("wikidata.org/wiki/Q904614 Colódio", "wikidata.org/wiki/Q904614 Collodion")
    # omeka_df.loc[
    #     omeka_df["Fabrication Method"] == "LANTERN SLIDE / Prata",
    #     ("dcterms:medium:pt", "dcterms:medium:en"),
    # ] = ("wikidata.org/wiki/Q87714739", "wikidata.org/wiki/Q87714739")

    # rename columns
    omeka_df = omeka_df.rename(
        columns={
            "Source ID": "dcterms:identifier",
            "Title": "dcterms:Title",
            "Description (Portuguese)": "dcterms:description",
            "Creator": "dcterms:creator",
            "date_created": "dcterms:created",
            "date_circa": "dcterms:temporal",
            "image_width": "schema:width",
            "image_height": "schema:height",
            "rights": "dcterms:rights",
            "citation": "dcterms:bibliographicCitation",
            "Source URL": "dcterms:source",
            "Wikidata ID": "dcterms:hasVersion",
            "geometry": "schema:polygon",
            "Depicts": "foaf:depicts",
            "Media URL": "media",
        }
    )

    # select columns
    omeka_df = omeka_df[
        [
            "dcterms:identifier",
            "dcterms:Title",
            "dcterms:description",
            "dcterms:creator",
            "dcterms:created",
            "dcterms:temporal",
            "dcterms:available",
            # "dcterms:type:pt",
            # "dcterms:type:en",
            # "dcterms:medium:pt",
            # "dcterms:medium:en",
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
        subset=["geometry", "First Year", "Last Year", "Source URL", "Media URL"]
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
            "format",
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

    quickstate["P571"] = df["Date"].apply(dt.isoformat)
    quickstate.loc[circa, "P571"] = quickstate["P571"] + "Z/8"
    quickstate.loc[year, "P571"] = quickstate["P571"] + "Z/9"
    quickstate.loc[month, "P571"] = quickstate["P571"] + "Z/10"
    quickstate.loc[day, "P571"] = quickstate["P571"] + "Z/11"
    # earliest date
    quickstate.loc[circa, "qal1319"] = df["First Year"].apply(dt.isoformat) + "Z/9"
    # latest date
    quickstate.loc[circa, "qal1326"] = df["Last Year"].apply(dt.isoformat) + "Z/9"
    # pt-br label
    quickstate["Lpt-br"] = df["Title"]
    # pt-br description
    quickstate["Dpt-br"] = "Fotografia de " + df["Creator"]
    # en description
    quickstate["Den"] = "Photograph by " + df["Creator"]
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
    quickstate["P170"] = df["Creator"]
    # made from material
    quickstate["P186"] = df["Materials"]
    # format
    quickstate["format"] = df["format"]
    # collection
    quickstate["P195"] = "Q71989864"
    # inventory number
    quickstate["P217"] = df["Source ID"]
    # fabrication method
    quickstate["P2079"] = df["Fabrication Method"]
    # field of view
    quickstate["P4036"] = df["fov"].astype(str) + "U28390"
    # width
    quickstate["P2049"] = df["image_width"].str.replace(",", ".") + "U174728"
    # height
    quickstate["P2048"] = df["image_height"].str.replace(",", ".") + "U174728"
    # IMS ID
    quickstate["P7835"] = df["portals_id"].astype(int)
    # qid
    quickstate["qid"] = df["Wikidata ID"]
    # Copyright status
    # quickstate["P6216"]

    # format data P186
    quickstate.loc[quickstate["P186"] == "FOTOGRAFIA/ Papel", "P186"] = "Q56055236"
    quickstate.loc[
        quickstate["P186"] == "REPRODUÇÃO FOTOMECÂNICA/ Papel", "P186"
    ] = "Q100575647"
    quickstate.loc[quickstate["P186"] == "NEGATIVO/ Vidro", "P186"] = "Q85621807"
    quickstate.loc[quickstate["P186"] == "DIAPOSITIVO/ Vidro", "P186"] = "Q97570383"
    filter = (quickstate["format"] == "Estereoscopia") & (
        quickstate["P186"] == "Q97570383"
    )
    quickstate.loc[filter, "P186"] = "Q97570383||Q35158"

    # format data P2079
    quickstate.loc[
        quickstate["P2079"] == "AUTOCHROME / Corante e prata", "P2079"
    ] = "Q355674"
    quickstate.loc[quickstate["P2079"] == "ALBUMINA/ Prata", "P2079"] = "Q107387614"
    quickstate.loc[quickstate["P2079"] == "GELATINA/ Prata", "P2079"] = "Q172984"
    quickstate.loc[quickstate["P2079"] == "COLÓDIO/ Prata", "P2079"] = "Q904614"
    quickstate.loc[
        quickstate["P2079"] == "LANTERN SLIDE / Prata", "P2079"
    ] = "Q87714739"

    quickstate = quickstate.drop(["format"], axis=1)

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

    values_tiles = DF[
        [
            "Source ID",
            "img_sd",
            "Media URL",
            "latitude",
            "longitude",
            "geometry",
            "portals_id",
            "Source URL",
            "Wikidata ID",
            "omeka_url",
        ]
    ]

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
    val_img = len(DF[DF["Media URL"].notna()])
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
    val_wiki_total = len(DF[DF["Wikidata ID"].notna()])
    DF_AUX["D"] = DF["Wikidata ID"].notna().astype(int)

    # omeka published
    val_omeka = len(DF[DF["omeka_url"].notna()])
    DF_AUX["E"] = DF["omeka_url"].notna().astype(int)
    # omeka total
    val_omeka_total = 0

    # count fields for each item and add result as column
    DF_AUX = DF_AUX.replace(0, np.nan)
    values_tiles["rate"] = DF_AUX.count(axis=1)

    values_hbar = {
        "Done_orange": [0, val_wiki, val_meta, 0, 0],
        "Done_blue": [val_omeka, 0, 0, val_img, val_kml],
        "To do": [
            val_omeka_total,
            val_wiki_total - val_wiki,
            val_meta_total - val_meta,
            val_img_total - val_img,
            val_kml_total,
        ],
        "y": ["Omeka-S", "Wikimedia", "Cumulus", "HiRes Images", "KML"],
    }

    values_pie = [val_omeka, val_wiki, val_meta, val_img, val_kml]

    values = [values_hbar, values_pie, values_tiles]

    return values


@dg.solid
def create_hbar(context, values):

    # construct a data source
    list1 = ["Done_orange", "Done_blue", "To do"]
    values_hbar = values[0]
    # deepcopy the data for later use
    values_hbar1 = deepcopy(values_hbar)

    values_hbar1.update(
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
        values_hbar[list1[i]] = [
            sum(x) for x in zip(values_hbar[list1[i]], values_hbar[list1[i - 1]])
        ]

    plot_hbar = figure(
        y_range=values_hbar["y"],
        x_range=(0, 6000),
        plot_height=300,
        plot_width=900,
        toolbar_location=None,
    )

    # construct bars with differents colors
    hbar_1 = plot_hbar.hbar(
        y=values_hbar["y"],
        right=values_hbar["Done_orange"],
        left=0,
        height=0.8,
        color="orange",
    )
    hbar_1.data_source.add(values_hbar1["tooltip_b-o"], "data")
    hbar_1.data_source.add(values_hbar1["Done_orange"], "value")

    hbar_2 = plot_hbar.hbar(
        y=values_hbar["y"],
        right=values_hbar["Done_blue"],
        left=values_hbar["Done_orange"],
        height=0.8,
        color="royalblue",
    )
    hbar_2.data_source.add(values_hbar1["tooltip_b-o"], "data")
    hbar_2.data_source.add(values_hbar1["Done_blue"], "value")

    hbar_3 = plot_hbar.hbar(
        y=values_hbar["y"],
        right=values_hbar["To do"],
        left=values_hbar["Done_blue"],
        height=0.8,
        color="lightgrey",
    )
    hbar_3.data_source.add(values_hbar1["tooltip_grey"], "data")
    hbar_3.data_source.add(values_hbar1["To do"], "value")

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
def create_pie(context, values):
    # construct a data source
    values_pie = values[1]
    total = 20000
    s = sum(values_pie)
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


@dg.solid
def create_tiles(context, values):
    values_tiles = values[2]

    # construct coordinates
    coord = []
    for y in range(0, 52):
        for x in range(0, 100):
            if len(coord) != len(values_tiles):
                coord.append([(x, y)])
            else:
                break

    df_coord = pd.DataFrame(coord, columns=["coordinate"])
    df_coord[["x", "y"]] = pd.DataFrame(
        df_coord["coordinate"].tolist(), index=df_coord.index
    )
    df_coord = df_coord.drop(columns="coordinate")

    df_tiles = pd.merge(
        values_tiles, df_coord, left_index=True, right_index=True, validate="one_to_one"
    )
    df_tiles_s = df_tiles.iloc[:, [0, 1, 2, 3, 4, 4, 5, 6, 7, 8, 9, 10]].sort_values(
        by=["rate"], ascending=False, ignore_index=True
    )
    df_tiles_sort = df_tiles_s.join(df_tiles[["x", "y"]])

    # setting colors
    colors = ["#edf8e9", "#c7e9c0", "#a1d99b", "#74c476", "#31a354", "#006d2c"]
    mapper = LinearColorMapper(
        palette=colors, low=values_tiles.rate.min(), high=values_tiles.rate.max()
    )

    # config tooltip
    TOOLTIPS = """
        <div style="margin: 5px; width: 400px" >
        <h3 
            style='font-size: 12px; font-weight: bold;'>
            @id
        </h3>
        <p 
            style='font-size: 10px; font-weight: bold;'>
            Geolocated: (@latitude,@longitude)
        </p>
        <p 
            style='font-size: 10px; font-weight: bold;'>
            Image: @img_sd @Media URL
        </p>
        <p 
            style='font-size: 10px; font-weight: bold;'>
            Portals: @Source URL
        </p>
        <p 
            style='font-size: 10px; font-weight: bold;'>
            Wikimedia: @wikidata_id
        </p>
        <p 
            style='font-size: 10px; font-weight: bold;'>
            Omeka-S: @omeka_url
        </p>
        <img
            src="@img_sd" alt="@img_sd" height=200
            style="margin: 0px;"
            border="2"
            ></img>        
        </div>
        """

    # construct base chart
    tiles = figure(
        x_axis_type=None,
        y_axis_type=None,
        plot_width=1500,
        plot_height=1000,
        min_border_bottom=150,
        min_border_top=150,
        toolbar_location=None,
    )

    # create tiles
    rect_sort = tiles.rect(
        x="x",
        y="y",
        width=0.8,
        height=0.8,
        fill_color={"field": "rate", "transform": mapper},
        line_color="black",
        line_join="round",
        line_width=1,
        source=df_tiles_sort,
    )

    rect = tiles.rect(
        x="x",
        y="y",
        width=0.8,
        height=0.8,
        fill_color={"field": "rate", "transform": mapper},
        line_color="black",
        line_join="round",
        line_width=1,
        source=df_tiles,
    )

    # add tooltip in hover
    h1 = HoverTool(renderers=[rect], tooltips=TOOLTIPS, mode="mouse", show_arrow=False)
    h2 = HoverTool(
        renderers=[rect_sort], tooltips=TOOLTIPS, mode="mouse", show_arrow=False
    )

    callback = CustomJS(
        args={"rect": rect, "rect_sort": rect_sort},
        code="""
    rect.visible = false;
    rect_sort.visible = false;
    if (cb_obj.active.includes(0)){rect_sort.visible = true;}
    else{rect.visible = true;}
    """,
    )

    button = CheckboxButtonGroup(labels=["Sort by rate"])
    button.js_on_click(callback)

    tiles.add_tools(h1, h2)
    tiles.grid.grid_line_color = None
    tiles.axis.axis_line_color = None
    tiles.axis.major_tick_line_color = None
    tiles.toolbar.active_drag = None
    tiles.axis.major_label_standoff = 0
    tiles.y_range.flipped = True

    plot_tiles = Row(button, tiles)

    return plot_tiles


@dg.solid(config_schema=dg.StringSource)
def export_html(context, plot_hbar, plot_pie, plot_tiles):
    path = context.solid_config

    output_file(path, Title="Situated Views - Progress Dashboard")
    show(layout([[plot_hbar, plot_pie], [plot_tiles]], sizing_mode="stretch_both"))

    print("Done!")
