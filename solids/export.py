import os
import shutil
from datetime import datetime as dt
from pathlib import Path
from copy import deepcopy

from numpy.core.numeric import NaN
import dagster_pandas as dp

import dagster as dg
import geojson
import numpy as np
import pandas as pd
from tests.dataframe_types import *
from tests.objects_types import *

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
    Title,
)


@dg.solid(
    input_defs=[
        dg.InputDefinition("metadata", root_manager_key="metadata_root"),
        dg.InputDefinition("camera", root_manager_key="camera_root"),
        dg.InputDefinition("cumulus", root_manager_key="cumulus_root"),
    ],
    output_defs=[dg.OutputDefinition(dagster_type=dp.DataFrame)],
)
def load_metadata(
    _,
    metadata: dp.DataFrame,
    camera: main_dataframe_types,
    cumulus: main_dataframe_types,
):
    """
    Merge relevant dataframes to access objects
    status and properties
    """
    camera_df = camera[["Source ID", "heading", "tilt", "altitude", "fov"]]
    cumulus_df = cumulus[["Source ID", "datetime", "date_accuracy"]]
    cumulus_df["datetime"] = pd.to_datetime(cumulus_df["datetime"])
    datas = [cumulus_df, camera_df]
    export_df = metadata

    for df in datas:
        export_df = export_df.merge(df, how="outer", on="Source ID")

    return export_df


@dg.solid(
    input_defs=[
        dg.InputDefinition("smapshot", root_manager_key="smapshot_root"),
        dg.InputDefinition("mapping", root_manager_key="mapping_root"),
    ],
    output_defs=[dg.OutputDefinition(dagster_type=dp.DataFrame)],
)
def organize_columns_to_omeka(
    _, df: dp.DataFrame, smapshot: dp.DataFrame, mapping: dp.DataFrame
):
    def string2url(string):
        if "||Stereoscopy" in string:
            string = string.split("||")[0]
            QID = mapping.loc[string, "Wiki ID"]
            return (
                f"http://wikidata.org/wiki/{QID} {string}"
                + "||http://wikidata.org/wiki/Q35158 Stereoscopy"
            )
        elif "||Estereoscopia" in string:
            string = string.split("||")[0]
            QID = mapping.loc[string, "Wiki ID"]
            return (
                f"http://wikidata.org/wiki/{QID} {string}"
                + "||http://wikidata.org/wiki/Q35158 Estereoscopia"
            )
        else:
            QID = mapping.loc[string, "Wiki ID"]
            return f"http://wikidata.org/wiki/{QID} {string}"

    def translateString(string):
        if "||Stereoscopy" in string:
            string = string.split("||")[0]
            kw = mapping.loc[string, "Label:pt"]
            return kw + "||Estereoscopia"
        else:
            kw = mapping.loc[string, "Label:pt"]
            return kw

    # filter items
    omeka_df = df.loc[
        (df["Source"] == "Instituto Moreira Salles")
        & (
            df["Latitude"].notna()
            & df["Source URL"].notna()
            & df["Media URL"].notna()
            & df["First Year"].notna()
            & df["Last Year"].notna()
        )
    ]
    omeka_df = omeka_df.dropna(subset=["Collections"])
    mapping.set_index("Label:en", inplace=True)
    omeka_df[["First Year", "Last Year"]] = omeka_df[
        ["First Year", "Last Year"]
    ].applymap(lambda x: str(int(x)), na_action="ignore")

    # create columns
    omeka_df.loc[omeka_df["First Year"].notna(), "dcterms:available"] = (
        omeka_df["First Year"].astype(str) + "/" + omeka_df["Last Year"].astype(str)
    )
    omeka_df["dcterms:format:en"] = omeka_df["Materials"]
    omeka_df["dcterms:format:pt"] = omeka_df["Materials"]
    omeka_df["dcterms:medium:en"] = omeka_df["Fabrication Method"]
    omeka_df["dcterms:medium:pt"] = omeka_df["Fabrication Method"]
    omeka_df["dcterms:type:en"] = omeka_df["Type"]
    omeka_df["dcterms:type:pt"] = omeka_df["Type"]

    # format data
    omeka_df["Source URL"] = omeka_df["Source URL"] + " " + omeka_df["Source"]
    omeka_df["Wikidata ID"] = (
        "www.wikidata.org/wiki/" + omeka_df["Wikidata ID"] + " Wikidata"
    )
    include = omeka_df["Source ID"].isin(smapshot["id"])
    omeka_df.loc[include, "Collections"] = omeka_df["Collections"] + "||Smapshot"

    omeka_df[["dcterms:format:en", "dcterms:medium:en", "dcterms:type:en"]] = omeka_df[
        ["dcterms:format:en", "dcterms:medium:en", "dcterms:type:en"]
    ].applymap(string2url, na_action="ignore")
    omeka_df[["dcterms:format:pt", "dcterms:medium:pt", "dcterms:type:pt"]] = omeka_df[
        ["dcterms:format:pt", "dcterms:medium:pt", "dcterms:type:pt"]
    ].applymap(translateString, na_action="ignore")
    mapping = mapping.reset_index()
    mapping.set_index("Label:pt", inplace=True)
    omeka_df[["dcterms:format:pt", "dcterms:medium:pt", "dcterms:type:pt"]] = omeka_df[
        ["dcterms:format:pt", "dcterms:medium:pt", "dcterms:type:pt"]
    ].applymap(string2url, na_action="ignore")

    # rename columns
    omeka_df = omeka_df.rename(
        columns={
            "Source ID": "dcterms:identifier",
            "Title": "dcterms:title",
            "Description (Portuguese)": "dcterms:description",
            "Creator": "dcterms:creator",
            "Date": "dcterms:date",
            "Width (mm)": "schema:width",
            "Height (mm)": "schema:height",
            "Rights": "dcterms:rights",
            "Attribution": "dcterms:bibliographicCitation",
            "Source URL": "dcterms:source",
            "Wikidata ID": "dcterms:hasVersion",
            "Depicts": "foaf:depicts",
            "Media URL": "media",
            "Latitude": "latitude",
            "Longitude": "longitude",
            "Collection": "item_sets",
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
            "dcterms:available",
            "dcterms:type:en",
            "dcterms:type:pt",
            "dcterms:medium:pt",
            "dcterms:medium:en",
            "dcterms:format:pt",
            "dcterms:format:en",
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
            "item_sets",
        ]
    ]

    return omeka_df


@dg.solid(
    output_defs=[
        dg.OutputDefinition(
            io_manager_key="pandas_csv", name="import_omeka", dagster_type=dp.DataFrame
        )
    ],
)
def import_omeka_dataframe(_, df: dp.DataFrame):
    omeka_df = df
    omeka_df.name = "import_omeka"

    return omeka_df.set_index("dcterms:identifier")


@dg.solid(
    input_defs=[dg.InputDefinition("mapping", root_manager_key="mapping_root")],
    output_defs=[dg.OutputDefinition(dagster_type=dp.DataFrame)],
)
def make_df_to_wikidata(_, df: dp.DataFrame, mapping: dp.DataFrame):
    def string2qid(string):
        QID = mapping.loc[string, "Wiki ID"]
        return QID

    # filter items
    df = df.loc[
        (df["Source"] == "Instituto Moreira Salles")
        & df["Latitude"].notna()
        & df["Source URL"].notna()
        & df["Media URL"].notna()
        & df["First Year"].notna()
        & df["Last Year"].notna()
        & df["Width (mm)"].notna()
        & df["Height (mm)"]
    ]
    df = df.dropna(subset=["Collections"])
    df[["First Year", "Last Year"]] = df[["First Year", "Last Year"]].applymap(
        lambda x: str(int(x)), na_action="ignore"
    )

    mapping.set_index("Label:en", inplace=True)

    df["First Year"] = pd.to_datetime(df["First Year"])
    df["Last Year"] = pd.to_datetime(df["Last Year"])

    df[["Materials", "Materials_"]] = df["Materials"].str.rsplit("||", n=1, expand=True)

    quickstate = pd.DataFrame(
        columns=[
            "qid",
            "P31",
            "P31_a",
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

    quickstate["P571"] = df["datetime"].apply(dt.isoformat)
    quickstate.loc[circa, "P571"] = "+" + quickstate["P571"] + "Z/8"
    quickstate.loc[year, "P571"] = "+" + quickstate["P571"] + "Z/9"
    quickstate.loc[month, "P571"] = "+" + quickstate["P571"] + "Z/10"
    quickstate.loc[day, "P571"] = "+" + quickstate["P571"] + "Z/11"
    # earliest date
    # quickstate["qal1319"] = df["First Year"].apply(dt.isoformat) + "Z/9"
    quickstate["P571"] = (
        quickstate["P571"] + "|P580|+" + df["First Year"].apply(dt.isoformat) + "Z/9"
        "|P582|+" + df["Last Year"].apply(dt.isoformat) + "Z/9"
    )
    # latest date
    # quickstate["qal1326"] = df["Last Year"].apply(dt.isoformat) + "Z/9"
    # pt-br label
    quickstate["Lpt-br"] = df["Title"]
    # creator
    quickstate["P170"] = df["Creator"]
    # description
    # pt-br
    quickstate["Dpt-br"] = "Fotografia de " + df["Creator"]
    # en
    quickstate["Den"] = np.where(
        df["Creator"] != "Anônimo",
        "Photograph by " + df["Creator"],
        "Photograph by Unknown",
    )

    list_creator = list(quickstate["P170"].unique())
    for author in list_creator:
        df_creator = quickstate.loc[quickstate["P170"] == author]
        duplicate = list(df_creator.duplicated(subset=["Lpt-br"]))
        df_creator.loc[duplicate, "Dpt-br"] = (
            "Fotografia de " + df_creator["P170"] + " (" + df_creator["P217"] + ")"
        )
        df_creator.loc[duplicate, "Den"] = np.where(
            df_creator.loc[duplicate, "P170"] != "Anônimo",
            "Photograph by "
            + df_creator.loc[duplicate, "P170"]
            + " ("
            + df_creator.loc[duplicate, "P217"]
            + ")",
            "Photograph by Unknown" + " (" + df_creator.loc[duplicate, "P217"] + ")",
        )
        quickstate.loc[quickstate["P170"] == author, ["Dpt-br", "Den"]] = df_creator[
            ["Dpt-br", "Den"]
        ]

    # Instance of
    quickstate["P31"] = "Q125191"
    quickstate["P31_a"] = df["Materials_"].map({"Stereoscopy": "Q35158"})
    # country
    quickstate["P17"] = "Q155"
    # coordinate of POV
    quickstate["P1259"] = (
        ("@" + df["Latitude"].astype(str) + "/" + df["Longitude"].astype(str))
        + "|P2044|"
        + df["altitude"].astype(str)
        + "U11573"
        + "|P7787|"
        + df["heading"].astype(str)
        + "U28390"
        + "|P8208|"
        + df["tilt"].astype(str)
        + "U28390"
    )
    # altitude
    # quickstate["qal2044"] = df["altitude"].astype(str) + "P11573"
    # heading
    # quickstate["qal7787"] = df["heading"].astype(str) + "P28390"
    # tilt
    # quickstate["qal8208"] = df["tilt"].astype(str) + "P28390"
    # made from material
    quickstate["P186"] = df["Materials"]
    # collection
    quickstate["P195"] = "Q71989864"
    # inventory number
    quickstate["P217"] = df["Source ID"]
    # fabrication method
    quickstate["P2079"] = df["Fabrication Method"]
    # field of view
    quickstate["P4036"] = df["fov"].astype(str) + "U28390"
    # width
    quickstate["P2049"] = df["Width (mm)"].astype(str) + "U174789"
    # height
    quickstate["P2048"] = df["Height (mm)"].astype(str) + "U174789"
    # IMS ID
    quickstate["P7835"] = df["Source URL"].str.extract(r"(\d+)").astype(int)
    # qid
    quickstate["qid"] = df["Wikidata ID"]
    # Copyright status
    # quickstate["P6216"]

    # format data P186 and P2079
    quickstate[["P186", "P2079"]] = quickstate[["P186", "P2079"]].applymap(
        string2qid, na_action="ignore"
    )

    return quickstate


@dg.solid(
    output_defs=[
        dg.OutputDefinition(
            io_manager_key="pandas_csv",
            name="import_wikidata",
            dagster_type=dp.DataFrame,
        )
    ]
)
def organise_creator(_, quickstate: dp.DataFrame):
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
        """
        Takes a string and returns the
        corresponding Wikidata QID
        """
        try:
            qid = creators[f"{name}"]
        except KeyError:
            qid = ""
        return qid

    quickstate["P170"] = quickstate["P170"].apply(name2qid)
    quickstate = quickstate.drop(columns="date_accuracy")
    quickstate.name = "import_wikidata"

    def df2quickstatements(df):
        create_str = ""
        edit_str = ""
        str_props = ["Lpt-br", "Dpt-br", "Den", "P217", "P7835"]
        no_ref_props = ["Lpt-br", "Dpt-br", "Den"]
        for _, row in df.iterrows():
            row = dict(row)
            props = []
            if row["qid"]:
                for key in row.keys():
                    if row[key]:
                        if key in str_props:
                            row[key] = '"{0}"'.format(row[key])
                        prop_str = "|".join(
                            [
                                str(row["qid"]),
                                str(key).replace("P31_a", "P31"),
                                str(row[key]),
                            ]
                        )
                        if key == "P217":
                            prop_str += "|P195|Q71989864"
                        if key == "P195":
                            prop_str += "|P217|" + '"{0}"'.format(row["P217"])
                        if key not in no_ref_props:
                            prop_str += "|S248|Q64995339|S813|+{0}Z/11".format(
                                dt.now().strftime("%Y-%m-%dT00:00:00")
                            )
                        props.append(prop_str)
                item_str = "||".join(props)
                if not edit_str:
                    edit_str += item_str
                else:
                    edit_str += "||" + item_str
            else:
                props.append("CREATE")
                for key in row.keys():
                    if row[key]:
                        if key in str_props:
                            row[key] = '"{0}"'.format(row[key])
                        prop_str = "|".join(
                            [
                                "LAST",
                                str(key).replace("P31_a", "P31"),
                                str(row[key]),
                            ]
                        )
                        if key == "P217":
                            prop_str += "|P195|Q71989864"
                        if key == "P195":
                            prop_str += "|P217|" + '"{0}"'.format(row["P217"])
                        if key not in no_ref_props:
                            prop_str += "|S248|Q64995339|S813|+{0}Z/11".format(
                                dt.now().strftime("%Y-%m-%dT00:00:00")
                            )
                        props.append(prop_str)
                item_str = "||".join(props)
                if not create_str:
                    create_str += item_str
                else:
                    create_str += "||" + item_str

        return {"create": create_str, "edit": edit_str}

    quickstate.fillna("", inplace=True)

    with open("data/output/create_quickstatements.txt", "w+") as f:
        f.write(df2quickstatements(quickstate)["create"])

    with open("data/output/edit_quickstatements.txt", "w+") as f:
        f.write(df2quickstatements(quickstate)["edit"])

    return quickstate.set_index("qid")


@dg.solid(
    input_defs=[
        dg.InputDefinition("cumulus", root_manager_key="cumulus_root"),
        dg.InputDefinition("camera", root_manager_key="camera_root"),
        dg.InputDefinition("images", root_manager_key="images_root"),
        dg.InputDefinition("omeka", root_manager_key="omeka_root"),
        dg.InputDefinition("wikidata", root_manager_key="wikidata_root"),
        dg.InputDefinition("portals", root_manager_key="portals_root"),
    ],
    output_defs=[dg.OutputDefinition(dagster_type=list)],
)
def format_values_chart(
    context,
    cumulus: main_dataframe_types,
    portals: main_dataframe_types,
    camera: main_dataframe_types,
    images: main_dataframe_types,
    omeka: main_dataframe_types,
    wikidata: main_dataframe_types,
):

    # kml finished
    kml_ims = camera.loc[camera["Source"] == "Instituto Moreira Salles"]
    val_kml = len(kml_ims[kml_ims["geometry"].notna()])
    # kml total
    val_kml_total = 0

    # image finished
    val_img = len(images[images["Media URL"].notna()])
    # image total
    val_img_total = len(images["Media URL"])

    # cumulus published
    api_p = portals["Source ID"].isin(cumulus["Source ID"])
    portals = portals[api_p]
    val_meta = len(portals[portals["Source URL"].notna()])
    # cumulus total
    val_meta_total = len(cumulus)

    # wiki published
    wiki = wikidata["Source ID"].isin(cumulus["Source ID"])
    wikidata = wikidata[wiki]
    val_wiki = len(wikidata[wikidata["wikidata_image"].notna()])
    # wiki total
    val_wiki_total = len(wikidata[wikidata["Wikidata ID"].notna()])

    # omeka published
    omk = omeka["Source ID"].isin(cumulus["Source ID"])
    omeka = omeka[omk]
    val_omeka = len(omeka[omeka["omeka_url"].notna()])
    # omeka total
    val_omeka_total = 0

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
        "y": ["Omeka-S", "Wiki", "Cumulus", "HiRes Images", "KML"],
    }

    values_pie = [val_omeka, val_wiki, val_meta, val_img, val_kml]

    values = [values_hbar, values_pie]

    return values


@dg.solid
def create_hbar(context, values: list):
    """
    Build bar graph stating progress
    in each area
    """

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
                "IMS items published",
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
        title="Situated Views of Rio de Janeiro",
        y_range=values_hbar["y"],
        x_range=(0, 6000),
        plot_height=300,
        plot_width=900,
        toolbar_location=None,
    )

    plot_hbar.title.text_font_size = "25px"
    plot_hbar.title.text_font_style = "bold"
    plot_hbar.add_layout(
        Title(
            text="Project Progress",
            align="left",
            text_font_size="15px",
            text_font_style="bold",
        ),
        "above",
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
def create_pie(context, values: list):
    """
    Build pie chart stating project's progress
    """

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


@dg.solid(config_schema=dg.StringSource)
def export_html(context, plot_hbar, plot_pie):
    """
    Create dashboard HTML page
    """
    path = context.solid_config

    output_file(path, title="Situated Views - Progress Dashboard")
    show(layout([[plot_hbar, plot_pie]], sizing_mode="stretch_both"))

    print("Done!")
