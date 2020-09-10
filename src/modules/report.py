import os
from copy import deepcopy

import numpy as np
import pandas as pd
from bokeh.models import (
    CheckboxButtonGroup,
    ColumnDataSource,
    CustomJS,
    HoverTool,
    LinearColorMapper,
    Row,
    Span,
    WheelZoomTool,
)
from bokeh.plotting import figure
from bokeh.transform import cumsum


def update(PATH):

    try:
        # load metadata.csv
        DF = pd.read_csv(PATH)
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

        values = {
            "Done": [val_omeka, val_wiki, val_meta, val_img, val_kml],
            "To do": [
                val_omeka_total,
                val_wiki_total - val_wiki,
                val_meta_total - val_meta,
                val_img_total - val_img,
                val_kml_total,
            ],
            "y": ["Omeka-S", "Wikimedia", "Cumulus", "HiRes Images", "KML"],
        }

        # count fields for each item and add result as column
        DF_AUX = DF_AUX.replace(0, np.nan)
        DF["rate"] = DF_AUX.count(axis=1)

        # update color bar
        plot_hbar = update_hbar(values)
        # update pie chart
        plot_pie = update_pie(values)
        # update tiles chart
        plot_tiles = update_tiles(DF)

        # export figures
        export_figures = {"hbar": plot_hbar, "pie": plot_pie, "tiles": plot_tiles}

        return export_figures

    except Exception as e:
        print(str(e))


def update_hbar(values):
    """ 
    Render hbar report
    """

    # construct a data source
    list1 = ["Done", "To do"]
    data = values
    # deepcopy the data for later use
    data1 = deepcopy(data)

    data1.update(
        {
            "tooltip_orange": [
                "Published",
                "Commons and Wikidata",
                "On IMS' Cumulus Portals",
                "Geolocated",
                "Total geolocated items",
            ],
            "tooltip_grey": [
                "Not on Omeka",
                "Wikidata only",
                "Potential items",
                "To geolocate",
                "Not geolocated",
            ],
        }
    )

    # base dashboard
    for i in range(1, len(list1)):
        data[list1[i]] = [sum(x) for x in zip(data[list1[i]], data[list1[i - 1]])]

    plot_hbar = figure(
        y_range=data["y"],
        x_range=(0, 5500),
        plot_height=300,
        plot_width=900,
        toolbar_location=None,
    )

    # construct bars with two differents datas
    hbar_1 = plot_hbar.hbar(
        y=data["y"], right=data["Done"], left=0, height=0.8, color="orange"
    )
    hbar_1.data_source.add(data1["tooltip_orange"], "data")
    hbar_1.data_source.add(data1["Done"], "value")

    hbar_2 = plot_hbar.hbar(
        y=data["y"],
        right=data["To do"],
        left=data["Done"],
        height=0.8,
        color="lightgrey",
    )
    hbar_2.data_source.add(data1["tooltip_grey"], "data")
    hbar_2.data_source.add(data1["To do"], "value")

    # add hover tool for each bar chart
    TOOLTIPS1 = "@data: @value"
    TOOLTIPS2 = "@data: @value"
    h1 = HoverTool(
        renderers=[hbar_1], tooltips=TOOLTIPS1, mode="mouse", show_arrow=False
    )
    h2 = HoverTool(
        renderers=[hbar_2], tooltips=TOOLTIPS2, mode="mouse", show_arrow=False
    )

    plot_hbar.add_tools(h1, h2)

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


def update_pie(values):
    """ 
    Render pie chart report
    """

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


def update_tiles(DF):
    """
    Render tiles chart report
    """

    df = DF.loc[
        :,
        [
            "id",
            "img_sd",
            "img_hd",
            "lat",
            "lng",
            "geometry",
            "portals_id",
            "portals_url",
            "wikidata_id",
            "omeka_url",
            "rate",
        ],
    ]

    # construct coordinates
    coord = []
    for y in range(0, 52):
        for x in range(0, 100):
            if len(coord) != len(df):
                coord.append([(x, y)])
            else:
                break

    df_coord = pd.DataFrame(coord, columns=["coordinate"])
    df_coord[["x", "y"]] = pd.DataFrame(
        df_coord["coordinate"].tolist(), index=df_coord.index
    )
    df_coord = df_coord.drop(columns="coordinate")

    df_tiles = pd.merge(
        df, df_coord, left_index=True, right_index=True, validate="one_to_one"
    )
    df_tiles_s = df_tiles.iloc[:, [0, 1, 2, 3, 4, 4, 5, 6, 7, 8, 9, 10]].sort_values(
        by=["rate"], ascending=False, ignore_index=True
    )
    df_tiles_sort = df_tiles_s.join(df_tiles[["x", "y"]])



    # setting colors
    colors = ["#edf8e9", "#c7e9c0", "#a1d99b", "#74c476", "#31a354", "#006d2c"]
    mapper = LinearColorMapper(palette=colors, low=df.rate.min(), high=df.rate.max())

    # config tooltip
    TOOLTIPS = """
        <div style="margin: 5px; width: 400px" >
        <h3 
            style='font-size: 12px; font-weight: bold;'>
            @id
        </h3>
        <p 
            style='font-size: 10px; font-weight: bold;'>
            Geolocated: (@lat,@lng)
        </p>
        <p 
            style='font-size: 10px; font-weight: bold;'>
            Image: @img_sd @img_hd
        </p>
        <p 
            style='font-size: 10px; font-weight: bold;'>
            Portals: @portals_url
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

    plot_tiles = Row(tiles, button)

    return plot_tiles
