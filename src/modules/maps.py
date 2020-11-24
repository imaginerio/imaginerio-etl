import json

import geopandas as gpd
import pandas as pd
import numpy as np
from bokeh.models import (
    Circle,
    GeoJSONDataSource,
    ColumnDataSource,
    HoverTool,
    Patches,
    WheelZoomTool,
    CustomJS,
    CustomJSFilter,
    TextInput,
    DataTable,
    TableColumn,
    DateFormatter,
    CDSView
)
from bokeh.plotting import figure
from bokeh.layouts import column, row
from bokeh.tile_providers import Vendors, get_provider
from pyproj import Proj, transform
from shapely import wkt


def update(PATH):
    try:
        # load metadata
        DF = pd.read_csv(PATH)
        DF = DF.dropna(subset=["geometry"])
        DF["img_sd"] = DF["img_sd"].str.strip('"')

        # create geodataframe for points with images
        geodf = to_geodf(DF)

        # transform map projection
        map_geodf = apply_transform(geodf)

        # update maps
        map1 = index_map(map_geodf)
        map2 = search_map(map_geodf)

        export_map = {"map":map1,"search":map2}

        return export_map

    except Exception as e:
        print(str(e))


def to_geodf(df):
    """
    Return geodf
    """

    df["geometry"] = df["geometry"].astype(str).apply(wkt.loads)

    geodf = gpd.GeoDataFrame(df, geometry="geometry", crs="epsg:4326")

    return geodf


def transform_proj(row):
    """
    Transforms WGS84(epsg:4326) to WEBMERCATOR(epsg:3857)
    """

    try:
        proj_in = Proj("epsg:4326")
        proj_out = Proj("epsg:3857")
        x1, y1 = row["lat"], row["lng"]
        x2, y2 = transform(proj_in, proj_out, x1, y1)
        # print(f"({x1}, {y1}) to ({x2}, {y2})")
        return pd.Series([x2, y2])

    except Exception as e:
        print(str(e))


def apply_transform(geodf):
    """
    Convert WGS84(epsg:4326) to WEBMERCATOR(epsg:3857) coordinates
    """

    # print("Transforming projections...")
    geodf[["lat2", "lng2"]] = geodf.apply(transform_proj, axis=1)
    # print("Done.")

    geodf["geometry"] = geodf["geometry"].to_crs("epsg:3857")

    # reduce columns and return final geodataframe

    map_geodf = geodf[["id", "title", "description", "creator", "date", "img_sd", "geometry", "lat2", "lng2"]]

    return map_geodf


def index_map(map_geodf):

    # filter map_geodf
    map_geodf_img = map_geodf.copy().dropna(subset=["img_sd"])
    map_geodf_noimg = map_geodf.copy()[map_geodf["img_sd"].isna()]

    # create a geodatasource
    geosource_img = GeoJSONDataSource(geojson=map_geodf_img.to_json())
    geosource_noimg = GeoJSONDataSource(geojson=map_geodf_noimg.to_json())

    # Description from points
    TOOLTIPS1 = """
            <div style="margin: 5px; width: 300px" >
            <img
                src="@img_sd" alt="@img_sd" height=200
                style="margin: 0px;"
                border="2"
                ></img>
                <h3 style='font-size: 10px; font-weight: bold;'>@id</h3>
                <p style='font-size: 10px; font-weight: light; font-style: italic;'>@creator</p>
            
            </div>
        """
    TOOLTIPS2 = """
            <div style="margin: 5px; width: 300px" >
                <h3 style='font-size: 10px; font-weight: bold;'>@id</h3>
                <p style='font-size: 10px; font-weight: light; font-style: italic;'>@creator</p>
            </div>
        """
    callback_tp1 = CustomJS(
        code="""
                                var tooltips = document.getElementsByClassName("bk-tooltip");
                                for (var i = 0, len = tooltips.length; i < len; i ++) {
                                    tooltips[i].style.top = ""; 
                                    tooltips[i].style.right = "";
                                    tooltips[i].style.bottom = "0px";
                                    tooltips[i].style.left = "0px";
                                }
                                """
    )

    # Base map
    fig1 = figure(
        x_axis_type="mercator",
        y_axis_type="mercator",
        plot_width=1400,
        plot_height=900,
        toolbar_location=None,
    )

    tile_provider1 = get_provider(Vendors.CARTODBPOSITRON_RETINA)
    fig1.add_tile(tile_provider1)

    # construct points and wedges from hover
    viewcone1 = fig1.patches(
        xs="xs",
        ys="ys",
        source=geosource_img,
        fill_color="white",
        fill_alpha=0,
        line_color=None,
        hover_alpha=0.7,
        hover_fill_color="grey",
        hover_line_color="grey",
    )

    viewcone2 = fig1.patches(
        xs="xs",
        ys="ys",
        source=geosource_noimg,
        fill_color="white",
        fill_alpha=0,
        line_color=None,
        hover_alpha=0.7,
        hover_fill_color="grey",
        hover_line_color="grey",
    )

    point_noimg = fig1.circle(
        x="lat2",
        y="lng2",
        source=geosource_noimg,
        size=7,
        fill_color="orange",
        fill_alpha=0.5,
        line_color="dimgray",
    )

    point_img = fig1.circle(
        x="lat2",
        y="lng2",
        source=geosource_img,
        size=7,
        fill_color="orange",
        fill_alpha=0.5,
        line_color="dimgray",
    )

    # create a hovertool
    h1 = HoverTool(
        renderers=[point_img],
        tooltips=TOOLTIPS1,
        mode="mouse",
        show_arrow=False,
        callback=callback_tp1,
    )
    h2 = HoverTool(
        renderers=[point_noimg],
        tooltips=TOOLTIPS2,
        mode="mouse",
        show_arrow=False,
        callback=callback_tp1,
    )

    fig1.add_tools(h1, h2)
    fig1.toolbar.active_scroll = fig1.select_one(WheelZoomTool)
    fig1.xaxis.major_tick_line_color = None
    fig1.xaxis.minor_tick_line_color = None
    fig1.yaxis.major_tick_line_color = None
    fig1.yaxis.minor_tick_line_color = None
    fig1.xaxis.major_label_text_font_size = "0pt"
    fig1.yaxis.major_label_text_font_size = "0pt"

    return fig1

def search_map(map_geodf):

    # replace NAN values for string
    map_geodf["img_sd"].copy().fillna(value="No img", inplace=True)
    #print(map_geodf["img_sd"])

    # create a geodatasource
    geosource = GeoJSONDataSource(geojson=map_geodf.to_json())
    
    # base map
    fig2 = figure(
        x_axis_type="mercator",
        y_axis_type="mercator",
        plot_width=1400,
        plot_height=800,
        toolbar_location=None)

    tile_provider2 = get_provider(Vendors.CARTODBPOSITRON_RETINA)
    fig2.add_tile(tile_provider2)

    # search bar
    search = TextInput(placeholder = "Enter filter")

    # callback triggers the filter when the text input changes
    custom_filter = CustomJSFilter(args=dict(search=search), code="""
    var indices = [];
    if(search.value == ''){
    for (var i = 0; i < source.get_length(); i++){
        indices.push(false);
        }
    }
    else if(search.value == 'all'){
    for (var i = 0; i <source.get_length(); i++){
        indices.push(true);
        }
    }
    else{
        indices = [];
        for (var i = 0; i < source.get_length(); i++){
            if (source.data['id'][i] == search.value){
                indices.push(true);
            }
            else {
                indices.push(false);
                }
        }
    }
    return indices;
    """)

    # filter data from input
    view = CDSView(source=geosource, filters=[custom_filter])
    search.js_on_change(
        'value',
        CustomJS(
            args=dict(source=geosource),
            code="""source.change.emit()""")
            )

    # config table
    datat = map_geodf[["id","title","description", "creator", "date"]]
    source = ColumnDataSource(datat)

    columns = [
            TableColumn(field="id", title="Record Name"),
            TableColumn(field="title", title="Título"),
            TableColumn(field="description", title="Resumo"),
            TableColumn(field="creator", title="Autoria"),
            TableColumn(field="date", title="Data")
        ]
    datatable1 = DataTable(source=source, columns=columns, width=600, height=280)
    datatable2 = DataTable(source=geosource, columns=columns, width=600, height=280, view=view)

    # construct points and wedges from hover
    viewcone = fig2.patches(
        xs="xs",
        ys="ys",
        source=geosource,
        fill_color="white",
        fill_alpha=0,
        line_color=None,
        hover_alpha=0.7,
        hover_fill_color="grey",
        hover_line_color="grey"
    )

    point = fig2.circle(
        x="lat2",
        y="lng2",
        source=geosource,
        size=7,
        fill_color="orange",
        fill_alpha=0.5,
        line_color="dimgray",
        view=view
    )


    fig2.toolbar.active_scroll = fig2.select_one(WheelZoomTool)
    fig2.xaxis.major_tick_line_color = None
    fig2.xaxis.minor_tick_line_color = None
    fig2.yaxis.major_tick_line_color = None
    fig2.yaxis.minor_tick_line_color = None
    fig2.xaxis.major_label_text_font_size = "0pt"
    fig2.yaxis.major_label_text_font_size = "0pt"

    col1 = column(search, fig2, sizing_mode="stretch_width")
    col2 = column(datatable2,datatable1,sizing_mode="stretch_both")
    layout = row(col1,col2, sizing_mode="scale_both")

    return layout
