import pandas as pd
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, HoverTool, WheelZoomTool, Span
from bokeh.transform import cumsum
from copy import deepcopy


def update(PATH):

    try:
        # load metadata.csv and images.csv
        DF = pd.read_csv(PATH)
        IMG = pd.read_csv(os.environ["IMAGES_PATH"])

        # kml finished
        val_kml = len(DF[DF["geometry"].notna()])
        # kml total
        val_kml_total = 0

        # image finished
        val_img = len(DF[DF["img_hd"].notna() & DF["geometry"].notna()])
        # image total
        val_img_total = len(IMG)

        # cumulus published
        val_meta = len(DF[DF["portals_id"].notna()])
        # cumulus total
        val_meta_total = len(DF)

        # wiki published
        val_wiki = len(DF[DF["wikidata_image"].notna()])
        # wiki total
        val_wiki_total = len(DF[DF["wikidata_id"].notna()])

        # omeka published
        val_omeka = len(DF[DF["omeka_url"].notna()])
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

        # update color bar
        plot_hbar = update_hbar(values)
        # update pie chart
        plot_pie = update_pie(values)

        # export figures
        export_figures = {"hbar": plot_hbar, "pie": plot_pie}

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
