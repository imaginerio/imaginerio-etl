import pandas as pd
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, HoverTool, WheelZoomTool


def update_hbar(METADATA_PATH):
    """ 
    Render hbar report
    """

    try:

        DF = pd.read_csv(METADATA_PATH)

        # kml yellow bar
        val_kml = len(DF[DF["geometry"].notna()])

        # image yellow bar
        val_img = len(DF[DF["img_hd"].notna() & DF["geometry"].notna()])

        # image grey bar
        val_img_total = len(DF[DF["img_hd"].notna()])

        # cumulus yellow bar
        val_meta = len(DF[DF["portals_id"].notna()])

        # cumulus grey bar
        val_meta_total = len(DF)

        # wiki yellow bar
        val_wiki = len(DF[DF["wikidata_image"].notna()])

        # wiki grey bar
        val_wiki_total = len(DF[DF["wikidata_id"].notna()])

        # construct a data source
        source = ColumnDataSource(
            data=dict(
                x1=[0, val_wiki, val_meta, val_img, val_kml],
                x2=[
                    0,
                    val_wiki_total - val_wiki,
                    val_meta_total - val_meta,
                    val_img_total - val_img,
                    4000 - val_kml,
                ],
                y=["Omeka-S", "Wikimedia", "Cumulus", "Images", "KML"],
            )
        )

        TOOLTIPS = [("OK", "@x1"), ("Not OK", "@x2")]

        # base dashboard
        plot_hbar = figure(
            y_range=["Omeka-S", "Wikimedia", "Cumulus", "Images", "KML"],
            x_range=(0, 5500),
            plot_height=300,
            plot_width=900,
            toolbar_location=None,
            tooltips=TOOLTIPS,
        )

        plot_hbar.ygrid.grid_line_color = None
        plot_hbar.toolbar.active_drag = None
        plot_hbar.background_fill_color = "ghostwhite"

        # construct bars with two differents datas
        bars1 = plot_hbar.hbar_stack(
            ["x1", "x2"],
            y="y",
            height=0.8,
            color=("orange", "lightgrey"),
            source=source,
        )

        # data goal line
        line = plot_hbar.line(
            x=[4000, 4000], y=[0, 5], line_width=3, line_dash="dashed", color="grey",
        )

        return plot_hbar

    except Exception as e:

        print(str(e))


def update_pie():
    """ 
    Render pie chart report
    """
    plot_pie = figure(
        plot_height=100,
        toolbar_location=None
        )

    plot_pie.wedge(
        x=0, y=1,radius=0.5,
        start_angle=0,
        end_angle=360,
        start_angle_units='deg',
        end_angle_units='deg',
        line_color=None,
        fill_color="lightgrey",
        )

    plot_pie.axis.axis_label=None
    plot_pie.axis.visible=False
    plot_pie.grid.grid_line_color = None

    return plot_pie
