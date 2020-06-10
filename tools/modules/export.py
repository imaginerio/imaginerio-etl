from bokeh.plotting import output_file, show
from bokeh.layouts import layout

import report, maps


def dashboard(METADATA_PATH):
    """
    Generates an HTML file with dashboard and map using bokeh
    """

    try:

        # load dashboard
        hbar = report.update_hbar(METADATA_PATH)
        pie = report.update_pie()

        # load map
        map_plot = maps.update(METADATA_PATH)

        # export
        output_file("./index.html", title="Situated Views")
        show(layout([[hbar,pie],[map_plot]], sizing_mode="stretch_both"))

        print("Dashboard updated")

    except Exception as e:
        print(str(e))


def omeka_csv():
    pass

dashboard('./metadata/metadata.csv')