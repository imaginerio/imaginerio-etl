from bokeh.plotting import output_file, show
from bokeh.layouts import layout

import report, maps


def dashboard(METADATA_PATH):
    """
    Generates an HTML file with dashboard and map using bokeh
    """

    try:

        # load dashboard
        dashboard_plot = report.update(METADATA_PATH)

        # load map
        map_plot = maps.update(METADATA_PATH)

        # export
        output_file("./index.html", title="Situated Views")
        show(layout([[dashboard_plot["hbar"],dashboard_plot["pie"]],[map_plot]], sizing_mode="stretch_both"))

        print("Dashboard updated")

    except Exception as e:
        print(str(e))


def omeka_csv():
    pass

dashboard("./metadata/metadata.csv")