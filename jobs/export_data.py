from dagster import job
from dotenv import load_dotenv
from ops.export import *
from resources.csv_root_input import csv_root_input
from resources.geojson_root_input import geojson_root_input
from resources.df_csv_io_manager import df_csv_io_manager

load_dotenv(override=True)


@job(
    resource_defs={
        "pandas_csv": df_csv_io_manager,
        "metadata_root": csv_root_input,
        "smapshot_root": csv_root_input,
        "cumulus_root": csv_root_input,
        "wikidata_root": csv_root_input,
        "camera_root": geojson_root_input,
        "images_root": csv_root_input,
        "omeka_root": csv_root_input,
        "mapping_root": csv_root_input,
        "portals_root": csv_root_input,
    },
    config={
        "resources": {
            "metadata_root": {"config": {"env": "METADATA"}},
            "smapshot_root": {"config": {"env": "SMAPSHOT"}},
            "camera_root": {"config": {"env": "CAMERA"}},
            "cumulus_root": {"config": {"env": "CUMULUS"}},
            "images_root": {"config": {"env": "IMAGES"}},
            "omeka_root": {"config": {"env": "OMEKA"}},
            "wikidata_root": {"config": {"env": "WIKIDATA"}},
            "mapping_root": {"config": {"env": "MAPPING"}},
            "portals_root": {"config": {"env": "PORTALS"}},
        },
        "solids": {
            "export_html": {"config": {"env": "INDEX"}},
        },
    },
)
def export_data():

    export_df = load_metadata()

    # Import WIKIDATA
    wikidata_df = make_df_to_wikidata(export_df)
    wikidata_df = organise_creator(quickstate=wikidata_df)

    # index.html
    values = format_values_chart()
    plot_hbar = create_hbar(values)
    plot_pie = create_pie(values)
    export_html(plot_hbar, plot_pie)
