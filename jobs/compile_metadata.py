from dagster import job
from dotenv import load_dotenv
from ops.append_jstor import append_jstor
from ops.generate_metadata import generate_metadata
from resources.csv_root_input import csv_root_input
from resources.df_csv_io_manager import df_csv_io_manager
from resources.geojson_root_input import geojson_root_input
from resources.xls_root_input import xls_root_input
from tests.dataframe_types import *
from tests.objects_types import *

load_dotenv(override=True)


@job(
    resource_defs={
        "pandas_csv": df_csv_io_manager,
        "cumulus_root": csv_root_input,
        "jstor_root": xls_root_input,
        "wikidata_root": csv_root_input,
        "portals_root": csv_root_input,
        "camera_root": geojson_root_input,
        "images_root": csv_root_input,
    },
    config={
        "resources": {
            "cumulus_root": {"config": {"env": "CUMULUS"}},
            "wikidata_root": {"config": {"env": "WIKIDATA"}},
            "portals_root": {"config": {"env": "PORTALS"}},
            "camera_root": {"config": {"env": "CAMERA"}},
            "images_root": {"config": {"env": "IMAGES"}},
            "jstor_root": {"config": {"env": "JSTOR_XLS"}},
        },
    },
)
def compile_metadata():
    metadata = generate_metadata()
    append_jstor(metadata=metadata)
