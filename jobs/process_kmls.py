from datetime import datetime

from dagster import job
from dotenv import load_dotenv
from ops.camera import *
from ops.update_metadata import update_metadata
from resources.df_csv_io_manager import df_csv_io_manager
from resources.geojson_io_manager import geojson_io_manager
from resources.csv_root_input import csv_root_input
from resources.geojson_io_manager import geojson_io_manager

load_dotenv(override=True)


@job(
    resource_defs={
        "geojson": geojson_io_manager,
        "pandas_csv": df_csv_io_manager,
        "metadata_root": csv_root_input,
        "cumulus_root": csv_root_input,
    },
    config={
        "ops": {
            "list_kmls": {"config": {"env": "NEW_RAW"}},
            "split_photooverlays": {
                "config": {
                    "new_single": {"env": "NEW_SINGLE"},
                    "processed_raw": {"env": "PROCESSED_RAW"},
                }
            },
            "rename_single": {"config": {"env": "NEW_SINGLE"}},
            "move_files": {
                "config": {
                    "new_single": {"env": "NEW_SINGLE"},
                    "processed_single": {"env": "PROCESSED_SINGLE"},
                }
            },
            "create_geojson": {"config": {"env": "CAMERA"}},
        },
        "resources": {
            "metadata_root": {"config": {"env": "METADATA"}},
            "cumulus_root": {"config": {"env": "CUMULUS"}},
        },
    },
)
def process_kmls():

    kmls = list_kmls()
    kmls = split_photooverlays(kmls)

    kmls = rename_single(ok=kmls)
    kmls = change_img_href(kmls)
    kmls = correct_altitude_mode(kmls)
    new_features = create_feature(kmls=kmls)
    move_files(new_features)
    geojson = create_geojson(new_features=new_features)
    update_metadata(df=geojson)
