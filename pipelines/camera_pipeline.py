from datetime import datetime

import dagster as dg
from dotenv import load_dotenv
from solids.camera import *
from solids.update_metadata import update_metadata
from utils.df_csv_io_manager import df_csv_io_manager
from utils.geojson_io_manager import geojson_io_manager
from utils.csv_root_input import csv_root_input
from utils.geojson_io_manager import geojson_io_manager
from solids.push_new_data import push_new_data

load_dotenv(override=True)

# Switch commits to another branch, change preset.
preset = {
    "solids": {
        "get_list": {"config": {"env": "NEW_RAW"}},
        "split_photooverlays": {
            "config": {
                "new_single": {
                    "env": "NEW_SINGLE"
                },
                "processed_raw": {
                    "env": "PROCESSED_RAW"}}},

        "rename_single": {
            "config": {"env": "NEW_SINGLE"}},
        "move_files": {
            "config": {
                "new_single": {
                    "env": "NEW_SINGLE"
                },
                "processed_single": {
                    "env": "PROCESSED_SINGLE"}}},

        "create_geojson": {"config": {"env": "CAMERA"}},
        # "push_new_data":{
        #     "config":{
        #         "commit":"Geojson",
        #         "branch":"dev"
        #     }},
    },
    "resources": {
        "metadata_root": {"config": {"env": "METADATA"}},
        "cumulus_root": {"config": {"env": "CUMULUS"}}},
}


@dg.pipeline(
    mode_defs=[
        dg.ModeDefinition(
            name="default",
            resource_defs={
                "geojson": geojson_io_manager,
                "pandas_csv": df_csv_io_manager,
                "metadata_root": csv_root_input,
                "cumulus_root": csv_root_input
            }
        )
    ],
    preset_defs=[
        dg.PresetDefinition(
            "default",
            run_config=preset,
            mode="default",
        )
    ],
)
def camera_pipeline():

    kmls = get_list()
    kmls = split_photooverlays(kmls)

    kmls = rename_single()
    kmls = change_img_href(kmls)
    kmls = correct_altitude_mode(kmls)
    new_features = create_feature(kmls=kmls)
    move_files(new_features)
    geojson = create_geojson(new_features=new_features)
    ok = update_metadata(df=geojson)
    #push_new_data(ok)


################   SENSORS   ##################


@dg.sensor(
    pipeline_name="camera_pipeline",
    solid_selection=["*split_photooverlays"],
    minimum_interval_seconds=60,
)
def trigger_camera_step1(context):
    path = "data/input/kmls/new_raw"
    kmls = os.listdir(path)
    list_kmls = [x for x in kmls if x != ".gitkeep"]

    now = datetime.datetime.now().strftime("%d/%m/%Y%H%M%S")
    run_key = f"step1_{now}"

    if list_kmls:
        yield dg.RunRequest(run_key=run_key, run_config=preset)


@dg.sensor(
    pipeline_name="camera_pipeline",
    solid_selection=["rename_single+++++"],
    minimum_interval_seconds=7200,
)
def trigger_camera_step2(context):
    path = "data/input/kmls/new_single"
    kmls = os.listdir(path)
    list_kmls = [x for x in kmls if x != ".gitkeep"]
    now = datetime.datetime.now().strftime("%d/%m/%Y%H%M%S")
    run_key = f"step2_{now}"

    if list_kmls != []:
        yield dg.RunRequest(run_key=run_key, run_config=preset)
