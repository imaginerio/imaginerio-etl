import dagster as dg
from datetime import datetime
from dotenv import load_dotenv

from solids.utils import *
from solids.camera import *

load_dotenv(override=True)

preset = {
    "solids": {
        "get_list": {"config": {"env": "NEW_RAW"}},
        "split_photooverlays": {"config": {"env": "NEW_SINGLE"}},
        "change_img_href": {"config": {"env": "NEW_SINGLE"}},
        "create_feature": {"config": {"env": "PROCESSED_SINGLE"}},
        "create_geojson": {"config": {"env": "CAMERA"}},
    },
    "resources": {"metadata_root": {"config": {"env": "METADATA"}}},
}


@dg.pipeline(
    mode_defs=[
        dg.ModeDefinition(
            resource_defs={
                "geojson": geojson_io_manager,
                "pandas_csv": df_csv_io_manager,
                "metadata_root": root_input_csv,
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

    kmls = change_img_href()
    kmls = correct_altitude_mode(kmls)
    new_features = create_feature(kmls=kmls)
    geojson = create_geojson(new_features=new_features)
    update_metadata(df=geojson)


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
    solid_selection=["change_img_href++++"],
    minimum_interval_seconds=240,
)
def trigger_camera_step2(context):
    path = "data/input/kmls/new_single"
    kmls = os.listdir(path)
    list_kmls = [x for x in kmls if x != ".gitkeep"]
    now = datetime.datetime.now().strftime("%d/%m/%Y%H%M%S")
    run_key = f"step2_{now}"

    if list_kmls != []:
        yield dg.RunRequest(run_key=run_key, run_config=preset)
