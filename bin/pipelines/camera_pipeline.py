import dagster as dg
from dotenv import load_dotenv

from bin.solids.utils import *
from bin.solids.camera import *

load_dotenv(override=True)

preset = {
    "solids": {
        "get_list": {"config": {"env": "KML_RAW"}},
        "split_photooverlays": {"config": {"env": "KML"}},
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
    kmls_splitteds = split_photooverlays(kmls)
    kmls_img_href = change_img_href(kmls_splitteds)
    kmls_altitude = correct_altitude_mode(kmls_img_href)
    geojson = create_geojson(kmls=kmls_altitude)
    update_metadata(df=geojson)


################   SENSORS   ##################


# @dg.sensor
# def trigger_camera(context):
#     last_mtime = float(context.cursor) if context.cursor else 0

#     # os.walk(top[,topdown=True[,onerror=None[,followlinks=False]))

#     max_mtime = last_mtime

#     fstats = os.stat("")
#     file_mtime = fstats.st_mtime
#     if file_mtime <= last_mtime:
#         # yield dg.RunRequest(run_key=run_key)
#         max_mtime = max(max_mtime, file_mtime)

#     context.update_cursor(str(max_mtime))


# CLI dagster pipeline execute -f bin/pipelines/camera_pipeline.py -c bin/pipelines/camera_pipeline.yaml
