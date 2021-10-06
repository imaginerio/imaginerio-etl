import dagster as dg
from dotenv import load_dotenv
from solids.images import *
from solids.utils import (
    df_csv_io_manager,
    push_new_data,
    root_input_csv,
    root_input_geojson,
    update_metadata,
)

load_dotenv(override=True)

preset = {
    "solids": {
        "file_picker": {
            "config": {"env": "SOURCE"},
        },
        "file_dispatcher": {
            "config": {
                "backlog": {"env": "IMG_BACKLOG"},
                "jpeg_hd": {"env": "JPEG_HD"},
                "jpeg_sd": {"env": "JPEG_SD"},
                "tiff": {"env": "TIFF"},
                "review": {"env": "REVIEW"},
            }
        },
        "create_images_df": {"config": {"env": "CLOUD"}},
        "write_metadata": {
            "config": {
                "env": "EXIFTOOL",
            }
        },
        "push_new_data":{"config":"Images"},
    },
    "resources": {
        "metadata_root": {"config": {"env": "METADATA"}},
    },
}


@dg.pipeline(
    mode_defs=[
        dg.ModeDefinition(
            resource_defs={
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
def images_pipeline():
    files = file_picker()
    to_tag = file_dispatcher(files=files)
    images_df = create_images_df(files=files)
    ok = update_metadata(df=images_df)
    to_upload = write_metadata(to_tag=to_tag)
    upload_to_cloud(to_upload)
    push_new_data(ok)

################   SCHEDULES   ##################


@dg.schedule(
    cron_schedule="0 18 * * 1-5",
    pipeline_name="images_pipeline",
    execution_timezone="America/Sao_Paulo",
)
def daily():
    return {}
