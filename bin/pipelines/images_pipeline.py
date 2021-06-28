import dagster as dg

from bin.solids.images import *
from bin.solids.utils import df_csv_io_manager, root_input_csv, update_metadata
from dotenv import load_dotenv

load_dotenv(override=True)

preset = {
    "solids": {
        "file_picker": {
            "config": {"env": "SOURCE"},
        },
        "file_dispatcher": {
            "config": {
                "env": {
                    "tiff": "TIFF",
                    "jpeg_hd": "JPEG_HD",
                    "jpeg_sd": "JPEG_SD",
                    "backlog": "IMG_BACKLOG",
                }
            }
        },
        "create_images_df": {"config": {"env:" "CLOUD"}},
        "write_metadata": {
            "config": "EXIFTOLL",
        },
    },
    "resources": {
        "metadata_root": {"config": {"env": "METADATA"}},
        "camera_root": {"config": {"env": "CAMERA"}},
    },
}


@dg.pipeline(
    mode_defs=[
        dg.ModeDefinition(
            resource_defs={
                "pandas_csv": df_csv_io_manager,
                "metadata_root": root_input_csv,
                "camera_root": root_input_csv,
            }
        )
    ]
)
def images_pipeline():
    files = file_picker()
    to_tag = file_dispatcher(files)
    images_df = create_images_df(files)
    update_metadata(df=images_df)
    write_metadata(files_to_tag=to_tag)
    # upload_to_cloud()


################   SCHEDULES   ##################


@dg.schedule(
    cron_schedule="0 18 * * 1-5",
    pipeline_name="images_pipeline",
    execution_timezone="America/Sao_Paulo",
)
def daily():
    return {}
