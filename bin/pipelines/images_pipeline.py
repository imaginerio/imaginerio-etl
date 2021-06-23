import dagster as dg

from bin.solids.images import *
from bin.solids.utils import df_csv_io_manager, root_input_csv, update_metadata

preset = {
    "solids": {
        "file_picker": {
            "config": "/mnt/y/projetos/getty",
            "inputs": {"camera": {"path": "data-out/camera.csv"}},
        },
        "file_dispatcher": {
            "config": {
                "tiff": "/mnt/d/imagineRio-images/tiff",
                "jpeg_hd": "/mnt/d/imagineRio-images/jpeg-hd",
                "jpeg_sd": "/mnt/d/imagineRio-images/jpeg-sd",
                "backlog": "/mnt/d/imagineRio-images/jpeg-sd/backlog",
            }
        },
        "create_images_df": {
            "config": "https://rioiconography.sfo2.digitaloceanspaces.com/situatedviews/"
        },
        "update_metadata": {"inputs": {"metadata": {"path": "data-out/metadata.csv"}}},
        "write_metadata": {
            "config": "/mnt/c/exiftool.exe",
            "inputs": {"metadata": {"path": "data-out/metadata.csv"}},
        },
    }
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
