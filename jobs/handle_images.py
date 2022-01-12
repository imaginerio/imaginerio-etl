from dagster import job
from dagster_aws.s3 import s3_resource
from dotenv import load_dotenv
from ops.images import *
from ops.update_metadata import update_metadata
from resources.df_csv_io_manager import df_csv_io_manager
from resources.csv_root_input import csv_root_input
from resources.s3_io_manager import s3_io_manager

load_dotenv(override=True)


@job(
    resource_defs={
        "pandas_csv": df_csv_io_manager,
        "metadata_root": csv_root_input,
    },
    config={
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
        },
        "resources": {
            "metadata_root": {"config": {"env": "METADATA"}},
        },
    },
)
def handle_images():
    files = file_picker()
    to_tag = file_dispatcher(files=files)
    images_df = create_images_df(files=files)
    update_metadata(df=images_df)
    to_upload = write_metadata(to_tag=to_tag)
    upload_to_cloud(to_upload)
