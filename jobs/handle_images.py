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
        "ops": {
            "file_picker": {
                "config": {"env": "SOURCE"},
            },
            "file_dispatcher": {
                "config": {
                    "tif": {"env": "TIF"},
                    "jpg": {"env": "JPG"},
                    "backlog": {"env": "BACKLOG"},
                    # "jpeg_sd": {"env": "JPEG_SD"},
                    "review": {"env": "REVIEW"},
                }
            },
            "create_images_df": {"config": {"env": "CLOUD"}},
            "embed_metadata": {
                "config": {
                    "env": "EXIFTOOL",
                },
            },
        },
        "resources": {
            "metadata_root": {"config": {"env": "METADATA"}},
        },
    },
)
def handle_images():
    image_list = file_picker()
    dispatched = file_dispatcher(image_list)
    images_df = create_images_df(image_list)
    update_metadata(df=images_df)
    embedded = embed_metadata(image_list=dispatched)
    # upload_to_cloud(embedded)
