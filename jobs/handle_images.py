from dagster import job, in_process_executor, Any
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
        "s3_manager": s3_io_manager,
        "s3": s3_resource,
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
                    "review": {"env": "REVIEW"},
                }
            },
            # "create_images_df": {"config": {"env": "BUCKET"}},
            "embed_metadata": {
                # "inputs": {"image": Any},
                "config": {
                    "env": "EXIFTOOL",
                },
            },
        },
        "resources": {
            "metadata_root": {"config": {"env": "METADATA"}},
            "s3_manager": {"config": {"s3_bucket": "imaginerio-images"}},
        },
        # "execution": {"config": {"multiprocess": {"max_concurrent": 1}}},
    },
    executor_def=in_process_executor,
)
def handle_images():
    images = file_picker()
    # images_df = create_images_df(images)
    # update_metadata(df=images_df)
    dispatched = images.map(file_dispatcher)  # (images)
    embedded = dispatched.map(embed_metadata)  # (images=dispatched)
    uploaded = embedded.map(upload_to_cloud)
    # images_df = create_images_df(uploaded.collect())
    # update_metadata(df=images_df)
