import warnings
from dagster import repository, ExperimentalWarning
from dagster_aws.s3 import s3_resource
from jobs.format_ims_metadata import format_ims_metadata
from jobs.handle_images import handle_images
from jobs.query_apis import query_apis
from jobs.export_data import export_data
from jobs.compile_metadata import compile_metadata
from jobs.process_kmls import process_kmls
from jobs.iiif_factory import iiif_factory
from resources.csv_root_input import csv_root_input
from resources.local_io_manager import local_io_manager
from resources.s3_io_manager import s3_io_manager

warnings.filterwarnings("ignore", category=ExperimentalWarning)


@repository
def test_repo():
    return [
        iiif_factory.to_job(
            config={
                "resources": {
                    "metadata_root": {"config": {"env": "METADATA"}},
                    "mapping_root": {"config": {"env": "MAPPING"}},
                    "iiif_manager": {"config": {"s3_bucket": "imaginerio-images"}},
                },
            },
            resource_defs={
                "metadata_root": csv_root_input,
                "mapping_root": csv_root_input,
                "iiif_manager": local_io_manager,
            },
            tags={"mode": "test"},
        ),
    ]


@repository
def prod_repo():
    return [
        format_ims_metadata,
        handle_images,
        query_apis,
        export_data,
        compile_metadata,
        process_kmls,
        iiif_factory.to_job(
            config={
                "resources": {
                    "metadata_root": {"config": {"env": "METADATA"}},
                    "mapping_root": {"config": {"env": "MAPPING"}},
                    "iiif_manager": {"config": {"s3_bucket": "imaginerio-images"}},
                },
                "execution": {"config": {"multiprocess": {"max_concurrent": 4}}},
            },
            resource_defs={
                "metadata_root": csv_root_input,
                "mapping_root": csv_root_input,
                "iiif_manager": s3_io_manager,
                "s3": s3_resource,
            },
            tags={"mode": "prod"},
        ),
    ]
