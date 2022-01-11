import dagster as dg
from jobs.format_ims_metadata import *
from jobs.handle_images import *
from jobs.query_apis import *
from jobs.export_data import *
from jobs.compile_metadata import *
from jobs.handle_images import *
from jobs.process_kmls import *
from jobs.iiif_pipeline import *


@dg.repository
def situated_views():
    return {
        "pipelines": {
            "cumulus_pipeline": lambda: format_ims_metadata,
            "images_pipeline": lambda: images_pipeline,
            "apis_pipeline": lambda: query_apis,
            "export_pipeline": lambda: export_pipeline,
            "metadata_pipeline": lambda: compile_metadata,
            "camera_pipeline": lambda: process_kmls,
            # "git_pipeline": lambda: git_pipeline,
            "iiif_pipeline": lambda: iiif_pipeline,
        },
    }
