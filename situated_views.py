import dagster as dg
from pipelines.catalog_pipeline import *
from pipelines.images_pipeline import *
from pipelines.apis_pipeline import *
from pipelines.export_pipeline import *
from pipelines.metadata_pipeline import *
from pipelines.images_pipeline import *
from pipelines.camera_pipeline import *
from pipelines.git_pipeline import *


@dg.repository
def situated_views():
    return {
        "pipelines": {
            "catalog_pipeline": lambda: catalog_pipeline,
            "images_pipeline": lambda: images_pipeline,
            "apis_pipeline": lambda: apis_pipeline,
            "export_pipeline": lambda: export_pipeline,
            "metadata_pipeline": lambda: metadata_pipeline,
            "camera_pipeline": lambda: camera_pipeline,
        },
        "schedules": {
            "weekly": lambda: weekly,
            "pull_new_data_weekly": lambda: pull_new_data_weekly,
        },
        "sensors": {
            # "trigger_catalog": lambda: trigger_catalog,
            "trigger_export": lambda: trigger_export,
            "trigger_metadata": lambda: trigger_metadata,
            "trigger_apis": lambda: trigger_apis,
        },
    }
