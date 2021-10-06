import dagster as dg
from pipelines.cumulus_pipeline import *
from pipelines.images_pipeline import *
from pipelines.apis_pipeline import *
from pipelines.export_pipeline import *
from pipelines.metadata_pipeline import *
from pipelines.images_pipeline import *
from pipelines.camera_pipeline import *
from pipelines.git_pipeline import *
from pipelines.IIIF_pipeline import *


@dg.repository
def situated_views():
    return {
        "pipelines": {
            "cumulus_pipeline": lambda: cumulus_pipeline,
            "images_pipeline": lambda: images_pipeline,
            "apis_pipeline": lambda: apis_pipeline,
            "export_pipeline": lambda: export_pipeline,
            "metadata_pipeline": lambda: metadata_pipeline,
            "camera_pipeline": lambda: camera_pipeline,
            "git_pipeline": lambda: git_pipeline,
            "IIIF_pipeline": lambda: IIIF_pipeline,
        },
        "schedules": {
            "apis_pipeline_weekly": lambda: apis_pipeline_weekly,
            "pull_new_data_hourly": lambda: pull_new_data_hourly,
            "push_new_data_daily": lambda: push_new_data_daily,
        },
        "sensors": {
            "trigger_cumulus": lambda: trigger_cumulus,
            "trigger_export": lambda: trigger_export,
            "trigger_metadata": lambda: trigger_metadata,
            "trigger_apis": lambda: trigger_apis,
            "trigger_camera_step1": lambda: trigger_camera_step1,
            "trigger_camera_step2": lambda: trigger_camera_step2,
        },
    }
