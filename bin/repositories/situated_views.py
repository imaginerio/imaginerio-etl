import dagster as dg
from bin.pipelines.catalog_pipeline import catalog_pipeline
from bin.pipelines.images_pipeline import images_pipeline
from bin.pipelines.apis_pipeline import apis_pipeline
from bin.schedules.daily import daily


@dg.repository
def situated_views():
    return {
        "pipelines": {
            "catalog_pipeline": lambda: catalog_pipeline,
            "images_pipeline": lambda: images_pipeline,
            "apis_pipeline": lambda: apis_pipeline,
        },
        "schedules": {"daily": lambda: daily},
    }
