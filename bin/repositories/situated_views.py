import dagster as dg
from bin.pipelines.catalog import catalog_pipeline
from bin.pipelines.images import images_pipeline
from bin.schedules.daily import daily


@dg.repository
def situated_views():
    return {
        "pipelines": {
            "catalog_pipeline": lambda: catalog_pipeline,
            "images_pipeline": lambda: images_pipeline,
        },
        "schedules": {"daily": lambda: daily},
    }
