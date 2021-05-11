import dagster as dg
from bin.pipelines.main import main
from bin.schedules.daily import daily


@dg.repository
def situated_views():
    return {
        "pipelines": {"main": lambda: main},
        "schedules": {"daily": lambda: daily},
    }
