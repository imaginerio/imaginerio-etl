import dagster as dg
from bin.pipelines.main import main

# from bin.schedules import biweekly


@dg.repository
def situated_views():
    return {
        "pipelines": {"main": lambda: main},
        # "schedules": {"biweekly": lambda: biweekly},
    }
