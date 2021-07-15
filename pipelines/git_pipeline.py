import dagster as dg
from solids.utils import *


@dg.pipeline
def git_pipeline():
    pull_new_data()
    push_new_data()


################   SCHEDULES   ##################


@dg.schedule(
    cron_schedule="0 0 * * *",
    pipeline_name="git_pipeline",
    solid_selection=["push_new_data"],
    execution_timezone="America/Sao_Paulo",
)
def push_new_data_daily():
    return {}


@dg.schedule(
    cron_schedule="0 * * * *",
    pipeline_name="git_pipeline",
    solid_selection=["pull_new_data"],
    execution_timezone="America/Sao_Paulo",
)
def pull_new_data_hourly():
    return {}
