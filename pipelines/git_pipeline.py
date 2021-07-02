import os
import dagster as dg
from solids.utils import *


@dg.pipeline
def git_pipeliene():
    pull_new_data()


################   SENSORS   ##################


################   SCHEDULES   ##################
@dg.schedule(
    cron_schedule="0 19 * * FRI",  # rodar toda sexta as 19hs
    pipeline_name="pull_new_data",
    execution_timezone="America/Sao_Paulo",
)
def pull_new_data_weekly():
    return {}


# CLI: dagster pipeline execute -f pipelines/git_pipeline.py
