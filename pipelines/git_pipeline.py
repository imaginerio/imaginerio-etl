import datetime
import os

import dagster as dg
from solids.utils import *


@dg.pipeline
def git_pipeline():
    pull_new_data()
    push_new_data()


################   SENSORS   ##################
# @dg.sensor(
#     pipeline_name="git_pipeline",
#     solid_selection=["push_new_data"],
#     minimum_interval_seconds=300,
# )
# def trigger_git_push(context):
#     events = context.instance.events_for_asset_key(
#         dg.AssetKey("metadata"),
#         after_cursor=context.last_run_key,
#         ascending=False,
#         limit=1,
#     )
#     if events:
#         record_id, event = events[0]  # take the most recent materialization
#         yield dg.RunRequest(
#             run_key=str(record_id),
#             run_config={},
#             tags={"source_pipeline": event.pipeline_name},
#         )


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
def pull_new_data_hour():
    return {}


# CLI: dagster pipeline execute -f pipelines/git_pipeline.py
# CLI: dagster sensor preview trigger_git_push
