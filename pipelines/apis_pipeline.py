import os
from datetime import datetime

import dagster as dg
from dotenv import load_dotenv
from pandas.core.frame import DataFrame
from solids.apis import *
from solids.update_metadata import update_metadata
from utils.csv_root_input import csv_root_input
from utils.df_csv_io_manger import df_csv_io_manager
from solids.push_new_data import push_new_data

load_dotenv(override=True)

# Switch commits to another branch, change preset.

# Call all APIs
preset = {
    "solids": {
        "omeka_dataframe": {"config": {"env": "OUTPUT"}},
        "query_omeka": {"config": {"env": "OMEKA_API"}},
        "query_wikidata": {"config": {"env": "WIKIDATA_API"}},
        "query_portals": {"config": {"env": "PORTALS_API"}},
        "portals_dataframe": {"config": {"env": "PORTALS_PREFIX"}},
        "push_new_data":{
            "config":{
                "commit":"APIS",
                "branch":"dev"}},
        "push_new_data_2":{
            "config":{
                "commit":"APIS",
                "branch":"dev"}},
        "push_new_data_3":{
            "config":{
                "commit":"APIS",
                "branch":"dev"}},
    },
    "resources": {"metadata_root": {"config": {"env": "METADATA"}}},
}
# Call Omeka-S API only
preset_omeka = {
    "solids": {
        "omeka_dataframe": {"config": {"env": "OUTPUT"}},
        "query_omeka": {"config": {"env": "OMEKA_API"}},
        "push_new_data":{
            "config":{
                "commit":"API Omeka",
                "branch":"dev"}},
    },
    "resources": {"metadata_root": {"config": {"env": "METADATA"}}},
}
# Call Wikidata API only
preset_wikidata = {
    "solids": {
        "query_wikidata": {"config": {"env": "WIKIDATA_API"}},
        "push_new_data":{
            "config":{
                "commit":"API Wikidata",
                "branch":"dev"}},
    },
    "resources": {"metadata_root": {"config": {"env": "METADATA"}}},
}
# Call Cumulus Portals API only
preset_portals = {
    "solids": {
        "query_portals": {"config": {"env": "PORTALS_API"}},
        "portals_dataframe": {"config": {"env": "PORTALS_PREFIX"}},
        "push_new_data":{
            "config":{
                "commit":"API Portals",
                "branch":"dev"}},
    },
    "resources": {"metadata_root": {"config": {"env": "METADATA"}}},
}

################   PIPELINE   ##################


@dg.pipeline(
    mode_defs=[
        dg.ModeDefinition(
            name="default",
            resource_defs={
                "pandas_csv": df_csv_io_manager,
                "metadata_root": csv_root_input,
            },
        ),
    ],
    preset_defs=[
        dg.PresetDefinition(
            "default",
            run_config=preset,
            mode="default",
        ),
        dg.PresetDefinition(
            "preset_omeka",
            run_config=preset_omeka,
            solid_selection=["query_omeka", "omeka_dataframe", "update_metadata","push_new_data"],
            mode="default",
        ),
        dg.PresetDefinition(
            "preset_wikidata",
            run_config=preset_wikidata,
            solid_selection=["query_wikidata", "wikidata_dataframe", "update_metadata","push_new_data"],
            mode="default",
        ),
        dg.PresetDefinition(
            "preset_portals",
            run_config=preset_portals,
            solid_selection=["query_portals", "portals_dataframe", "update_metadata","push_new_data"],
            mode="default",
        ),
    ],
)
def apis_pipeline():

    omeka_results = query_omeka()
    omeka_df = omeka_dataframe(omeka_results)
    # omeka_df = validate_omeka(omeka_df)
    ok_omeka = update_metadata(df=omeka_df)
    push_new_data(ok_omeka)

    wikidata_results = query_wikidata()
    wikidata_df = wikidata_dataframe(wikidata_results)
    # wikidata_df = validate_wikidata(wikidata_df)
    ok_wikidata = update_metadata(df=wikidata_df)
    push_new_data(ok_wikidata)

    portals_results = query_portals()
    portals_df = portals_dataframe(portals_results)
    # portals_df = validate_portals(portals_df)
    ok_portals = update_metadata(df=portals_df)
    push_new_data(ok_portals)


################   SENSORS   ##################


@dg.sensor(pipeline_name="apis_pipeline", minimum_interval_seconds=120)
def trigger_apis(context):
    api_wikidata = "data/output/api_wikidata.csv"
    api_portals = "data/output/api_portals.csv"
    api_omeka = "data/output/api_omeka.csv"
    now = datetime.now().strftime("%d/%m/%Y%H%M%S")
    apis = [api_omeka, api_portals, api_wikidata]

    for item in apis:
        if not os.path.exists(item):
            # if not os.path.exists(f"data/output/{item}.csv"):
            run_key = f"{item}_{now}"

            yield dg.RunRequest(run_key=run_key, run_config=preset)


def test_sensor():
    for run_request in trigger_apis(None):
        assert dg.validate_run_config(dg.log_file_pipeline, run_request.run_config)


################   SCHEDULES   ##################


@dg.schedule(
    cron_schedule="0 0 * * 0",
    pipeline_name="apis_pipeline",
    execution_timezone="America/Sao_Paulo",
)
def apis_pipeline_weekly():
    return {}
