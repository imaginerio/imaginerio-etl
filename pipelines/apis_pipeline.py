import os
import dagster as dg
from datetime import datetime
from dagster.core.definitions import mode
from dotenv import load_dotenv


from solids.utils import (
    df_csv_io_manager,
    update_metadata,
    root_input_csv,
)  # slack_solid
from solids.apis import (
    portals_dataframe,
    query_portals,
    query_wikidata,
    query_omeka,
    omeka_dataframe,
    wikidata_dataframe,
)

load_dotenv(override=True)

preset = {
    "solids": {
        "query_omeka": {"config": {"env": "OMEKA_API"}},
        "query_wikidata": {"config": {"env": "WIKIDATA_API"}},
        "query_portals": {"config": {"env": "PORTALS_API"}},
        "portals_dataframe": {"config": {"env": "PORTALS_PREFIX"}},
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
                "metadata_root": root_input_csv,
            },
        )
    ],
    preset_defs=[
        dg.PresetDefinition(
            "default",
            run_config=preset,
            mode="default",
        )
    ],
)
def apis_pipeline():

    omeka_results = query_omeka()
    omeka_df = omeka_dataframe(omeka_results)
    update_metadata(df=omeka_df)

    wikidata_results = query_wikidata()
    wikidata_df = wikidata_dataframe(wikidata_results)
    update_metadata(df=wikidata_df)

    portals_results = query_portals()
    portals_df = portals_dataframe(portals_results)
    update_metadata(df=portals_df)


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
def weekly():
    return {}
