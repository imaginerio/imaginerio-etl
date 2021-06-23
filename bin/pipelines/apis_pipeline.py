import os
import dagster as dg
from datetime import datetime

from dotenv import load_dotenv

load_dotenv(override=True)

METADATA = os.environ["METADATA"]
OMEKA_API = os.environ["OMEKA_API"]
PORTALS_API = os.environ["PORTALS_API"]
PORTALS_API = os.environ["PORTALS_API"]
PORTALS_PREFIX = os.environ["PORTALS_PREFIX"]


from bin.solids.utils import (
    df_csv_io_manager,
    update_metadata,
    root_input_csv,
)  # slack_solid
from bin.solids.apis import (
    portals_dataframe,
    query_portals,
    query_wikidata,
    query_omeka,
    omeka_dataframe,
    wikidata_dataframe,
)


preset_apis = {
    "solids": {
        "query_omeka": {"config": {"env": "OMEKA_API"}},
        "query_wikidata": {"config": {"env": "WIKIDATA_API"}},
        "query_portals": {"config": {"env": "PORTALS_API"}},
        "portals_dataframe": {"config": {"env": "PORTALS_PREFIX"}},
    },
    "resources": {"metadata_root": {"config": {"env": "METADATA"}}},
}


@dg.pipeline(
    mode_defs=[
        dg.ModeDefinition(
            name="one",
            resource_defs={
                "pandas_csv": df_csv_io_manager,
                "metadata_root": root_input_csv,
            },
        )
    ],
    preset_defs=[
        dg.PresetDefinition(
            "one",
            run_config=preset_apis,
            mode="one",
        ),
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


# CLI: dagit -f bin/pipelines/apis_pipeline.py
# CLI: dagster pipeline execute apis_pipeline --preset one
# dagster pipeline execute -f bin/pipelines/apis_pipeline.py --preset one

################   SENSORS   ##################


@dg.sensor(pipeline_name="apis_pipeline")
def trigger_apis(context):
    api_wikidata = "data-out/api_wikidata.csv"
    api_portals = "data-out/api_portals.csv"
    api_omeka = "data-out/api_omeka.csv"
    now = datetime.now().strftime("%d/%m/%Y%H%M%S")
    apis = [api_omeka, api_portals, api_wikidata]

    for item in apis:
        if not os.path.exists(item):
            # if not os.path.exists(f"data-out/{item}.csv"):
            run_key = f"{item}_{now}"

            yield dg.RunRequest(run_key=run_key, run_config=preset_apis)


################   SCHEDULES   ##################


@dg.schedule(
    cron_schedule="0 18 * * 1-5",
    pipeline_name="apis_pipeline",
    execution_timezone="America/Sao_Paulo",
)
def daily():
    return {}
