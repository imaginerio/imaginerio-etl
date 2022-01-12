import os
from datetime import datetime

from dagster import job
from dotenv import load_dotenv
from pandas.core.frame import DataFrame
from ops.apis import *
from ops.update_metadata import update_metadata
from resources.csv_root_input import csv_root_input
from resources.df_csv_io_manager import df_csv_io_manager

load_dotenv(override=True)


@job(
    resource_defs={
        "pandas_csv": df_csv_io_manager,
        "metadata_root": csv_root_input,
    },
    config={
        "ops": {
            "query_wikidata": {"config": {"env": "WIKIDATA_API"}},
            "query_portals": {"config": {"env": "PORTALS_API"}},
            "portals_dataframe": {"config": {"env": "PORTALS_PREFIX"}},
        },
        "resources": {"metadata_root": {"config": {"env": "METADATA"}}},
    },
)
def query_apis():

    wikidata_results = query_wikidata()
    wikidata_df = wikidata_dataframe(wikidata_results)
    # wikidata_df = validate_wikidata(wikidata_df)
    ok_wikidata = update_metadata(df=wikidata_df)

    portals_results = query_portals()
    portals_df = portals_dataframe(portals_results)
    # portals_df = validate_portals(portals_df)
    ok_portals = update_metadata(df=portals_df)
