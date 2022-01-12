import os
from datetime import datetime
from dagster import job
from dotenv import load_dotenv
from ops.cumulus import *
from ops.update_metadata import update_metadata
from resources.csv_root_input import csv_root_input
from resources.xml_root_input import xml_root_input
from resources.df_csv_io_manager import df_csv_io_manager

load_dotenv(override=True)


@job(
    resource_defs={
        "pandas_csv": df_csv_io_manager,
        "cumulus_root": xml_root_input,
        "metadata_root": csv_root_input,
    },
    config={
        "resources": {
            "cumulus_root": {"config": {"env": "CUMULUS_XML"}},
            "metadata_root": {"config": {"env": "METADATA"}},
        }
    },
)
def format_ims_metadata():
    cumulus_df = xml_to_df()
    cumulus_df = organize_columns(cumulus_df)
    cumulus_df = extract_dimensions(cumulus_df)
    creators_list(cumulus_df)
    cumulus_df = format_date(cumulus_df)
    cumulus_df = create_columns(cumulus_df)
    select_columns(cumulus_df)
    update_metadata(df=cumulus_df)
