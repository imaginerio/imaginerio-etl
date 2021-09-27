import os
from datetime import datetime
import dagster as dg
from dotenv import load_dotenv
from solids.cumulus import *
from solids.utils import *

load_dotenv(override=True)

preset = {
    "resources": {
        "cumulus_root": {"config": {"env": "CUMULUS_XML"}},
        "metadata_root": {"config": {"env": "METADATA"}},
        "push_new_data":{"config":"Cumulus"},
    }
}


@dg.pipeline(
    mode_defs=[
        dg.ModeDefinition(
            name="default",
            resource_defs={
                "pandas_csv": df_csv_io_manager,
                "cumulus_root": root_input_xml,
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
def cumulus_pipeline():
    cumulus_df = xml_to_df()
    cumulus_df = organize_columns(cumulus_df)
    cumulus_df = extract_dimensions(cumulus_df)
    listed_creators = creators_list(cumulus_df)
    cumulus_df = format_date(cumulus_df)
    cumulus_df = create_columns(cumulus_df)
    select_columns(cumulus_df)
    update_metadata(df=cumulus_df)
    push_new_data()


################   SENSORS   ##################


@dg.sensor(pipeline_name="cumulus_pipeline")
def trigger_cumulus(context):
    last_mtime = datetime(context.cursor) if context.cursor else 0
    max_mtime = last_mtime
    fstats = os.stat("data/input/cumulus.xml")
    file_mtime = fstats.st_mtime

    if file_mtime <= last_mtime:
        run_key = f"cumulus.xml:{str(file_mtime)}"
        yield dg.RunRequest(run_key=run_key, run_config=preset)

        max_mtime = max(max_mtime, file_mtime)
        context.update_cursor(str(max_mtime))
