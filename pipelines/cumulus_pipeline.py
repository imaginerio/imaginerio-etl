import os

import dagster as dg
from dotenv import load_dotenv
from solids.cumulus import *
from solids.utils import *


load_dotenv(override=True)


preset = {
    "resources": {
        "metadata_root": {"config": {"env": "METADATA"}},
        "xml_root": {"config": {"env": "CUMULUS_XML"}},
        # "current_df_root": {"config": {"env": "CUMULUS"}},
    }
}


@dg.pipeline(
    mode_defs=[
        dg.ModeDefinition(
            name="default",
            resource_defs={
                "pandas_csv": df_csv_io_manager,
                "metadata_root": root_input_csv,
                "xml_root": root_input_xml,
                # "current_df_root": root_input_csv,
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
    cumulus_df = dates_accuracy(cumulus_df)
    update_metadata(df=cumulus_df)


################   SENSORS   ##################


@dg.sensor(pipeline_name="cumulus_pipeline")
def trigger_cumulus(context):
    last_mtime = float(context.cursor) if context.cursor else 0

    max_mtime = last_mtime

    fstats = os.stat("/mnt/y/projetos/getty/cumulus.xml")
    file_mtime = fstats.st_mtime
    if file_mtime <= last_mtime:
        return

    # the run key should include mtime if we want to kick off new runs based on file modifications
    run_key = f"cumulus.xml:{str(file_mtime)}"
    # run_config = {"solids": {"process_file": {"config": {"filename": filename}}}}
    yield dg.RunRequest(run_key=run_key)

    max_mtime = max(max_mtime, file_mtime)

    context.update_cursor(str(max_mtime))
