import os

import dagster as dg
from dotenv import load_dotenv
from solids.catalog import (
    creators_list,
    dates_accuracy,
    extract_dimensions,
    organize_columns,
    xml_to_df,
)
from solids.utils import (
    df_csv_io_manager,
    update_metadata,
    root_input_csv,
    root_input_xml,
)


load_dotenv(override=True)


preset = {
    "resources": {
        "metadata_root": {"config": {"env": "METADATA"}},
        "xml_root": {"config": {"env": "CUMULUS_XML"}},
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
def catalog_pipeline():
    catalog_df = xml_to_df()
    catalog_df = organize_columns(catalog_df)
    catalog_df = extract_dimensions(catalog_df)
    listed_creators = creators_list(catalog_df)
    catalog = dates_accuracy(catalog_df)
    update_metadata(df=catalog)


################   SENSORS   ##################

# @dg.sensor(pipeline_name="catalog_pipeline")
# def trigger_catalog(context):
#     last_mtime = float(context.cursor) if context.cursor else 0

#     max_mtime = last_mtime

#     fstats = os.stat("/mnt/y/projetos/getty/cumulus.xml")
#     file_mtime = fstats.st_mtime
#     if file_mtime <= last_mtime:
#         return

#     # the run key should include mtime if we want to kick off new runs based on file modifications
#     run_key = f"cumulus.xml:{str(file_mtime)}"
#     # run_config = {"solids": {"process_file": {"config": {"filename": filename}}}}
#     yield dg.RunRequest(run_key=run_key)
#     max_mtime = max(max_mtime, file_mtime)

#     context.update_cursor(str(max_mtime))


# CLI: dagit -f bin/pipelines/catalog_pipeline.py
# CLI: dagster pipeline execute -f bin/pipelines/catalog_pipeline.py -c bin/pipelines/catalog_pipeline.yaml
# CLI: dagster pipeline execute -f bin/pipelines/catalog_pipeline.py --preset default
