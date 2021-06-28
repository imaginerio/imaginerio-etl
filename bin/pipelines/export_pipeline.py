import dagster as dg
from bin.solids.export import *
from bin.solids.utils import df_csv_io_manager, geojson_io_manager, root_input_csv
from dotenv import load_dotenv

load_dotenv(override=True)

# from dagster_slack import slack_resource
preset = {
    "resources": {
        "metadata_root": {"config": {"env": "METADATA"}},
        "jstor_root": {"config": {"env": "JSTOR"}},
    },
}


@dg.pipeline(
    mode_defs=[
        dg.ModeDefinition(
            name="default",
            resource_defs={
                "pandas_csv": df_csv_io_manager,
                "jstor_root": root_input_csv,
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
def export_pipeline():

    export_df = load_metadata()

    # Import OMEKA

    omeka_organized = organize_columns_to_omeka(export_df)
    omeka_df = import_omeka_dataframe(df=omeka_organized)

    # import WIKIDATA
    wikidata_df = make_df_to_wikidata(export_df)
    organised_creator = organise_creator(quickstate=wikidata_df)


################   SENSORS   ##################


@dg.sensor(pipeline_name="export_pipeline")
def trigger_export(context):
    events = context.instance.events_for_asset_key(
        dg.AssetKey("metadata"),
        after_cursor=context.last_run_key,
        ascending=False,
        limit=1,
    )
    if events:
        record_id, event = events[0]  # take the most recent materialization
        yield dg.RunRequest(
            run_key=str(record_id),
            run_config={},
            tags={"source_pipeline": event.pipeline_name},
        )


# CLI: dagit -f bin/pipelines/export_pipeline.py
