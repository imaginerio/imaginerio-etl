import dagster as dg
from solids.export import *
from solids.utils import df_csv_io_manager, geojson_io_manager, root_input_csv
from dotenv import load_dotenv

load_dotenv(override=True)

# from dagster_slack import slack_resource
preset = {
    "resources": {
        "metadata_root": {"config": {"env": "METADATA"}},
        "jstor_root": {"config": {"env": "JSTOR"}},
        "smapshot_root":{"config":{"env":"SMAPSHOT"}}
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
                "smapshot_root": root_input_csv,
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

    omeka_df = organize_columns_to_omeka(export_df)
    omeka_df = import_omeka_dataframe(df=omeka_df)

    # Import WIKIDATA
    wikidata_df = make_df_to_wikidata(export_df)
    wikidata_df = organise_creator(quickstate=wikidata_df)

    # index.html
    #graph_html = format_values_chart(export_df)
    #plot_hbar = create_hbar(graph_html[0])
    #plot_pie = create_pie(graph_html[1])
    #graph_html = export_html(plot_hbar,plot_pie)


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
            run_config=preset,
            tags={"source_pipeline": event.pipeline_name},
        )


# CLI: dagit -f pipelines/export_pipeline.py
