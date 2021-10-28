import dagster as dg
from dotenv import load_dotenv
from solids.export import *
from utils.csv_root_input import csv_root_input
from utils.geojson_root_input import geojson_root_input
from utils.df_csv_io_manager import df_csv_io_manager
from solids.push_new_data import push_new_data

load_dotenv(override=True)

# Switch commits to another branch, change preset.
# from dagster_slack import slack_resource
preset = {
    "resources": {
        "metadata_root": {"config": {"env": "METADATA"}},
        "smapshot_root": {"config": {"env": "SMAPSHOT"}},
        "camera_root":{"config":{"env": "CAMERA"}},
        "cumulus_root":{"config":{"env": "CUMULUS"}},
        "images_root":{"config":{"env": "IMAGES"}},
        "omeka_root":{"config":{"env": "OMEKA"}},
        "wikidata_root":{"config":{"env": "WIKIDATA"}},
        "mapping_root":{"config":{"env": "MAPPING"}},
        "portals_root": {"config": {"env": "PORTALS"}},
    },
    "solids": {
        "export_html": {"config": {"env": "INDEX"}},
        "push_new_data":{
            "config":{
                "commit":"import omeka/wikidata and dashboard",
                "branch":"dev"}},
    },
}

preset_html = {
    "resources": {
        "camera_root":{"config":{"env": "CAMERA"}},
        "portals_root": {"config": {"env": "PORTALS"}},
        "cumulus_root":{"config":{"env": "CUMULUS"}},
        "images_root":{"config":{"env": "IMAGES"}},
        "omeka_root":{"config":{"env": "OMEKA"}},
        "wikidata_root":{"config":{"env": "WIKIDATA"}}
    },
    "solids": {
        "export_html": {"config": {"env": "INDEX"}},
        "push_new_data":{"config":"dashboard"},
    },
}


@dg.pipeline(
    mode_defs=[
        dg.ModeDefinition(
            name="default",
            resource_defs={
                "pandas_csv": df_csv_io_manager,
                "metadata_root": csv_root_input,
                "smapshot_root": csv_root_input,
                "cumulus_root": csv_root_input,
                "wikidata_root": csv_root_input,
                "camera_root": geojson_root_input,
                "images_root": csv_root_input,
                "omeka_root": csv_root_input,
                "mapping_root": csv_root_input,
                "portals_root": csv_root_input
            },
        )
    ],
    preset_defs=[
        dg.PresetDefinition(
            "default",
            run_config=preset,
            mode="default",
        ),
        dg.PresetDefinition(
            "preset_html",
            run_config=preset_html,
            solid_selection=["format_values_chart","create_hbar","create_pie","export_html","push_new_data"],
            mode="default",
        )
    ])
def export_pipeline():

    export_df = load_metadata()

    # Import OMEKA

    omeka_df = organize_columns_to_omeka(export_df)
    omeka_df = import_omeka_dataframe(df=omeka_df)

    # Import WIKIDATA
    wikidata_df = make_df_to_wikidata(export_df)
    wikidata_df = organise_creator(quickstate=wikidata_df)

    #push_new_data(omeka_df)
    push_new_data(wikidata_df)


    # index.html
    values = format_values_chart()
    plot_hbar = create_hbar(values)
    plot_pie = create_pie(values)
    graph_html = export_html(plot_hbar, plot_pie)

    push_new_data(graph_html)


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
