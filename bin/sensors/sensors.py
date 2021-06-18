import os
import dagster as dg


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


@dg.sensor(pipeline_name="catalog_pipeline")
def trigger_catalog(context):
    last_mtime = float(context.cursor) if context.cursor else 0

    max_mtime = last_mtime

    fstats = os.stat("/mnt/y/projetos/getty/cumulus.xml")
    file_mtime = fstats.st_mtime
    if file_mtime <= last_mtime:
        return

    # the run key should include mtime if we want to kick off new runs based on file modifications
    run_key = f"cumulus.xml:{str(file_mtime)}"
    #run_config = {"solids": {"process_file": {"config": {"filename": filename}}}}
    yield dg.RunRequest(run_key=run_key)
    max_mtime = max(max_mtime, file_mtime)

    context.update_cursor(str(max_mtime))


@dg.sensor(pipeline_name="apis_pipeline", solid_selection = merge_df, query_omeka, omeka_dataframe])
def trigger_api_omeka(context):
    api_omeka = 'data-out/api_omeka.csv'
    now = datetime.now().strftime("%d/%m/%Y%H%M%S")

    if not os.path.exists (api_omeka):
        run_key = f"api_omeka_{now}"
        yield dg.RunRequest(run_key=run_key])   


@dg.sensor(pipeline_name="apis_pipeline", solid_selection = merge_df, query_wikidata, wikidata_dataframe])
def trigger_api_wikidata(context):
    api_wikidata = 'data-out/api_wikidata.csv'
    now = datetime.now().strftime("%d/%m/%Y%H%M%S")

    if not os.path.exists (api_wikidata):
        run_key = f"api_wikidata_{now}"
        yield dg.RunRequest(run_key=run_key)


@dg.sensor(pipeline_name="apis_pipeline", solid_selection = merge_df, query_portals, portals_dataframe])
def trigger_api_portals(context):    
    api_portals = 'data-out/api_portals.csv    
    now = datetime.now().strftime("%d/%m/%Y%H%M%S")
    
    if not os.path.exists (api_portals):
        run_key = f"api_portals_{now}"
        yield dg.RunRequest(run_key=run_key)


@dg.sensor
def trigger_camera(context):
    last_mtime = float(context.cursor) if context.cursor else 0

    os.walk(top[, topdown=True[, onerror=None[, followlinks=False]]])

    max_mtime = last_mtime

    fstats = os.stat("")
    file_mtime = fstats.st_mtime
    if file_mtime <= last_mtime:        
        yield dg.RunRequest(run_key=run_key)
        max_mtime = max(max_mtime, file_mtime)

    context.update_cursor(str(max_mtime))


@dg.sensor(pipeline_name="metadata_pipeline")
def trigger_metadata(context):
    metadata = "/mnt/d/ims/situated-views/data-out/metadata.csv"    
    if not os.path.exists(metadata):
        now = datetime.now().strftime("%d/%m/%Y%H%M%S")
        run_key = f"metadata_{now}"
        yield dg.RunRequest(run_key=run_key)