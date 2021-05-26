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
