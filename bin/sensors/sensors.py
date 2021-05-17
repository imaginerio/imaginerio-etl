import dagster as dg


@dg.sensor(pipeline_name="export")
def trigger_export(context):
    events = context.instance.events_for_asset_key(
        dg.AssetKey("Metadata"),
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
