from dagster import schedule


# https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules
@schedule(
    cron_schedule="0 18 * * 1-5",
    pipeline_name="images_pipeline",
    execution_timezone="America/Sao_Paulo",
)
def daily():
    return {}
