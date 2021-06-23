# Situated Views of Rio de Janeiro

## Commands



```bash

# Launch Dagit for monitoring pipelines, sensors and schedules
dagit

# Launch daemon
dagster-daemon run

# Force pipeline execution
dagster pipeline execute -f some_pipeline.py --preset some_preset

# Test sensor 
dagster sensor preview my_sensor_name
```