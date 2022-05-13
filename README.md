# Situated Views of Rio de Janeiro


## Commands

```bash

# Launch Dagit for monitoring pipelines, sensors and schedules
dagit

# Launch daemon
dagster-daemon run

# Force pipeline execution
dagster job execute -f jobs/some_job.py

# Test manifest creation (uses first item in metadata.csv and saves manifest/tiles locally)
dagster job execute -f situated_views.py -r test_repo -j iiif_factory

# Run manifest creation (processes all new or modified items and uploads manifests and tiles to S3)
dagster job execute -f situated_views.py -r prod_repo -j iiif_factory

# cloning submodule
git submodule update --init --recursivedagit
