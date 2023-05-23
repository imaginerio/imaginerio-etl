# Situated Views of Rio de Janeiro


## Commands

```bash

# Parse Cumulus data, query Cumulus Portals and pull new images
# (requires Cumulus XML export)
python scripts/ims.py


<<<<<<< HEAD
# Force pipeline execution
dagster job execute -f jobs/some_job.py

# Test manifest creation (uses first item in metadata.csv and saves manifest/tiles locally)
dagster job execute -f situated_views.py -r test_repo -j iiif_factory

# Run manifest creation (processes all new or modified items and uploads manifests and tiles to S3)
dagster job execute -f situated_views.py -r prod_repo -j iiif_factory

# cloning submodule
git submodule update --init --recursivedagit
=======
# Generate a single manifest and tile image if needed (default mode, nothing is uploaded)
# (requires JSTOR data and vocabulary XLS exports)
python scripts/iiif.py --mode test [--index] # defaults to first item

# Generate IIIF manifests, tile unprocessed images, update collections
# (requires JSTOR data and vocabulary XLS exports)
python scripts/iiif.py --mode prod


# Parse KMLs and generate viewcones geojson
python scripts/viewcones.py

```
>>>>>>> dev
