# Situated Views of Rio de Janeiro


## Commands

```bash

# Parse Cumulus data, query Cumulus Portals and pull new images
# (requires Cumulus XML export)
python scripts/ims.py

# Generate IIIF manifests, tile unprocessed images, update collections
# (requires JSTOR data and vocabulary XLS exports)
python scripts/iiif.py

# Parse KMLs and generate viewcones geojson
python scripts/viewcones.py

```
