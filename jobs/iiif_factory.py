from dagster import graph
from dotenv import load_dotenv
from ops.iiif import *

load_dotenv(override=True)


@graph
def iiif_factory():
    items = get_items()
    items.map(tile_image)
    items.map(write_manifests)
