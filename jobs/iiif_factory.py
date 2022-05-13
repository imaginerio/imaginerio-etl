from dagster import ExperimentalWarning, graph
from dotenv import load_dotenv
from ops.iiif import *
import warnings

load_dotenv(override=True)

warnings.filterwarnings("ignore", category=ExperimentalWarning)


@graph
def iiif_factory():
    items = get_items()
    tiles = items.map(tile_image)
    manifests = tiles.map(write_manifest)
    upload_collections(manifests.collect())
