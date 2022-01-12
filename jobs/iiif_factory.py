from dagster import job
from dagster_aws.s3 import s3_resource
from dotenv import load_dotenv
from ops.iiif import *

# from solids.upload_to_cloud import upload_to_cloud
from resources.csv_root_input import csv_root_input
from resources.json_local_io_manager import json_local_io_manager
from resources.s3_io_manager import s3_io_manager

load_dotenv(override=True)


@job(
    resource_defs={
        "metadata_root": csv_root_input,
        "mapping_root": csv_root_input,
        "iiif_manager": s3_io_manager,
        "s3": s3_resource,
    },
    config={
        "resources": {
            "metadata_root": {"config": {"env": "METADATA"}},
            "mapping_root": {"config": {"env": "MAPPING"}},
            "iiif_manager": {"config": {"s3_bucket": "imaginerio-images"}},
        },
    },
)

#         dg.ModeDefinition(
#             name="test",
#             resource_defs={
#                 "metadata_root": csv_root_input,
#                 "mapping_root": csv_root_input,
#                 "iiif_manager": json_local_io_manager,
#             },
#         ),
#     ],
#     preset_defs=[
#         dg.PresetDefinition(
#             name="prod_all",
#             run_config=default,
#             mode="prod",
#             solid_selection=[
#                 "get_items",
#                 "tile_image",
#                 "write_manifests",
#             ],
#         ),
#         dg.PresetDefinition(
#             name="prod_manifests",
#             run_config=default,
#             mode="prod",
#             solid_selection=[
#                 "get_items",
#                 "write_manifests",
#             ],
#         ),
#         dg.PresetDefinition(
#             name="prod_tile",
#             run_config=default,
#             mode="prod",
#             solid_selection=["get_items", "tile_image"],
#         ),
#         dg.PresetDefinition(
#             name="test_all",
#             run_config=default,
#             mode="test",
#             solid_selection=[
#                 "get_items",
#                 "tile_image",
#                 "write_manifests",
#             ],
#         ),
#         dg.PresetDefinition(
#             name="test_manifests",
#             run_config=default,
#             mode="test",
#             solid_selection=["get_items", "write_manifests"],
#         ),
#         dg.PresetDefinition(
#             name="test_tile",
#             run_config=default,
#             mode="test",
#             solid_selection=["get_items", "tile_image"],
#         ),
#     ],
# )
def iiif_factory():
    items = get_items()
    items.map(write_manifests)
    items.map(tile_image)
