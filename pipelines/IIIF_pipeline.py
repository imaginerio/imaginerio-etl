from os import write
import dagster as dg
from dotenv import load_dotenv
from solids.IIIF import *
from solids.utils import *


load_dotenv(override=True)


preset = {
    "solids": {
        "create_manifest": {
            "solids": {
                "image_tiling": {"config": {"env": "TMP"}},
                "write_manifest": {"config": {"env": "TMP"}},
                "write_collection": {"config": {"env": "TMP"}},
                "upload_to_cloud2": {"config": {"env": "TMP"}},
            }
        }
    },
    "resources": {
        "metadata_root": {"config": {"env": "METADATA"}},
        "import_omeka_root": {"config": {"env": "IMPORT_OMEKA"}},
    },
}

preset_debug = {
    "solids": {
        "image_tiling": {"config": {"env": "TMP"}},
        "write_manifest": {"config": {"env": "TMP"}},
        "write_collection": {"config": {"env": "TMP"}},
    },
    "resources": {
        "metadata_root": {"config": {"env": "METADATA"}},
        "import_omeka_root": {"config": {"env": "IMPORT_OMEKA"}},
    },
}


@dg.pipeline(
    mode_defs=[
        dg.ModeDefinition(
            name="default",
            resource_defs={
                "metadata_root": root_input_csv,
                "import_omeka_root": root_input_csv,
            },
        )
    ],
    preset_defs=[
        dg.PresetDefinition(
            "default",
            run_config=preset,
            solid_selection=["create_manifest"],
            mode="default",
        ),
        dg.PresetDefinition(
            "Debbug",
            run_config=preset_debug,
            solid_selection=[
                "list_of_items",
                "image_tiling",
                "write_manifest",
                "write_collection",
            ],
            mode="default",
        ),
    ],
)
def IIIF_pipeline():
    list = list_of_items()
    create_manifest(list)

    info = image_tiling(list)
    manifest = write_manifest(info)
    write_collection(manifest)
