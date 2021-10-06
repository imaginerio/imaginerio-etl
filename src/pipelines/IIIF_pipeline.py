from os import write
import dagster as dg
from dotenv import load_dotenv
from solids.IIIF import *
from utils.csv_root_input import csv_root_input


load_dotenv(override=True)


default = {
    "solids": {
        "list_items": {"config": {"slice_debug": False}},
        "create_manifest": {"config": {"upload": True}},
    },
    "resources": {
        "metadata_root": {"config": {"env": "METADATA"}},
        "import_omeka_root": {"config": {"env": "IMPORT_OMEKA"}},
    },
}

debug = {
    "solids": {
        "list_items": {"config": {"slice_debug": True}},
        "create_manifest": {"config": {"upload": False}},
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
                "metadata_root": csv_root_input,
                "import_omeka_root": csv_root_input,
            },
        )
    ],
    preset_defs=[
        dg.PresetDefinition(
            "default",
            run_config=default,
            #solid_selection=["list_items", "create_manifest"],
            mode="default",
        ),
        dg.PresetDefinition(
            "debug",
            run_config=debug,
            #solid_selection=["list_items", "create_manifest"],
            mode="default",
        ),
    ],
)
def IIIF_pipeline():
    to_do = list_items()
    create_manifest(to_do=to_do)
