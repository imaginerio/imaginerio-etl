from os import write
import dagster as dg
from dotenv import load_dotenv
from solids.IIIF import *
from solids.utils import *


load_dotenv(override=True)


preset = {
    "solids": {
        "create_manifest": {"config": {"tmp_path": {"env": "TMP"}, "upload": True}},
    },
    "resources": {
        "metadata_root": {"config": {"env": "METADATA"}},
        "import_omeka_root": {"config": {"env": "IMPORT_OMEKA"}},
    },
}

preset_debug = {
    "solids": {
        "create_manifest": {"config": {"tmp_path": {"env": "TMP"}, "upload": False}},
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
            solid_selection=["list_of_items", "create_manifest"],
            mode="default",
        ),
        dg.PresetDefinition(
            "Debbug",
            run_config=preset_debug,
            solid_selection=["list_of_items", "create_manifest"],
            mode="default",
        ),
    ],
)
def IIIF_pipeline():
    to_do = list_of_items()
    manifest = create_manifest(to_do=to_do)
