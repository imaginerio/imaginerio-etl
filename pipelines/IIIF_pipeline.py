import dagster as dg
from dotenv import load_dotenv
from solids.IIIF import *
from solids.utils import *
from solids.export import *

load_dotenv(override=True)


preset = {
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
            mode="default",
        )
    ],
)
def IIIF_pipeline():
    df = load_metadata()
    create_manifest(df)
