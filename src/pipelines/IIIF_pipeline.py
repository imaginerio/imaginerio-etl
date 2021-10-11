from os import write
import dagster as dg
from dotenv import load_dotenv
from solids.iiif import *
from utils.csv_root_input import csv_root_input


load_dotenv(override=True)


default = {
    "solids": {
        "iiify": {"config": {"manifest_only": False}},
    },
    "resources": {
        "metadata_root": {"config": {"env": "METADATA"}},
        "mapping_root": {"config": {"env": "MAPPING"}},
    },
}

manifest_only = {
    "solids": {
        "iiify": {"config": {"manifest_only": True}},
    },
    "resources": {
        "metadata_root": {"config": {"env": "METADATA"}},
        "mapping_root": {"config": {"env": "MAPPING"}},
    },
}

#TO-DO implement S3 and Local IO/file managers according to Mode
@dg.pipeline(
    mode_defs=[
        dg.ModeDefinition(
            name="prod",
            resource_defs={
                "metadata_root": csv_root_input,
                "mapping_root": csv_root_input,
            },
        ),
        dg.ModeDefinition(
            name="test",
            resource_defs={
                "metadata_root": csv_root_input,
                "mapping_root": csv_root_input,
            },
        ),
    ],
    preset_defs=[
        dg.PresetDefinition(
            name="prod",
            run_config=default,
            mode="prod",
        ),
        dg.PresetDefinition(
            name="manifest_only",
            run_config=manifest_only,
            mode="prod",
        ),
        dg.PresetDefinition(
            name="test",
            run_config=default,
            mode="test",
        ),
    ]
)
def IIIF_pipeline():
    iiify()
