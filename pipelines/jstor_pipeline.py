import dagster as dg
from dotenv import load_dotenv
from solids.jstor import *
from solids.utils import *

load_dotenv(override=True)

preset = {
    "resources": {
        "jstor_root": {"config": {"env": "JSTOR"}},
        "camera_root": {"config": {"env": "CAMERA"}},
        "metadata_root": {"config": {"env": "METADATA"}},
    },
}


@dg.pipeline(
    mode_defs=[
        dg.ModeDefinition(
            resource_defs={
                "jstor_root": root_input_xls,
                "camera_root": root_input_csv,
                "pandas_csv": df_csv_io_manager,
                "metadata_root": root_input_csv,
            }
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
def jstor_pipeline():
    coodinates = geolocalization()
    jstor = rename_columns()
    jstor = select_columns(jstor=jstor, camera=coodinates)
    update_metadata(jstor)
