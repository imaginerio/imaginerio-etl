import dagster as dg
import pandas as pd
from solids.utils import *
from datetime import datetime

from dotenv import load_dotenv

load_dotenv(override=True)

preset = {
    "resources": {
        "catalog_root": {"config": {"env": "CATALOG"}},
        "omeka_root": {"config": {"env": "OMEKA"}},
        "wikidata_root": {"config": {"env": "WIKIDATA"}},
        "portals_root": {"config": {"env": "PORTALS"}},
        "camera_root": {"config": {"env": "CAMERA"}},
        "images_root": {"config": {"env": "IMAGES"}},
    },
}

################   SOLIDS   ##################


@dg.solid(
    input_defs=[
        dg.InputDefinition("omeka", root_manager_key="omeka_root"),
        dg.InputDefinition("catalog", root_manager_key="catalog_root"),
        dg.InputDefinition("wikidata", root_manager_key="wikidata_root"),
        dg.InputDefinition("portals", root_manager_key="portals_root"),
        dg.InputDefinition("camera", root_manager_key="camera_root"),
        dg.InputDefinition("images", root_manager_key="images_root"),
    ],
    output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="metadata")],
)
def create_metadata(context, omeka, catalog, wikidata, portals, camera, images):
    camera_new = camera[
        [
            "id",
            "fov",
            "longitude",
            "altitude",
            "latitude",
            "heading",
            "tilt",
            "geometry",
        ]
    ]

    catalog[["first_year", "last_year"]] = catalog[
        ["first_year", "last_year"]
    ].applymap(lambda x: x if pd.isnull(x) else str(int(x)))

    dataframes_outer = [catalog, camera_new, images]
    dataframe_left = [portals, omeka, wikidata]
    metadata = pd.DataFrame(columns=["id"])
    print("CATALOG:", catalog["first_year"][10], type(catalog["first_year"][10]))

    for df in dataframes_outer:
        metadata = metadata.merge(df, how="outer", on="id")

    for df in dataframe_left:
        metadata = metadata.merge(df, how="left", on="id")

    metadata_new = metadata[
        [
            "id",
            "title",
            "creator",
            "date",
            "date_circa",
            "date_created",
            "date_accuracy",
            "first_year",
            "last_year",
            "description",
            "type",
            "fabrication_method",
            "image_width",
            "image_height",
            "portals_id",
            "portals_url",
            "wikidata_depict",
            "wikidata_id",
            "wikidata_image",
            "wikidata_ims_id",
            "img_hd",
            "img_sd",
            "omeka_url",
            "latitude",
            "longitude",
            "altitude",
            "heading",
            "tilt",
            "fov",
            "geometry",
        ]
    ]
    print("METADATA:", metadata["first_year"][10], type(catalog["first_year"][10]))
    metadata.name = "metadata"
    return metadata_new.set_index("id")


################   PIPELINE   ##################


@dg.pipeline(
    mode_defs=[
        dg.ModeDefinition(
            name="default",
            resource_defs={
                "pandas_csv": df_csv_io_manager,
                "catalog_root": root_input_csv,
                "omeka_root": root_input_csv,
                "catalog_root": root_input_csv,
                "wikidata_root": root_input_csv,
                "portals_root": root_input_csv,
                "camera_root": root_input_geojson,
                "images_root": root_input_csv,
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
def metadata_pipeline():
    create_metadata()


################   SENSORS   ##################


@dg.sensor(pipeline_name="metadata_pipeline")
def trigger_metadata(context):
    metadata = "data/output/metadata.csv"
    if not os.path.exists(metadata):
        now = datetime.now().strftime("%d/%m/%Y%H%M%S")
        run_key = f"metadata_{now}"
        yield dg.RunRequest(run_key=run_key)


# CLT: dagster pipeline execute -f pipelines/metadata_pipeline.py --preset default
