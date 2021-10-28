import datetime
import re
from datetime import datetime

import dagster as dg
import dagster_pandas as dp
import pandas as pd
from dotenv import load_dotenv
from utils.df_csv_io_manager import df_csv_io_manager
from utils.csv_root_input import csv_root_input
from utils.xls_root_input import xls_root_input
from utils.geojson_root_input import geojson_root_input
from solids.push_new_data import push_new_data
from tests.dataframe_types import *
from tests.objects_types import *

from functools import reduce
load_dotenv(override=True)

# Switch commits to another branch, change preset.
preset = {
    "resources": {
        "cumulus_root": {"config": {"env": "CUMULUS"}},
        "wikidata_root": {"config": {"env": "WIKIDATA"}},
        "portals_root": {"config": {"env": "PORTALS"}},
        "camera_root": {"config": {"env": "CAMERA"}},
        "images_root": {"config": {"env": "IMAGES"}},
        "jstor_root": {"config": {"env": "JSTOR_XLS"}},
    },
    # "solids":{
    #     "push_new_data":{
    #         "config":{
    #             "commit":"Metadata",
    #             "branch":"dev"
    #         }
    #     }
    # }
}


################   SOLIDS   ##################


@ dg.solid(
    input_defs=[
        dg.InputDefinition("cumulus", root_manager_key="cumulus_root"),
        dg.InputDefinition("wikidata", root_manager_key="wikidata_root"),
        dg.InputDefinition("portals", root_manager_key="portals_root"),
        dg.InputDefinition("camera", root_manager_key="camera_root"),
        dg.InputDefinition("images", root_manager_key="images_root"),
    ], output_defs=[dg.OutputDefinition(dagster_type=pd.DataFrame)]
)
def create_metadata(context, cumulus: main_dataframe_types, wikidata: main_dataframe_types, portals: main_dataframe_types, camera: gpd.GeoDataFrame, images: main_dataframe_types):
    camera_new = camera[
        [
            "Source ID",
            "Longitude",
            "Latitude",
        ]
    ]
    
    dfs = [cumulus, camera_new, images, portals, wikidata]
    metadata = reduce(lambda left, right: pd.merge(left,
                                                   right, how="left", on='Source ID'), dfs)

    # find itens who are not in metadata
    def review_items(df1, df2):
        filter = df2["Source ID"].isin(df1["Source ID"])
        review = list(df2["Source ID"].loc[~filter])
        context.log.info(f"{len(review)} Items to review on :  {review}")

    review_items(metadata, camera_new)
    review_items(metadata, images)

    metadata_new = metadata[
        [
            "Source ID",
            "Title",
            "Creator",
            # vazio ou string fixa feito no cumulus ok
            "Description (English)",
            "Description (Portuguese)",
            "Date",
            "First Year",
            "Last Year",
            "Type",
            "Item Set",
            "Source",
            "Source URL",  # url do portals
            "Materials",
            "Fabrication Method",
            "Rights",  # vazio ou string fixa feito no cumulus ok
            "License",  # vazio ou string fixa feito no cumulus ok
            "Attribution",  # vazio ou string fixa feito no cumulus ok
            "Width (mm)",
            "Height (mm)",
            "Latitude",  # camera
            "Longitude",  # camera
            "Depicts",  # wikidata
            "Wikidata ID",  # id do wikiddata
            "Smapshot ID",  # vazio
            "Media URL",  # Media URL do images
        ]
    ]

    metadata.name = "metadata"
    return metadata_new


@ dg.solid(
    input_defs=[dg.InputDefinition( "jstor", root_manager_key="jstor_root", dagster_type=pd.DataFrame)],
    output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="metadata")])
def metadata_jstor(context, jstor, metadata):
    jstor = jstor.rename(columns=lambda x: re.sub(r'\[[0-9]*\]','',x))
    jstor = jstor.rename(columns={"Title original Language":"Title"})
    jstor["Source ID"] = jstor["SSID"]
    jstor["Item Set"] = jstor["Item Set"].fillna("All")
    jstor.loc[~jstor["Item Set"].str.contains("All"),"Item Set"] = jstor["Item Set"].astype(str) + "||All"
    metadata = metadata.append(jstor)

    metadata_new = metadata[
        [
            "Source ID",
            "SSID",
            "Title",
            "Creator",
            # vazio ou string fixa feito no cumulus ok
            "Description (English)",
            "Description (Portuguese)",
            "Date",
            "First Year",
            "Last Year",
            "Type",
            "Item Set",
            "Source",
            "Source URL",  # url do portals
            "Materials",
            "Fabrication Method",
            "Rights",  # vazio ou string fixa feito no cumulus ok
            "License",  # vazio ou string fixa feito no cumulus ok
            "Attribution",  # vazio ou string fixa feito no cumulus ok
            "Width (mm)",
            "Height (mm)",
            "Latitude",  # camera
            "Longitude",  # camera
            "Depicts",  # wikidata
            "Wikidata ID",  # id do wikiddata
            "Smapshot ID",  # vazio
            "Media URL",  # Media URL do images
        ]
    ]

    metadata["SSID"].astype(str)
    metadata_new.name = "metadata"
    return metadata_new.set_index("Source ID")


################   PIPELINE   ##################


@ dg.pipeline(
    mode_defs=[
        dg.ModeDefinition(
            name="default",
            resource_defs={
                "pandas_csv": df_csv_io_manager,
                "cumulus_root": csv_root_input,
                "jstor_root": xls_root_input,
                "wikidata_root": csv_root_input,
                "portals_root": csv_root_input,
                "camera_root": geojson_root_input,
                "images_root": csv_root_input,
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
    metadata = create_metadata()
    ok = metadata_jstor(metadata=metadata)
    #push_new_data(ok)


################   SENSORS   ##################


@ dg.sensor(pipeline_name="metadata_pipeline")
def trigger_metadata(context):
    metadata = "data/output/metadata.csv"
    if not os.path.exists(metadata):
        now = datetime.now().strftime("%d/%m/%Y%H%M%S")
        run_key = f"metadata_{now}"
        yield dg.RunRequest(run_key=run_key)
