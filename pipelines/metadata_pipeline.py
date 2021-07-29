from datetime import datetime
import re

import dagster as dg
import pandas as pd
from dotenv import load_dotenv
from solids.utils import *

load_dotenv(override=True)


preset = {
    "resources": {
        "cumulus_root": {"config": {"env": "CUMULUS"}},
        "wikidata_root": {"config": {"env": "WIKIDATA"}},
        "portals_root": {"config": {"env": "PORTALS"}},
        "camera_root": {"config": {"env": "CAMERA"}},
        "images_root": {"config": {"env": "IMAGES"}},
        "jstor_root": {"config": {"env": "JSTOR_XLS"}},
    },
}

################   SOLIDS   ##################


@dg.solid(
    input_defs=[
        dg.InputDefinition("cumulus", root_manager_key="cumulus_root"),
        dg.InputDefinition("wikidata", root_manager_key="wikidata_root"),
        dg.InputDefinition("portals", root_manager_key="portals_root"),
        dg.InputDefinition("camera", root_manager_key="camera_root"),
        dg.InputDefinition("images", root_manager_key="images_root"),        
    ]
)
def create_metadata(context, cumulus, wikidata, portals, camera, images):    
    camera_new = camera[
        [
            "Source ID",
            "Longitude",
            "Latitude",            
        ]
    ]

    cumulus[["First Year", "Last Year"]] = cumulus[
        ["First Year", "Last Year"]
    ].applymap(lambda x: x if pd.isnull(x) else str(int(x)))

    dataframes_outer = [cumulus, camera_new, images]
    dataframe_left = [portals, wikidata]
    metadata = pd.DataFrame(columns=["Source ID"])   

    for df in dataframes_outer:
        metadata = metadata.merge(df, how="outer", on="Source ID")

    for df in dataframe_left:
        metadata = metadata.merge(df, how="left", on="Source ID")
    
    metadata_new = metadata[
        [
            "Source ID",
            "Title",
            "Creator",
            "Description (English)",  # vazio ou string fixa feito no cumulus ok
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

@dg.solid(
    input_defs=[dg.InputDefinition("jstor", root_manager_key="jstor_root")],
    output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="metadata")])
def metadata_jstor(context, jstor, metadata):
        jstor = jstor.rename(columns=lambda x: re.sub(r'\[[0-9]*\]','',x)) 
        jstor["Source ID"] = jstor["SSID"]  
        metadata = metadata.append(jstor)

        metadata_new = metadata[
        [
            "Source ID",
            "SSID",
            "Title",
            "Creator",
            "Description (English)",  # vazio ou string fixa feito no cumulus ok
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
    
        metadata_new.name = "metadata"
        return metadata_new.set_index("Source ID")




################   PIPELINE   ##################


@dg.pipeline(
    mode_defs=[
        dg.ModeDefinition(
            name="default",
            resource_defs={
                "pandas_csv": df_csv_io_manager,
                "cumulus_root": root_input_csv,
                "jstor_root": root_input_xls,
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
    metadata = create_metadata()
    metadata_jstor(metadata=metadata)


################   SENSORS   ##################


@dg.sensor(pipeline_name="metadata_pipeline")
def trigger_metadata(context):
    metadata = "data/output/metadata.csv"
    if not os.path.exists(metadata):
        now = datetime.now().strftime("%d/%m/%Y%H%M%S")
        run_key = f"metadata_{now}"
        yield dg.RunRequest(run_key=run_key)
