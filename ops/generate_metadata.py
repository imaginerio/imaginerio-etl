from dagster import op, In, Out
import pandas as pd
import geopandas as gpd

from functools import reduce
from tests.dataframe_types import main_dataframe_types


@op(
    ins={
        "cumulus": In(root_manager_key="cumulus_root"),
        "wikidata": In(root_manager_key="wikidata_root"),
        "portals": In(root_manager_key="portals_root"),
        "camera": In(root_manager_key="camera_root"),
        "images": In(root_manager_key="images_root"),
    },
    out={"metadata": Out(dagster_type=pd.DataFrame)},
)
def generate_metadata(
    context,
    cumulus: main_dataframe_types,
    wikidata: main_dataframe_types,
    portals: main_dataframe_types,
    camera: gpd.GeoDataFrame,
    images: main_dataframe_types,
):
    camera_new = camera[
        [
            "Source ID",
            "Longitude",
            "Latitude",
        ]
    ]

    dfs = [cumulus, camera_new, images, portals, wikidata]
    metadata = reduce(
        lambda left, right: pd.merge(left, right, how="left", on="Source ID"), dfs
    )

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
            "Creator",  # vazio ou string fixa feito no cumulus ok
            "Description (English)",
            "Description (Portuguese)",
            "Date",
            "First Year",
            "Last Year",
            "Type",
            "Collections",
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
