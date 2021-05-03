import dagster as dg
import pandas as pd
import geopandas as gpd
from modules.catalog import catalog_main


@dg.solid
def read_csv(context):
    path = context.solid_config
    df = pd.read_csv(path)
    df.name = path.split("/")[-1]
    return df

@dg.solid
def read_geojson(context):
    path = context.solid_config
    geometry = gpd.read_file(path)
    geometry = geometry.rename(columns={"name":"id"})
    geometry = geometry[["id","geometry"]]
    geometry = geometry.drop_duplicates(subsete="id",keep ="last")
    geometry.name = path.split("/")[-1]
    return geometry

@dg.solid
def save_csv(context, *dataframes):
    for df in dataframes:
        df.to_csv(df.name, index=False)

@dg.solid
def merge(context, df, *dataframes):
    #using reduce?
    final_df = df
    for dataframe in dataframes:
        final_df = pd.merge(
            final_df, dataframe, on=["id"], how="left", validate="one_to_one",
        )
    return final_df

@dg.composite_solid
def camera_main():
    cameras = read_camera().rename(columns={"long":"lng"})
    viewcones = read_geojson()
    camera = merge(cameras, viewcones)

    return camera

read_images = read_csv.alias("images")
read_camera = read_csv.alias("camera")
read_geometry = read_geojson.alias("geoCamera") 
query_portals = query_api.alias("portals")
query_wikidata = query_api.alias("wikidata")
query_omeka = query_api.alias("omeka")

@dg.pipeline
def main():
    images_df = read_images()
    camera_df = camera_main()
    catalog_df= catalog_main()
    portals_df = 
    wikidata_df = 
    omeka_df =
    metadata_df = merge_by_id(catalog_df,camera_df, portals_df, wikidata_df,omeka_df)
    # omeka = omeka(metadata)
    # wiki = wiki(metadata)
    save_csv(catalog_df, images_df, camera_df, portals_df, wikidata_df, omeka_df, metadata_df)
