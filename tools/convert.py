# SCRIPT TO CONVERT COORDINATES FOR MAP.PY

import pandas as pd
import geopandas as gpd

from pyproj import Proj, transform
from shapely import wkt

def create_geo_df(path):
    """
    Return a geodataframe from metadata.csv
    """

    # reads CSV file
    df_file = pd.read_csv(path)
    df_file['geometry'] = df_file['geometry'].apply(wkt.loads)

    # transfoms in geodataframe
    geo_df = gpd.GeoDataFrame(df_file, geometry = 'geometry', crs ='epsg:4326')

    return geo_df

def LongLat_to_EN(x,y):
    """
    Transforms the projections

            Parameters:
                x (float): coordinate X
                y (float): coordinate Y 
            Returns:
                e (float): coordinate E
                n (float): coordinate N
    """

    try:
      e,n = transform(Proj('epsg:4326'), Proj('epsg:3857'), x,y)
      return e,n
    except:
      return None

def convert_coord (geo_df):
    """
    Convert WGS84(epsg:4326) to WEBMERCATOR (epsg:3857) coordinates
    """

    # convert and split coordinates adding in 2 columns differents
    geo_df['coordinates'] = geo_df.apply(lambda x: LongLat_to_EN(x['lat'],x['lng']),axis=1)
    geo_df = geo_df.join(pd.DataFrame(geo_df['coordinates'].values.tolist(), columns=['e','n']))
    geo_df['geometry'] = geo_df['geometry'].to_crs('epsg:3857')

    # reduce columns and return final geodataframe
    final_gdf = geo_df[['identifier','title', 'author', 'image','geometry','e','n']]
    return final_gdf

def create_dataset_map (gdf):
    """
    Create a CSV file
    """
    gdf.to_csv("./metadata/dataset_map.csv", index=False)

    return None


def main ():
    """
    Execute all functions:
    """

    # path variables

    METADATA_PATH = "./metadata/metadata.csv"

    try:
        
        print ("Creating a geodataframe...")
        geo_df = create_geo_df(METADATA_PATH)
        print(geo_df.head())

        print ("Converting a coordinates...")
        final_gdf = convert_coord(geo_df)
        print(final_gdf.head())

        print ("Creating a dataset for map...")
        dataset_map = create_dataset_map(final_gdf)

    except Exception as e:

        print(str(e))


if __name__ == "__main__":
    main()
    # geopandas saves all geodata in metadata/dataset_map.csv
