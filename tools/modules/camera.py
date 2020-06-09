import pandas as pd
import geopandas as gpd


def load(path):
    try:
        cameras = load_camera(path)
        cones = load_cones(path)

        df = pd.merge(cameras, cones, on=["id"], how="left", validate="one_to_one")

        print("Camera loaded \n")
        print(df.head())

        return df

    except Exception as e:
        print(str(e))


def load_camera(path):
    """
    Return dataframe from camera.csv
    """

    # read csv
    camera = pd.read_csv(path + "camera.csv")

    # rename columns
    camera = camera.rename(columns={"name": "id", "long": "lng",})

    # drop duplicates
    camera = camera.drop_duplicates(subset="id", keep="last")

    return camera


def load_cones(path):
    """
    Return geodataframe from camera.geojson
    """

    # read geojson
    viewcone = gpd.read_file(path + "camera.geojson")

    # rename columns
    viewcone = viewcone.rename(columns={"name": "id",})

    # subset by columns
    viewcone = viewcone[["id", "geometry"]]

    # remove duplicates
    viewcone = viewcone.drop_duplicates(subset="id", keep="last")

    return viewcone
