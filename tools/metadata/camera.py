import pandas as pd
import geopandas as gpd


def load(path):
    try:
        cameras = load_camera(path)
        cones = load_cones(path)
        df = cameras.merge(cones, on=["identifier"], how="outer")
        print("Camera loaded")
        print(80 * "-")
        print(df.head())
        print(80 * "-")
        return df

    except Exception as e:
        print(str(e))


def load_camera(path):
    """
    Return dataframe from camera.csv
    """

    # read csv
    camera = pd.read_csv(path + "camera.csv")

    # remove spaces
    camera.columns = camera.columns.str.strip().str.lower().str.replace(" ", "")

    # rename columns
    camera = camera.rename(columns={"name": "identifier", "long": "lng",})

    # drop duplicates
    camera = camera.drop_duplicates(subset="identifier", keep="last")

    return camera


def load_cones(path):
    """
    Return geodataframe from camera.geojson
    """

    # read geojson
    viewcone = gpd.read_file(path + "camera.geojson")

    # rename columns
    viewcone = viewcone.rename(columns={"name": "identifier",})

    # remove record_name file extension
    viewcone["identifier"] = viewcone["identifier"].str.split(".", n=1, expand=True)

    # subset by columns
    viewcone = viewcone[["identifier", "geometry"]]

    # remove duplicates
    viewcone = viewcone.drop_duplicates(subset="identifier", keep="last")

    return viewcone
