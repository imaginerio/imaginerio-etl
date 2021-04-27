import pandas as pd
import dagster as dg


@dg.solid
def read_csv(context):
    path = context.solid_config
    df = pd.read_csv(path)
    df.name = path.split("/")[-1]
    return df


@dg.solid
def save_csv(context, df):
    df.to_csv(df.name, index=False)


"""
@dg.solid
def merge(context)



run_config = {
    "solids": {
        "camera": {"config": {"env": "CAMERA_CSV"}},
        "images": {"config": {"env": "IMAGES"}},
    }
}
"""

read_camera = read_csv.alias("camera")
read_images = read_csv.alias("images")

