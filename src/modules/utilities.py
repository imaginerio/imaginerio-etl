import pandas as pd
import dagster as dg
from dotenv import load_dotenv

load_dotenv(override=True)


@dg.solid(config_schema=dg.StringSource)
def read_csv(context):
    path = context.solid_config
    df = pd.read_csv(path)
    return df


run_config = {
    "solids": {
        "camera": {"config": {"env": "CAMERA_CSV"}},
        "images": {"config": {"env": "IMAGES"}},
    }
}

read_camera = read_csv.alias("camera")
read_images = read_csv.alias("images")


@dg.pipeline
def utilities_main():
    read_camera()
    read_images()


if __name__ == "__main__":
    result = dg.execute_pipeline(utilities_main, run_config=run_config)

