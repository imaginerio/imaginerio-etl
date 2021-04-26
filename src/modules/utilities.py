import numpy as np
import pandas as pd
import dagster as dg

#objetivo: ler qualquer csv e retornar um df 

@dg.solid
def read_csv(context):
    path = context.solid_config
    df = pd.read_csv(path)
    return df

@dg.pipeline
def utilities_main():
    read_camera = read_csv.alias("camera")
    read_images = read_csv.alias("images")
    read_camera()
    read_images()
