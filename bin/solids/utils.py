import dagster as dg
import pandas as pd
import requests
#import geopandas as gpd
import os
from urllib3.util import Retry
from requests.adapters import HTTPAdapter


class PandasCsvIOManager(dg.IOManager):
    def load_input(self, context):
        file_path = os.path.join("src", "data-out", context.upstream_output.name)
        return pd.read_csv(file_path + ".csv")
    
    def handle_output(self, context, obj):
        file_path = os.path.join("src", "data-out", context.name)
        obj.to_csv(file_path + ".csv", index=False)

        yield dg.AssetMaterialization(asset_key = dg.AssetKey(file_path), description = "saved csv")


@dg.solid
def read_csv(context):
    path = context.solid_config
    df = pd.read_csv(path)
    df.name = path.split("/")[-1]
    return df

#@dg.solid
#def read_geojson(context):
    #path = context.solid_config
    #geometry = gpd.read_file(path)
    #geometry = geometry.rename(columns={"name":"id"})
    #geometry = geometry[["id","geometry"]]
    #geometry = geometry.drop_duplicates(subsete="id",keep ="last")
    #geometry.name = path.split("/")[-1]
    #return geometry

@dg.solid
def save_csv(context, dataframes):
    for df in dataframes:
        df.to_csv(df.name, index=False)

@dg.solid
def rename_column(context,df,dic):
    df = df.rename(columns = dic)
    return df

@dg.io_manager
def df_csv_io_manager(init_context):
    return PandasCsvIOManager()

@dg.solid(
    input_defs=[dg.InputDefinition("input1", root_manager_key="root_input")],
    output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="metadata")]    
)
def merge_dfs(_, input1, df):
    metadata = df.combine_first(input1)
    return metadata