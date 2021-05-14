from typing import Any
import dagster as dg
import pandas as pd
import requests
#import geopandas as gpd
import os
from urllib3.util import Retry
from requests.adapters import HTTPAdapter


class PandasCsvIOManager(dg.IOManager):
    def load_input(self, context):
        file_path = context.config.upstream_output.name
        return pd.read_csv(file_path + ".csv")
    
    def handle_output(self, context, obj):
        file_path = os.path.join("data-out", context.name)
        obj.to_csv(file_path + ".csv", index=False)

        yield dg.AssetMaterialization(asset_key = dg.AssetKey(file_path), description = "saved csv")

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
    input_defs=[dg.InputDefinition("metadata", root_manager_key="metadata_root")],
    output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="metadata")]    
)
def merge_dfs(_,metadata,df):
    metadata = df.combine_first(metadata)

    return metadata

@dg.root_input_manager
def root_input(context):
    return pd.read_csv(name=context.config["path"])

@dg.solid(required_resource_keys={'slack'})
def slack_solid(context):
    context.resources.slack.chat_postMessage(channel='#tutoriais-e-links', text=':wave: testeeeeee!')