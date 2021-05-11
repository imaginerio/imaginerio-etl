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
def save_csv(context, dataframes):
    for df in dataframes:
        df.to_csv(df.name, index=False)

@dg.solid
def merge(context, df, dataframes):
    #using reduce?
    final_df = df
    for dataframe in dataframes:
        final_df = pd.merge(
            final_df, dataframe, on=["id"], how="left", validate="one_to_one",
        )
    return final_df

@dg.solid
def rename_column(context,df,dic):
    df = df.rename(columns = dic)
    return df

@dg.io_manager
def df_csv_io_manager(init_context):
    return PandasCsvIOManager()


#@dg.solid(config_schema=StringSource)
@dg.solid
def query_api(context):
# start session
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    endpoint = (context.solid_config)
    response = http.get(endpoint, params={"per_page": 1}) 
    return response.json()