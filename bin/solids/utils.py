import os

import dagster as dg
import pandas as pd
import geopandas as gpd


@dg.solid
def read_geojson(context):
    path = context.solid_config
    geometry = gpd.read_file(path)
    geometry = geometry.rename(columns={"name": "id"})
    geometry = geometry[["id", "geometry"]]
    geometry = geometry.drop_duplicates(subsete="id", keep="last")
    geometry.name = path.split("/")[-1]
    return geometry


class PandasCsvIOManager(dg.IOManager):
    def load_input(self, context):
        file_path = os.path.join("data-out", context.upstream_output.name)
        return pd.read_csv(file_path + ".csv")

    def handle_output(self, context, obj):
        file_path = os.path.join("data-out", context.name)
        obj.to_csv(file_path + ".csv", index=False)

        yield dg.AssetMaterialization(
            asset_key=dg.AssetKey(file_path), description="saved csv"
        )


@dg.solid
def rename_column(context, df):
    df = df.rename(columns={"id": "ids"})
    return df


@dg.solid(
    input_defs=[dg.InputDefinition("metadata", root_manager_key="metadata_root")],
    output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="metadata")]    
)
def merge_dfs(_,metadata,df):
    metadata = df.combine_first(metadata)
    return metadata


@dg.io_manager
def df_csv_io_manager(init_context):
    return PandasCsvIOManager()

@dg.root_input_manager
def root_input(context):
    return pd.read_csv(name=context.config["path"])


