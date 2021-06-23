import geopandas as gpd
import os
from typing import Any
from xml.etree import ElementTree

import dagster as dg
from dagster.core.definitions.events import Failure
import geojson
import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


class PandasCsvIOManager(dg.IOManager):
    def load_input(self, context):
        file_path = os.path.join("data-out", context.upstream_output.name)
        return pd.read_csv(file_path + ".csv", index_col="id")

    def handle_output(self, context, obj):
        file_path = os.path.join("data-out", context.name)

        if context.name == "metadata":
            obj.to_csv(file_path + ".csv")
        else:
            obj.to_csv(file_path + ".csv", index=False)

        yield dg.AssetMaterialization(
            asset_key=dg.AssetKey(context.name), description="saved csv"
        )
        yield dg.EventMetadataEntry.int(obj.shape[0], label="number of rows")


@dg.io_manager
def df_csv_io_manager(init_context):
    return PandasCsvIOManager()


class GeojsonIOManager(dg.IOManager):
    def load_input(self, context):
        file_path = os.path.join("data-out", context.upstream_output.name)
        return (gpd.read_file(file_path + ".geojson")).set_index("id")

    def handle_output(self, context, feature_collection):
        file_path = os.path.join("data-out", context.name) + ".geojson"
        with open(file_path, "w", encoding="utf-8") as f:
            geojson.dump(feature_collection, f, ensure_ascii=False, indent=4)

        yield dg.AssetMaterialization(
            asset_key=dg.AssetKey(file_path), description="saved geojson"
        )


@dg.io_manager
def geojson_io_manager(init_context):
    return GeojsonIOManager()


@dg.solid
def rename_column(context, df, dic):
    df = df.rename(columns=dic)
    return df


@dg.solid(
    input_defs=[dg.InputDefinition("metadata", root_manager_key="metadata_root")],
    output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="metadata")],
)
def update_metadata(_, df, metadata):
    metadata.update(df)
    return metadata


@dg.io_manager
def df_csv_io_manager(init_context):
    return PandasCsvIOManager()


@dg.root_input_manager(config_schema=dg.StringSource)
def root_input_csv(context):
    return pd.read_csv(context.resource_config, index_col="id")


@dg.root_input_manager(config_schema=dg.StringSource)
def root_input_xml(context):
    path = context.resource_config
    with open(path, encoding="utf8") as f:
        tree = ElementTree.parse(f)
    root = (tree.getroot()).set_index("id")
    return root


@dg.root_input_manager(config_schema=dg.StringSource)
def root_input_geojson(context):
    return (gpd.read_file(context.resource_config)).set_index("id")


@dg.solid(required_resource_keys={"slack"})
def slack_solid(context):
    context.resources.slack.chat_postMessage(
        channel="#tutoriais-e-links", text=":wave: teste!"
    )
