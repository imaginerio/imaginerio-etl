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
        return pd.read_csv(file_path + ".csv")
    
    def handle_output(self, context, obj):
        file_path = os.path.join("data-out", context.name)
        obj.to_csv(file_path + ".csv", index=False)

        yield dg.AssetMaterialization(asset_key = dg.AssetKey(file_path), description = "saved csv")
        yield dg.EventMetadataEntry.int(obj.shape[0], label="number of rows")

@dg.io_manager
def df_csv_io_manager(init_context):
    return PandasCsvIOManager()


class GeojsonIOManager(dg.IOManager):
    def load_input(self, context):
        file_path = os.path.join("data-out", context.upstream_output.name)
        return geojson.loads(file_path + ".geojson")
    
    def handle_output(self, context, feature_collection):
        file_path = os.path.join("data-out", context.name)
        with open(f'{file_path}.geojson', "w", encoding="utf-8") as f:
            geojson.dump(feature_collection, f, ensure_ascii=False, indent=4)        

        yield dg.AssetMaterialization(asset_key = dg.AssetKey(file_path), description = "saved geojson")
        

@dg.io_manager
def geojson_io_manager(init_context):
    return GeojsonIOManager()

@dg.solid
def rename_column(context,df,dic):
    df = df.rename(columns = dic)
    return df

@dg.solid(
    input_defs=[dg.InputDefinition("metadata", root_manager_key="metadata_root")],
    output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="metadata")]    
)
def merge_dfs(context,metadata,df):
        metadata = metadata.merge(df, how="outer", on="id")
        id = metadata.pop('id')
        metadata.insert(0, 'id', id)
        context.log.info(f'{df.name} merged on metadata') 
        return metadata
    

@dg.root_input_manager
def root_input(context):
    return pd.read_csv(context.config['path'])

@dg.root_input_manager
def root_input_xml(context):
    path = context.config['path']
    with open(path, encoding="utf8") as f:
        tree = ElementTree.parse(f)
    root = tree.getroot()
    return root       

@dg.solid(required_resource_keys={'slack'})
def slack_solid(context):
    context.resources.slack.chat_postMessage(channel='#tutoriais-e-links', text=':wave: teste!')

@dg.solid(input_defs=[dg.InputDefinition( "omeka", root_manager_key= "omeka_root"), dg.InputDefinition( "catalog", root_manager_key= "catalog_root"), dg.InputDefinition( "wikidata", root_manager_key= "wikidata_root")], output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="metadatateste")])
def create_metadata(context, omeka, catalog, wikidata):
    dataframes = [omeka, catalog, wikidata]
    df1 = pd.DataFrame(columns=['id'])
    for df in dataframes:       
        metadata = df1.merge(df, how="outer", on="id")
        id = metadata.pop('id')
        metadata.insert(0, 'id', id) 
       
    return metadata
       

    
