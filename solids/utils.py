import os
import subprocess
from xml.etree import ElementTree

import dagster as dg
import dagster_pandas as dp
import geojson
import geopandas as gpd
import numpy as np
import pandas as pd
from dagster import config
from dagster.core.definitions import output
from numpy.core.numeric import NaN
from numpy.lib.arraysetops import isin
from tests.dataframe_types import *
from tests.objects_types import *


class PandasCsvIOManager(dg.IOManager):
    def load_input(self, context):
        file_path = os.path.join(
            "data", "output", context.upstream_output.name)
        df = pd.read_csv(file_path + ".csv", error_bad_lines=False)

        to_convert = {
            "SSID": function_int,
            "First Year": function_int,
            "Last Year": function_int,
            "Smapshot ID": function_int,
            "Width (mm)": function_int,
            "Height (mm)": function_int,
        }
        conversion = {}
        for column in df.columns:
            if column in to_convert.keys():
                conversion[column] = to_convert[column]
            else:
                pass

        return pd.read_csv(
            file_path + ".csv", error_bad_lines=False, converters=conversion, index_col="Source ID")

    def handle_output(self, context, obj):
        obj_name = context.name
        obj.index = obj.index.astype(str)
        obj.sort_index(inplace=True)
        file_path = os.path.join("data", "output", obj_name)
        obj.to_csv(file_path + ".csv")

        if obj_name == "metadata":
            yield dg.AssetMaterialization(
                asset_key=dg.AssetKey(obj_name),
                description=f" {obj_name.upper()} was saved <----------------------",
                metadata_entries=[dg.EventMetadataEntry.json(label="CSV",data={
                    "total items": len(obj),
                    "creators": len(obj[obj["Creator"].notna()]),
                    "title": len(obj[obj["Title"].notna()]),
                    "date": len(obj[obj["Date"].notna()]),
                    "first year/last year": len(obj[obj["First Year"].notna()]),
                    "geolocated": len(obj[obj["Latitude"].notna()]),
                    "published on wikidata": len(obj[obj["Wikidata ID"].notna()])
                })],
            )
        
        elif obj_name == "cumulus":
            yield dg.AssetMaterialization(
                asset_key=dg.AssetKey(obj_name),
                description=f" {obj_name.upper()} was saved <----------------------",
                metadata_entries=[dg.EventMetadataEntry.json(label="CSV",data={
                    "total items": len(obj),
                    "creators": len(obj[obj["Creator"].notna()]),
                    "title": len(obj[obj["Title"].notna()]),
                    "date": len(obj[obj["Date"].notna()]),
                    "first year/last year": len(obj[obj["First Year"].notna()]),
                })],
            )
        
        else:
            yield dg.AssetMaterialization(
                asset_key=dg.AssetKey(obj_name),
                description=f" {obj_name.upper()} was saved <----------------------",
                metadata_entries=[dg.EventMetadataEntry.json(label="CSV",data={
                    "total items": len(obj)})],
            )


@dg.io_manager
def df_csv_io_manager(init_context):
    return PandasCsvIOManager()


class GeojsonIOManager(dg.IOManager):
    def load_input(self, context):
        file_path = os.path.join(
            "data", "output", context.upstream_output.name)
        return gpd.read_file(file_path + ".geojson")  # retorno um df

    def handle_output(self, context, feature_collection):
        file_path = os.path.join("data", "output", context.name) + ".geojson"
        with open(file_path, "w", encoding="utf-8") as f:
            geojson.dump(feature_collection, f, ensure_ascii=False, indent=4)

        yield dg.AssetMaterialization(
            asset_key=dg.AssetKey(file_path),
            description=f" {context.name.upper()} was saved <----------------------",
        )


@dg.io_manager
def geojson_io_manager(init_context):
    return GeojsonIOManager()


@dg.solid(
    input_defs=[dg.InputDefinition(
        "metadata", root_manager_key="metadata_root")],
    output_defs=[dg.OutputDefinition(
        io_manager_key="pandas_csv", name="metadata", dagster_type=dp.DataFrame)],
)
def update_metadata(context, df: main_dataframe_types, metadata: metadata_dataframe_types):
    df.reset_index(inplace=True)
    # find items how not are found on metadata
    filter = df["Source ID"].isin(metadata["Source ID"])
    review = list(df["Source ID"].loc[~filter])
    context.log.info(f"{len(review)} Items to review: {review}")

    df.set_index("Source ID",inplace=True)

    metadata.set_index("Source ID", inplace=True)
    metadata.update(df)
    
    return metadata


@dg.io_manager
def df_csv_io_manager(init_context):
    return PandasCsvIOManager()


def function_int(x): return str(str(x).split(".")[0]) if x else np.nan


@dg.root_input_manager(config_schema=dg.StringSource)
def root_input_xls(context):
    path = context.resource_config
    return pd.read_excel(path, converters={"First Year[19466]": function_int, "Last Year[19467]": function_int})


@dg.root_input_manager(config_schema=dg.StringSource)
def root_input_csv(context):
    df = pd.read_csv(context.resource_config, error_bad_lines=False)

    to_convert = {
        "SSID": function_int,
        "First Year": function_int,
        "Last Year": function_int,
        "Smapshot ID": function_int,
        "Width (mm)": function_int,
        "Height (mm)": function_int,
    }
    conversion = {}
    for column in df.columns:
        if column in to_convert.keys():
            conversion[column] = to_convert[column]
        else:
            pass

    return pd.read_csv(
        context.resource_config, error_bad_lines=False, converters=conversion)


@dg.root_input_manager(config_schema=dg.StringSource)
def root_input_xml(context):
    path = context.resource_config
    with open(path, encoding="utf8") as f:
        tree = ElementTree.parse(f)
    root = tree.getroot()
    return root


@dg.root_input_manager(config_schema=dg.StringSource)
def root_input_geojson(context):
    return gpd.read_file(context.resource_config)  # retorn geopandas


@dg.solid
def pull_new_data(context):
    comands = ["git", "submodule", "update", "--init", "--recursive"]
    pull = subprocess.Popen(comands)


@dg.solid
def push_new_data(context):
    submodule_push = [
        "pwd",
        "git checkout main",
        "git add .",
        "git commit -a -m ':card_file_box: Update data'",
        "git push",
    ]

    for command in submodule_push:
        git_cli_sub = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            cwd="data",
        )
        output, errors = git_cli_sub.communicate()
        if "nothing" in output:
            break

    etl_push = [
        "pwd",
        "git checkout feature/dagster-submodule",
        "git add data",
        "git commit -m ':card_file_box: Update submodule'",
        "git push",
    ]

    for command in etl_push:
        git_cli_etl = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            cwd=".",
        )

        output, errors = git_cli_etl.communicate()
        context.log.info(
            f"command: {command} \noutput: {output} \nERRO: {errors}")
