import datetime
import os
import subprocess
from typing import Any
from xml.etree import ElementTree

import dagster as dg
import geojson
import geopandas as gpd
import numpy as np
import pandas as pd
import requests
from dagster.core.definitions import output
from dagster.core.definitions.events import Failure
from git.refs.symbolic import _git_dir
from git.repo.base import Repo
from pandas.core.frame import DataFrame
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


class PandasCsvIOManager(dg.IOManager):
    def load_input(self, context):
        obj_name = context.upstream_output.name

        if obj_name.startswith("imp"):
            file_path = os.path.join("data", "output", context.upstream_output.name)
        else:
            file_path = os.path.join(
                "data", "output", "log", context.upstream_output.name
            )

        return pd.read_csv(file_path + ".csv", index_col="id")

    def handle_output(self, context, obj):
        obj_name = context.name
        obj.index = obj.index.astype(str)
        obj.sort_index(inplace=True)

        if obj_name.startswith("imp"):
            file_path = os.path.join("data", "output", obj_name)
        else:
            file_path = os.path.join("data", "output", "log", obj_name)

        obj.to_csv(file_path + ".csv")

        yield dg.AssetMaterialization(
            asset_key=dg.AssetKey(obj_name),
            description=f" {obj_name.upper()} was saved <----------------------",
            # metadata={"number of rows": dg.EventMetadata.int(len(obj))},
        )
        # yield dg.EventMetadataEntry.text(obj.shape[0], label="number of rows")
        # metadata={"head": dg.EventMetadata.md(obj.head(5).to_markdown())}
        # EventMetadataEntry.md(obj.head(5).to_markdown(), "head(5)")


@dg.io_manager
def df_csv_io_manager(init_context):
    return PandasCsvIOManager()


class GeojsonIOManager(dg.IOManager):
    def load_input(self, context):
        file_path = os.path.join("data", "output", context.upstream_output.name)
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


@dg.solid
def rename_column(context, df, dic):
    df = df.rename(columns=dic)
    return df


@dg.solid(
    input_defs=[dg.InputDefinition("metadata", root_manager_key="metadata_root")],
    output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="metadata")],
)
def update_metadata(_, df, metadata):
    metadata.set_index("id", inplace=True)
    metadata.update(df)
    metadata[["first_year", "last_year"]] = metadata[
        ["first_year", "last_year"]
    ].applymap(lambda x: x if pd.isnull(x) else str(int(x)))
    return metadata


@dg.io_manager
def df_csv_io_manager(init_context):
    return PandasCsvIOManager()


@dg.root_input_manager(config_schema=dg.StringSource)
def root_input_xls(context):
    path = context.resource_config
    return pd.read_excel(path)


@dg.root_input_manager(config_schema=dg.StringSource)
def root_input_csv(context):
    return pd.read_csv(context.resource_config)


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
            print(f"command: {command} \noutput: {output} \nERRO: {errors}")

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
        print(f"command: {command} \noutput: {output} \nERRO: {errors}")
