from dagster.core.definitions import output
import geopandas as gpd
import os
from typing import Any
from xml.etree import ElementTree

import dagster as dg
from dagster.core.definitions.events import Failure
import geojson
from git.refs.symbolic import _git_dir
from git.repo.base import Repo
import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import datetime

# import git
import subprocess


class PandasCsvIOManager(dg.IOManager):
    def load_input(self, context):
        file_path = os.path.join("data/output", context.upstream_output.name)
        return pd.read_csv(file_path + ".csv", index_col="id")

    def handle_output(self, context, obj):
        obj_name = context.name
        file_path = os.path.join("data/output", obj_name)
        obj.to_csv(file_path + ".csv")

        yield dg.AssetMaterialization(
            asset_key=dg.AssetKey(obj_name),
            description=f"The {obj_name.upper()} was saved as csv",
            # metadata={"number of rows": dg.EventMetadata.int(len(obj))},
        )
        # yield dg.EventMetadataEntry.text(obj.shape[0], label="number of rows")


@dg.io_manager
def df_csv_io_manager(init_context):
    return PandasCsvIOManager()


class GeojsonIOManager(dg.IOManager):
    def load_input(self, context):
        file_path = os.path.join("data/output", context.upstream_output.name)
        return gpd.read_file(file_path + ".geojson")

    def handle_output(self, context, feature_collection):
        file_path = os.path.join("data/output", context.name) + ".geojson"
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
    return metadata.set_index("id")


@dg.io_manager
def df_csv_io_manager(init_context):
    return PandasCsvIOManager()


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
    return gpd.read_file(context.resource_config)


@dg.solid(required_resource_keys={"slack"})
def slack_solid(context):
    context.resources.slack.chat_postMessage(
        channel="#tutoriais-e-links", text=":wave: teste!"
    )


@dg.solid
def pull_new_data(context):
    comands = ["git", "submodule", "update", "--init", "--recursive"]
    pull = subprocess.Popen(comands)


@dg.solid
def push_new_data(context):
    # commit_messege = f"Data files updated on {datetime.date.today()}"
    # git_cd_submodule = subprocess.Popen("cd data")
    # git_checkout = subprocess.Popen("git checkout main")
    # git_commit = subprocess.Popen("git commit -a -m 'Update data in submodule'")
    # git_push = subprocess.Popen("git push")
    # git_cd_submodule = subprocess.Popen("cd ..")
    # git_add = subprocess.Popen("git add data")
    # git_commit = subprocess.Popen("git commit -m 'Update submodule'")

    commands = [
        "cd data",
        "git checkout main",
        "git commit -a -m 'Update data in submodule'",
        "git push",
        "cd ..",
        "git add data",
        "git commit -m 'Update submodule'",
        "git push",
    ]

    for command in commands:
        git_cli = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )

        output, errors = git_cli.communicate()
        print(command, output, errors)

        # print(git_cli.stderr)
        # slack message with git_cli.strerr
