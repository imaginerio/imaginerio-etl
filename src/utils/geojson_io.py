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


class GeojsonIOManager(dg.IOManager):
    """
    GEOJSON Input/Output Manager
    """

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
