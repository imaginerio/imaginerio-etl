import os
import subprocess
from xml.etree import ElementTree

import dagster as dg
import dagster_pandas as dp
import geopandas as gpd
import numpy as np
import pandas as pd
from dagster import config
from dagster.core.definitions import output
from numpy.core.numeric import NaN
from numpy.lib.arraysetops import isin
from tests.dataframe_types import *
from tests.objects_types import *


def function_int(x):
    return str(str(x).split(".")[0]) if x else np.nan


class PandasCsvIOManager(dg.IOManager):
    """
    CSV Input/Output Manager
    """

    def load_input(self, context):
        """
        Loads DataFrame from CSV file
        """
        file_path = os.path.join(
            "data", "output", context.upstream_output.name)
        df = pd.read_csv(file_path + ".csv", error_bad_lines=False)

        # by default pandas converts int+nan columns to float
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
            file_path + ".csv",
            error_bad_lines=False,
            converters=conversion,
            index_col="Source ID",
        )

    def handle_output(self, context, obj):
        """
        Writes CSV from DataFrame
        """
        obj_name = context.name
        obj.index = obj.index.astype(str)
        obj.sort_index(inplace=True)
        file_path = os.path.join("data", "output", obj_name)
        obj.to_csv(file_path + ".csv")

        # emmit AssetMaterialization when saving metadata.csv
        if obj_name == "metadata":
            yield dg.AssetMaterialization(
                asset_key=dg.AssetKey(obj_name),
                description=f" {obj_name.upper()} was saved <----------------------",
                metadata_entries=[
                    dg.EventMetadataEntry.json(
                        label="CSV",
                        data={
                            "total items": len(obj),
                            "creators": len(obj[obj["Creator"].notna()]),
                            "title": len(obj[obj["Title"].notna()]),
                            "date": len(obj[obj["Date"].notna()]),
                            "first year/last year": len(obj[obj["First Year"].notna()]),
                            "geolocated": len(obj[obj["Latitude"].notna()]),
                            "published on wikidata": len(
                                obj[obj["Wikidata ID"].notna()]
                            ),
                        },
                    )
                ],
            )

        elif obj_name == "cumulus":
            yield dg.AssetMaterialization(
                asset_key=dg.AssetKey(obj_name),
                description=f" {obj_name.upper()} was saved <----------------------",
                metadata_entries=[
                    dg.EventMetadataEntry.json(
                        label="CSV",
                        data={
                            "total items": len(obj),
                            "creators": len(obj[obj["Creator"].notna()]),
                            "title": len(obj[obj["Title"].notna()]),
                            "date": len(obj[obj["Date"].notna()]),
                            "first year/last year": len(obj[obj["First Year"].notna()]),
                        },
                    )
                ],
            )

        else:
            yield dg.AssetMaterialization(
                asset_key=dg.AssetKey(obj_name),
                description=f" {obj_name.upper()} was saved <----------------------",
                metadata_entries=[
                    dg.EventMetadataEntry.json(
                        label="CSV", data={"total items": len(obj)}
                    )
                ],
            )


@dg.io_manager
def df_csv_io_manager(init_context):
    return PandasCsvIOManager()
