import os
import pandas
import dagster as dg
import dagster_pandas as dp
import validators
import datetime
import geojson
import json
import pandas as pd
from urllib.request import urlopen, URLError


# def validate_JSON(jsonData):
#     try:
#         json.loads(jsonData)
#     except ValueError as err:
#         return False
#     return True


def validate_list_of_features(list_object):
    for n in list_object:
        if any(key in n for key in ["Type", "geometry", "properties"]):
            continue
        else:
            return False
    return True


def validate_geojson(geojson_object):
    if any(key in geojson_object for key in ["Type", "features"]):
        pass
    else:
        return False
    return True


def validate_str_url(url):
    try:
        if pd.notna(url):
            urlopen(url)
            print("sim:", url)
        else:
            pass
        return True
    except URLError:
        print("não:", url)
        return False


def validate_url(url):
    try:
        if pd.notna(url):
            urlopen(url)
            print("sim:", url)
        else:
            pass
        return True
    except URLError:
        print("não:", url)
        return False


def validate_kml(kml):

    return False


type_list_of_kmls = dg.DagsterType(
    name="type_list_of_kmls",
    type_check_fn=lambda _, value: all(
        list(map(lambda x: True if x.endswith(".kml") else False, value)))
)


type_list_of_features = dg.DagsterType(
    name="type_list_of_features",
    type_check_fn=lambda _, value: validate_list_of_features(value)
)


type_geojson = dg.DagsterType(
    name="type_geojson",
    type_check_fn=lambda _, value: validate_geojson(value)
)


# type_json = dg.DagsterType(
#     name="type_json",
#     type_check_fn=lambda _, value: validate_JSON(value)

# )
