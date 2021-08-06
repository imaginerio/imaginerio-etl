import os
import pandas
import dagster as dg
import dagster_pandas as dp
import validators
import datetime
import geojson
import geojson


def validate_list_of_kmls(list_object):
    for n in list_object:
        if n.endswith(".kml"):
            continue
        else:
            return False
    return True


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


type_list_of_kmls = dg.DagsterType(
    name="type_list_of_kmls",
    type_check_fn=lambda _, value: validate_list_of_kmls(value)
)


type_list_of_features = dg.DagsterType(
    name="type_list_of_features",
    type_check_fn=lambda _, value: validate_list_of_features(value)
)


type_geojson = dg.DagsterType(
    name="type_geojson",
    type_check_fn=lambda _, value: True if isinstance(
        value, geojson.GeoJSON) else False
)


# class url_column(ColumnConstraint):
#     def __init__(self):
#         message = "Value must be a  valid link"
#         super(url_column, self).__init__(
#             error_description=message, markdown_description=message
#         )

#     def validate(self, dataframe, column_name):
#         rows_with_unexpected_buckets = dataframe[dataframe[column_name].apply(
#             lambda x: False if validators.url(x.split(" ")[0]) else True)]
#         if not rows_with_unexpected_buckets.empty:
#             raise dp.ColumnConstraintViolationException(
#                 constraint_name=self.name,
#                 constraint_description=self.error_description,
#                 column_name=column_name,
#                 offending_rows=rows_with_unexpected_buckets,
#             )


# metadata_dataframe_types = dp.create_dagster_pandas_dataframe_type(
#     name="metadata_dataframe_types",
#     description="Dataframe type to validate the metadata",
#     columns=[
#         dp.PandasColumn.string_column(
#             "Source ID", unique=True, non_nullable=True),
#         dp.PandasColumn.string_column(
#             "SSID", unique=True, non_nullable=True),
#         dp.PandasColumn.string_column("Title", ignore_missing_vals=True),
#         dp.PandasColumn.string_column(
#             "Description (English)", ignore_missing_vals=True),
#         dp.PandasColumn.string_column(
#             "Description (Portuguese)", ignore_missing_vals=True),
#         dp.PandasColumn.string_column("Date", ignore_missing_vals=True),
#         dp.PandasColumn.integer_column(
#             "First Year", ignore_missing_vals=True, max_value=datetime.datetime.now().year),
#         dp.PandasColumn.integer_column(
#             "Last Year", ignore_missing_vals=True, max_value=datetime.datetime.now().year),
#         dp.PandasColumn.string_column("Type", ignore_missing_vals=True),
#         dp.PandasColumn.string_column("Item Set", ignore_missing_vals=True),
#         dp.PandasColumn.string_column("Source", ignore_missing_vals=True),
#         dp.PandasColumn("Source URL", constraints=[url_column()]),
#         dp.PandasColumn.string_column(
#             "Materials", ignore_missing_vals=True),  # teste
#         dp.PandasColumn.string_column(
#             "Fabrication Method", ignore_missing_vals=True),
#         dp.PandasColumn.string_column("Rights", ignore_missing_vals=True),
#         dp.PandasColumn.string_column("License", ignore_missing_vals=True),
#         dp.PandasColumn.string_column("Attribution", ignore_missing_vals=True),
#         dp.PandasColumn.string_column("Width (mm)", ignore_missing_vals=True),
#         dp.PandasColumn.string_column("Height (mm)", ignore_missing_vals=True),
#         dp.PandasColumn.float_column("Latitude", ignore_missing_vals=True),
#         dp.PandasColumn.float_column("Longitude", ignore_missing_vals=True),
#         dp.PandasColumn.string_column("Depicts", ignore_missing_vals=True),
#         dp.PandasColumn("Wikidata ID", constraints=url_column()),
#         dp.PandasColumn.string_column("Smapshot ID"),
#         dp.PandasColumn("Media URL", constraints=[url_column()]),

#     ],
# )


# cumulus_dataframe_types = dp.create_dagster_pandas_dataframe_type(
#     name="cumulus_dataframe_types",
#     description="Dataframe type to validate the metadata",
#     columns=[
#         dp.PandasColumn.string_column(
#             "Source ID", unique=True, non_nullable=True),
#         dp.PandasColumn.string_column("Title", ignore_missing_vals=True),
#         dp.PandasColumn.string_column(
#             "Description (English)", ignore_missing_vals=True),
#         dp.PandasColumn.string_column(
#             "Description (Portuguese)", ignore_missing_vals=True),
#         dp.PandasColumn.string_column("Date", ignore_missing_vals=True),
#         dp.PandasColumn.integer_column(
#             "First Year", ignore_missing_vals=True, max_value=datetime.datetime.now().year),
#         dp.PandasColumn.integer_column(
#             "Last Year", ignore_missing_vals=True, max_value=datetime.datetime.now().year),
#         dp.PandasColumn.string_column("Type", ignore_missing_vals=True),
#         dp.PandasColumn.string_column("Item Set", ignore_missing_vals=True),
#         dp.PandasColumn.string_column("Source", ignore_missing_vals=True),
#         dp.PandasColumn("Source URL", constraints=[url_column()]),
#         dp.PandasColumn.string_column(
#             "Materials", ignore_missing_vals=True),
#         dp.PandasColumn.string_column(
#             "Fabrication Method", ignore_missing_vals=True),
#         dp.PandasColumn.string_column("Rights", ignore_missing_vals=True),
#         dp.PandasColumn.string_column("License", ignore_missing_vals=True),
#         dp.PandasColumn.string_column("Attribution", ignore_missing_vals=True),
#         dp.PandasColumn.string_column("Width (mm)", ignore_missing_vals=True),
#         dp.PandasColumn.string_column("Height (mm)", ignore_missing_vals=True),
#         dp.PandasColumn.string_column("Smapshot ID"),


#     ],
# )
