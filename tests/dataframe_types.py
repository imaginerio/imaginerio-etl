import os
from dagster_pandas.constraints import ColumnConstraint
import pandas
import dagster as dg
import dagster_pandas as dp
import validators
import datetime
import geojson
import json


class url_column(ColumnConstraint):
    def __init__(self):
        message = "Value must be a  valid link"
        super(url_column, self).__init__(
            error_description=message, markdown_description=message
        )

    def validate(self, dataframe, column_name):
        rows_with_unexpected_buckets = dataframe[dataframe[column_name].apply(
            lambda x: False if validators.url(x.split(" ")[0]) else True)]
        if not rows_with_unexpected_buckets.empty:
            raise ColumnConstraintViolationException(
                constraint_name=self.name,
                constraint_description=self.error_description,
                column_name=column_name,
                offending_rows=rows_with_unexpected_buckets,
            )


class string_column_type(ColumnConstraint):
    def __init__(self):
        message = "Value must be a string"
        super(string_column_type, self).__init__(
            error_description=message, markdown_description=message
        )

    def validate(self, dataframe, column_name):
        rows_with_unexpected_buckets = dataframe[dataframe[column_name].apply(
            lambda x: True if isinstance(x, str) else False)]
        if not rows_with_unexpected_buckets.empty:
            raise ColumnConstraintViolationException(
                constraint_name=self.name,
                constraint_description=self.error_description,
                column_name=column_name,
                offending_rows=rows_with_unexpected_buckets,
            )


metadata_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="metadata_dataframe_types",
    description="Dataframe type to validate the metadata.csv",
    columns=[
        dp.PandasColumn.string_column(
            "Source ID", unique=True, non_nullable=True),
        dp.PandasColumn.string_column(
            "SSID", unique=True, non_nullable=True),
        dp.PandasColumn.string_column("Title", ignore_missing_vals=True),
        dp.PandasColumn.string_column(
            "Description (English)", ignore_missing_vals=True),
        dp.PandasColumn.string_column(
            "Description (Portuguese)", ignore_missing_vals=True),
        dp.PandasColumn.string_column("Date", ignore_missing_vals=True),
        dp.PandasColumn.integer_column(
            "First Year", ignore_missing_vals=True, max_value=datetime.datetime.now().year),
        dp.PandasColumn.integer_column(
            "Last Year", ignore_missing_vals=True, max_value=datetime.datetime.now().year),
        dp.PandasColumn.string_column("Type", ignore_missing_vals=True),
        dp.PandasColumn.string_column("Item Set", ignore_missing_vals=True),
        dp.PandasColumn.string_column("Source", ignore_missing_vals=True),
        dp.PandasColumn("Source URL", constraints=[url_column()]),
        dp.PandasColumn.string_column(
            "Materials", ignore_missing_vals=True),
        dp.PandasColumn.string_column(
            "Fabrication Method", ignore_missing_vals=True),
        dp.PandasColumn.string_column("Rights", ignore_missing_vals=True),
        dp.PandasColumn.string_column("License", ignore_missing_vals=True),
        dp.PandasColumn.string_column("Attribution", ignore_missing_vals=True),
        dp.PandasColumn.string_column("Width (mm)", ignore_missing_vals=True),
        dp.PandasColumn.string_column("Height (mm)", ignore_missing_vals=True),
        dp.PandasColumn.float_column("Latitude", ignore_missing_vals=True),
        dp.PandasColumn.float_column("Longitude", ignore_missing_vals=True),
        dp.PandasColumn.string_column("Depicts", ignore_missing_vals=True),
        dp.PandasColumn("Wikidata ID", constraints=[url_column()]),
        dp.PandasColumn.string_column("Smapshot ID"),
        dp.PandasColumn("Media URL", constraints=[url_column()]),

    ],
)


cumulus_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="cumulus_dataframe_types",
    description="Dataframe type to validate the cumulus.csv",
    columns=[
        dp.PandasColumn.string_column(
            "Source ID", unique=True, non_nullable=True),
        dp.PandasColumn.string_column("Title", ignore_missing_vals=True),
        dp.PandasColumn.string_column(
            "Description (English)", ignore_missing_vals=True),
        dp.PandasColumn.string_column(
            "Description (Portuguese)", ignore_missing_vals=True),
        dp.PandasColumn.string_column("Date", ignore_missing_vals=True),
        dp.PandasColumn.integer_column(
            "First Year", ignore_missing_vals=True, max_value=datetime.datetime.now().year),
        dp.PandasColumn.integer_column(
            "Last Year", ignore_missing_vals=True, max_value=datetime.datetime.now().year),
        dp.PandasColumn.string_column("Type", ignore_missing_vals=True),
        dp.PandasColumn.string_column("Item Set", ignore_missing_vals=True),
        dp.PandasColumn.string_column("Source", ignore_missing_vals=True),
        dp.PandasColumn("Source URL", constraints=[url_column()]),
        dp.PandasColumn.string_column(
            "Materials", ignore_missing_vals=True),
        dp.PandasColumn.string_column(
            "Fabrication Method", ignore_missing_vals=True),
        dp.PandasColumn.string_column("Rights", ignore_missing_vals=True),
        dp.PandasColumn.string_column("License", ignore_missing_vals=True),
        dp.PandasColumn.string_column("Attribution", ignore_missing_vals=True),
        dp.PandasColumn.string_column("Width (mm)", ignore_missing_vals=True),
        dp.PandasColumn.string_column("Height (mm)", ignore_missing_vals=True),
        dp.PandasColumn.string_column("Smapshot ID"),
    ],
)


images_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="images_dataframe_type",
    description="Dataframe type to validate the images.csv",
    columns=[
        dp.PandasColumn("Source ID", constraints=[string_column_type()]),
        dp.PandasColumn("Media URL", constraints=[url_column()]),
        dp.PandasColumn("img_sd", constraints=[url_column()])]
)


api_omeka_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="api_omeka_dataframe_type",
    description="Dataframe type to validate the api_omeka.csv",
    columns=[
        dp.PandasColumn("Source ID", constraints=[string_column_type()]),
        dp.PandasColumn("omeka_url", constraints=[url_column()]),
    ])

api_portals_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="api_portals_dataframe_type",
    description="Dataframe type to validate the api_portals.csv",
    columns=[]
)


api_wikidata_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="api_wikidata_dataframe_type",
    description="Dataframe type to validate the api_wikidata.csv",
    columns=[]
)


import_omeka_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="import_omeka_dataframe_type",
    description="Dataframe type to validate the import_omeka.csv",
    columns=[]
)

import_wikidata_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="import_omeka_dataframe_type",
    description="Dataframe type to validate the import_wikidata.csv",
    columns=[]
)
