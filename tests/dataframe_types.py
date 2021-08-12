import os
import pandas
import dagster as dg
import dagster_pandas as dp
import validators
import datetime
import geojson
import json
from tests.column_types import *

metadata_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="metadata_dataframe_types",
    description="Dataframe type to validate the metadata.csv",
    columns=[
        dp.PandasColumn(
            "Source ID", constraints=[str_column()]),
        dp.PandasColumn(
            "SSID", constraints=[str_column()]),
        dp.PandasColumn("Title", constraints=[str_column()]),
        dp.PandasColumn(
            "Description (English)", constraints=[str_column()]),
        dp.PandasColumn(
            "Description (Portuguese)", constraints=[str_column()]),
        dp.PandasColumn("Date", constraints=[str_column()]),
        dp.PandasColumn(
            "First Year", constraints=[int_column()]),
        dp.PandasColumn(
            "Last Year", constraints=[int_column()]),
        dp.PandasColumn("Type", constraints=[str_column()]),
        dp.PandasColumn("Item Set", constraints=[str_column()]),
        dp.PandasColumn("Source", constraints=[str_column()]),
        dp.PandasColumn("Source URL", constraints=[url_column()]),
        dp.PandasColumn(
            "Materials", constraints=[str_column()]),
        dp.PandasColumn(
            "Fabrication Method", constraints=[str_column()]),
        dp.PandasColumn("Rights", constraints=[str_column()]),
        dp.PandasColumn("License", constraints=[str_column()]),
        dp.PandasColumn("Attribution", constraints=[str_column()]),
        dp.PandasColumn("Width (mm)", constraints=[str_column()]),
        dp.PandasColumn("Height (mm)", constraints=[str_column()]),
        dp.PandasColumn("Latitude", constraints=[float_column()]),
        dp.PandasColumn("Longitude", constraints=[float_column()]),
        dp.PandasColumn("Depicts", constraints=[str_column()]),
        dp.PandasColumn("Wikidata ID", constraints=[url_column()]),
        dp.PandasColumn("Smapshot ID", constraints=[str_column()]),
        dp.PandasColumn("Media URL", constraints=[url_column()]),

    ],
)


cumulus_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="cumulus_dataframe_types",
    description="Dataframe type to validate the cumulus.csv",
    columns=[
        dp.PandasColumn(
            "Source ID", constraints=[str_column()]),
        dp.PandasColumn("Title", constraints=[str_column()]),
        dp.PandasColumn(
            "Description (English)", constraints=[str_column()]),
        dp.PandasColumn(
            "Description (Portuguese)", constraints=[str_column()]),
        dp.PandasColumn("Date", constraints=[str_column()]),
        dp.PandasColumn("First Year", constraints=[int_column()]),
        dp.PandasColumn("Last Year", constraints=[int_column()]),
        dp.PandasColumn("Type", constraints=[str_column()]),
        dp.PandasColumn("Item Set", constraints=[str_column()]),
        dp.PandasColumn("Source", constraints=[str_column()]),
        dp.PandasColumn("Source URL", constraints=[url_column()]),
        dp.PandasColumn(
            "Materials", constraints=[str_column()]),
        dp.PandasColumn(
            "Fabrication Method", constraints=[str_column()]),
        dp.PandasColumn("Rights", constraints=[str_column()]),
        dp.PandasColumn("License", constraints=[str_column()]),
        dp.PandasColumn("Attribution", constraints=[str_column()]),
        dp.PandasColumn("Width (mm)", constraints=[int_column()]),
        dp.PandasColumn("Height (mm)", constraints=[int_column()]),
        dp.PandasColumn("Smapshot ID", constraints=[str_column()]),
    ],
)


images_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="images_dataframe_type",
    description="Dataframe type to validate the images.csv",
    columns=[
        dp.PandasColumn("Source ID", constraints=[str_column()]),
        dp.PandasColumn("Media URL", constraints=[str_column()]),
        dp.PandasColumn("img_sd", constraints=[str_column()])]
)


api_omeka_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="api_omeka_dataframe_type",
    description="Dataframe type to validate the api_omeka.csv",
    columns=[
        dp.PandasColumn("Source ID", constraints=[str_column()]),
        dp.PandasColumn("omeka_url", constraints=[url_column()]),
    ])

api_portals_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="api_portals_dataframe_type",
    description="Dataframe type to validate the api_portals.csv",
    columns=[
        dp.PandasColumn.string_column(
            "Source ID", unique=True, non_nullable=True),
        dp.PandasColumn("portals_id", constraints=[int_column()]),
        dp.PandasColumn("Source URL", constraints=[url_column()]),

    ])


api_wikidata_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="api_wikidata_dataframe_type",
    description="Dataframe type to validate the api_wikidata.csv",
    columns=[
        dp.PandasColumn("Source ID", constraints=[str_column()]),
        dp.PandasColumn("wikidata_id_url", constraints=[url_column()]),
        dp.PandasColumn("wikidata_ims_id", constraints=[str_column()]),
        dp.PandasColumn("wikidata_image", constraints=[str_column()]),
        dp.PandasColumn("Depicts", constraints=[url_column()]),
        dp.PandasColumn("Wikidata ID", constraints=[str_column()]),

    ])


import_omeka_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="import_omeka_dataframe_type",
    description="Dataframe type to validate the import_omeka.csv",
    columns=[
        dp.PandasColumn('dcterms:identifier', constraints=[str_column()]),
        dp.PandasColumn('dcterms:title', constraints=[str_column()]),
        dp.PandasColumn('dcterms:description', constraints=[str_column()]),
        dp.PandasColumn('dcterms:creator', constraints=[str_column()]),
        dp.PandasColumn('dcterms:date', constraints=[str_column()]),
        dp.PandasColumn('dcterms:available', constraints=[str_column()]),
        dp.PandasColumn('dcterms:type:en', constraints=[url_column()]),
        dp.PandasColumn('dcterms:type:pt', constraints=[str_column()]),
        dp.PandasColumn('dcterms:medium:pt', constraints=[str_column()]),
        dp.PandasColumn('dcterms:medium:en', constraints=[url_column()]),
        dp.PandasColumn('dcterms:format:pt', constraints=[str_column()]),
        dp.PandasColumn('dcterms:format:en', constraints=[url_column()]),
        dp.PandasColumn('dcterms:rights', constraints=[str_column()]),
        dp.PandasColumn('dcterms:bibliographicCitation',
                        constraints=[str_column()]),
        dp.PandasColumn('dcterms:source', constraints=[str_column()]),
        dp.PandasColumn('dcterms:hasVersion', constraints=[str_column()]),
        dp.PandasColumn('latitude', constraints=[float_column()]),
        dp.PandasColumn('longitude', constraints=[float_column()]),
        dp.PandasColumn('schema:polygon', constraints=[str_column()]),
        dp.PandasColumn('foaf:depicts', constraints=[url_column()]),
        dp.PandasColumn('schema:width', constraints=[str_column()]),
        dp.PandasColumn('schema:height', constraints=[str_column()]),
        dp.PandasColumn('media', constraints=[url_column()]),
        dp.PandasColumn('item_sets', constraints=[str_column()])]
)

import_wikidata_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="import_wikidata_dataframe_type",
    description="Dataframe type to validate the import_wikidata.csv",
    columns=[
        dp.PandasColumn('qid', constraints=[str_column()]),
        dp.PandasColumn('Qid', constraints=[str_column()]),
        dp.PandasColumn('P31', constraints=[str_column()]),
        dp.PandasColumn('Lpt-br', constraints=[str_column()]),
        dp.PandasColumn('Dpt-br', constraints=[str_column()]),
        dp.PandasColumn('Den', constraints=[str_column()]),
        dp.PandasColumn('P571', constraints=[str_column()]),
        dp.PandasColumn('qal1319', constraints=[str_column()]),
        dp.PandasColumn('qal1326', constraints=[str_column()]),
        dp.PandasColumn('P17', constraints=[str_column()]),
        dp.PandasColumn('P1259', constraints=[str_column()]),
        dp.PandasColumn('qal2044', constraints=[str_column()]),
        dp.PandasColumn('qal7787', constraints=[str_column()]),
        dp.PandasColumn('qal8208', constraints=[str_column()]),
        dp.PandasColumn('P170', constraints=[str_column()]),
        dp.PandasColumn('P186', constraints=[str_column()]),
        dp.PandasColumn('format', constraints=[str_column()]),
        dp.PandasColumn('P195', constraints=[str_column()]),
        dp.PandasColumn('P217', constraints=[str_column()]),
        dp.PandasColumn('P2079', constraints=[str_column()]),
        dp.PandasColumn('P4036', constraints=[str_column()]),
        dp.PandasColumn('P2049', constraints=[str_column()]),
        dp.PandasColumn('P2048', constraints=[str_column()]),
        dp.PandasColumn('P7835', constraints=[str_column()])

    ]
)
