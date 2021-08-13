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
        dp.PandasColumn.string_column(
            "Source ID"),
        dp.PandasColumn(
            "SSID", constraints=[str_column()]),
        dp.PandasColumn.string_column("Title"),
        dp.PandasColumn(
            "Description (English)", constraints=[str_column()]),
        dp.PandasColumn(
            "Description (Portuguese)", constraints=[str_column()]),
        dp.PandasColumn.string_column("Date"),
        dp.PandasColumn(
            "First Year", constraints=[int_column()]),
        dp.PandasColumn(
            "Last Year", constraints=[int_column()]),
        dp.PandasColumn.string_column("Type"),
        dp.PandasColumn.string_column(
            "Item Set"),
        dp.PandasColumn.string_column(
            "Source"),
        dp.PandasColumn("Source URL", constraints=[url_column()]),
        dp.PandasColumn(
            "Materials", constraints=[str_column()]),
        dp.PandasColumn(
            "Fabrication Method", constraints=[str_column()]),
        dp.PandasColumn.string_column(
            "Rights"),
        dp.PandasColumn.string_column(
            "License"),
        dp.PandasColumn.string_column(
            "Attribution"),
        dp.PandasColumn.string_column(
            "Width (mm)"),
        dp.PandasColumn.string_column(
            "Height (mm)"),
        dp.PandasColumn("Latitude", constraints=[float_column()]),
        dp.PandasColumn("Longitude", constraints=[float_column()]),
        dp.PandasColumn.string_column(
            "Depicts"),
        dp.PandasColumn("Wikidata ID", constraints=[url_column()]),
        dp.PandasColumn.string_column(
            "Smapshot ID"),
        dp.PandasColumn("Media URL", constraints=[url_column()]),

    ],
)


cumulus_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="cumulus_dataframe_types",
    description="Dataframe type to validate the cumulus.csv",
    columns=[
        dp.PandasColumn.string_column(
            "Source ID"),
        dp.PandasColumn.string_column("Title"),
        dp.PandasColumn(
            "Description (English)", constraints=[str_column()]),
        dp.PandasColumn(
            "Description (Portuguese)", constraints=[str_column()]),
        dp.PandasColumn.string_column("Date"),
        dp.PandasColumn("First Year", constraints=[int_column()]),
        dp.PandasColumn("Last Year", constraints=[int_column()]),
        dp.PandasColumn.string_column("Type"),
        dp.PandasColumn.string_column(
            "Item Set"),
        dp.PandasColumn.string_column(
            "Source"),
        dp.PandasColumn("Source URL", constraints=[url_column()]),
        dp.PandasColumn(
            "Materials", constraints=[str_column()]),
        dp.PandasColumn(
            "Fabrication Method", constraints=[str_column()]),
        dp.PandasColumn.string_column(
            "Rights"),
        dp.PandasColumn.string_column(
            "License"),
        dp.PandasColumn.string_column(
            "Attribution"),
        dp.PandasColumn("Width (mm)", constraints=[int_column()]),
        dp.PandasColumn("Height (mm)", constraints=[int_column()]),
        dp.PandasColumn.string_column(
            "Smapshot ID"),
    ],
)


images_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="images_dataframe_type",
    description="Dataframe type to validate the images.csv",
    columns=[
        dp.PandasColumn.string_column(
            "Source ID"),
        dp.PandasColumn.string_column(
            "Media URL"),
        dp.PandasColumn.string_column("img_sd")]
)


api_omeka_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="api_omeka_dataframe_type",
    description="Dataframe type to validate the api_omeka.csv",
    columns=[
        dp.PandasColumn.string_column(
            "Source ID", unique=True, non_nullable=True),
        dp.PandasColumn("omeka_url", constraints=[url_column()]),
    ])

api_portals_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="api_portals_dataframe_type",
    description="Dataframe type to validate the api_portals.csv",
    columns=[
        dp.PandasColumn.string_column(
            "Source ID", unique=True, non_nullable=True),

        # Portals Columns
        dp.PandasColumn.string_column(
            "portals_id"),
        dp.PandasColumn("Source URL", constraints=[url_column()]),
    ])


api_wikidata_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="api_wikidata_dataframe_type",
    description="Dataframe type to validate the api_wikidata.csv",
    columns=[
        dp.PandasColumn.string_column(
            "Source ID", unique=True, non_nullable=True),
        dp.PandasColumn("wikidata_id_url", constraints=[url_column()]),
        dp.PandasColumn.string_column(
            "wikidata_ims_id"),
        dp.PandasColumn.string_column(
            "wikidata_image"),
        dp.PandasColumn("Depicts", constraints=[url_column()]),
        dp.PandasColumn.string_column(
            "Wikidata ID"),

    ])


import_omeka_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="import_omeka_dataframe_type",
    description="Dataframe type to validate the import_omeka.csv",
    columns=[
        dp.PandasColumn.string_column(
            'dcterms:identifier', unique=True),
        dp.PandasColumn.string_column(
            'dcterms:title'),
        dp.PandasColumn.string_column(
            'dcterms:description'),
        dp.PandasColumn.string_column(
            'dcterms:creator'),
        dp.PandasColumn.string_column(
            'dcterms:date'),
        dp.PandasColumn.string_column(
            'dcterms:available'),
        dp.PandasColumn('dcterms:type:en', constraints=[url_column()]),
        dp.PandasColumn.string_column(
            'dcterms:type:pt'),
        dp.PandasColumn.string_column(
            'dcterms:medium:pt'),
        dp.PandasColumn('dcterms:medium:en', constraints=[url_column()]),
        dp.PandasColumn.string_column(
            'dcterms:format:pt'),
        dp.PandasColumn('dcterms:format:en', constraints=[url_column()]),
        dp.PandasColumn.string_column(
            'dcterms:rights'),
        dp.PandasColumn('dcterms:bibliographicCitation',
                        constraints=[str_column()]),
        dp.PandasColumn.string_column(
            'dcterms:source'),
        dp.PandasColumn.string_column(
            'dcterms:hasVersion'),
        dp.PandasColumn('latitude', constraints=[float_column()]),
        dp.PandasColumn('longitude', constraints=[float_column()]),
        dp.PandasColumn.string_column(
            'schema:polygon'),
        dp.PandasColumn('foaf:depicts', constraints=[url_column()]),
        dp.PandasColumn.string_column(
            'schema:width'),
        dp.PandasColumn.string_column(
            'schema:height'),
        dp.PandasColumn('media', constraints=[url_column()]),
        dp.PandasColumn.string_column('item_sets')]
)

import_wikidata_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="import_wikidata_dataframe_type",
    description="Dataframe type to validate the import_wikidata.csv",
    columns=[
        dp.PandasColumn.string_column('qid'),
        dp.PandasColumn.string_column('Qid'),
        dp.PandasColumn.string_column('P31'),
        dp.PandasColumn.string_column(
            'Lpt-br'),
        dp.PandasColumn.string_column(
            'Dpt-br'),
        dp.PandasColumn.string_column('Den'),
        dp.PandasColumn.string_column('P571'),
        dp.PandasColumn.string_column(
            'qal1319'),
        dp.PandasColumn.string_column(
            'qal1326'),
        dp.PandasColumn.string_column('P17'),
        dp.PandasColumn.string_column('P1259'),
        dp.PandasColumn.string_column(
            'qal2044'),
        dp.PandasColumn.string_column(
            'qal7787'),
        dp.PandasColumn.string_column(
            'qal8208'),
        dp.PandasColumn.string_column('P170'),
        dp.PandasColumn.string_column('P186'),
        dp.PandasColumn.string_column(
            'format'),
        dp.PandasColumn.string_column('P195'),
        dp.PandasColumn.string_column('P217'),
        dp.PandasColumn.string_column('P2079'),
        dp.PandasColumn.string_column('P4036'),
        dp.PandasColumn.string_column('P2049'),
        dp.PandasColumn.string_column('P2048'),
        dp.PandasColumn.string_column('P7835')

    ]
)

main_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="import_wikidata_dataframe_type",
    description="Dataframe type to validate the import_wikidata.csv",
    columns=[

        # Portals Columns
        dp.PandasColumn.string_column("portals_id", is_required=False),
        dp.PandasColumn("Source URL", constraints=[
                        url_column()], is_required=False),

        # Wikidata Columns
        dp.PandasColumn("wikidata_id_url", constraints=[
                        url_column()], is_required=False),
        dp.PandasColumn.string_column("wikidata_ims_id", is_required=False),
        dp.PandasColumn.string_column("wikidata_image", is_required=False),
        dp.PandasColumn("Depicts", constraints=[
                        url_column()], is_required=False),
        dp.PandasColumn.string_column("Wikidata ID", is_required=False),

        # Omeka Columns
        dp.PandasColumn("omeka_url", constraints=[
                        url_column()], is_required=False),

        # cumulus Columns
        dp.PandasColumn.string_column("Title", is_required=False),
        dp.PandasColumn.string_column(
            "Description (English)", is_required=False),
        dp.PandasColumn.string_column(
            "Description (Portuguese)", is_required=False),
        dp.PandasColumn.string_column("Date", is_required=False),
        dp.PandasColumn.float_column("First Year", is_required=False),
        dp.PandasColumn.float_column("Last Year", is_required=False),
        dp.PandasColumn.string_column("Type", is_required=False),
        dp.PandasColumn.string_column("Item Set", is_required=False),
        dp.PandasColumn.string_column("Source", is_required=False),
        dp.PandasColumn("Materials", is_required=False),
        dp.PandasColumn("Fabrication Method", is_required=False),
        dp.PandasColumn.string_column("Rights", is_required=False),
        dp.PandasColumn.string_column("License", is_required=False),
        dp.PandasColumn.string_column("Attribution", is_required=False),
        dp.PandasColumn.float_column("Width (mm)", is_required=False),
        dp.PandasColumn.float_column("Height (mm)", is_required=False),
        dp.PandasColumn.string_column("Smapshot ID", is_required=False),


    ],
)
