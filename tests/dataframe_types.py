import os
from dagster_pandas.constraints import non_null_validation
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
        dp.PandasColumn.string_column("Document ID", unique=True, non_nullable=True),
        dp.PandasColumn.string_column("SSID", unique=True, ignore_missing_vals=True),
        dp.PandasColumn.string_column("Title"),
        # dp.PandasColumn.string_column(
        #     "Description (English)"),
        dp.PandasColumn.string_column("Description (Portuguese)"),
        dp.PandasColumn.string_column("Date"),
        dp.PandasColumn("First Year", constraints=[int_column()]),
        dp.PandasColumn("Last Year", constraints=[int_column()]),
        dp.PandasColumn.string_column("Type"),
        dp.PandasColumn.string_column("Collections"),
        dp.PandasColumn.string_column("Provider"),
        dp.PandasColumn("Document URL", constraints=[url_column()]),
        dp.PandasColumn.string_column("Materials"),
        dp.PandasColumn.string_column("Fabrication Method"),
        dp.PandasColumn.string_column("Rights", ignore_missing_vals=True),
        # dp.PandasColumn.string_column(
        #     "License", ignore_missing_vals=True),
        # dp.PandasColumn.string_column(
        #    "Attribution", ignore_missing_vals=True),
        dp.PandasColumn("Width (mm)", constraints=[int_column()]),
        dp.PandasColumn("Height (mm)", constraints=[int_column()]),
        dp.PandasColumn("Latitude", constraints=[float_column()]),
        dp.PandasColumn("Longitude", constraints=[float_column()]),
        dp.PandasColumn.string_column("Depicts"),
        dp.PandasColumn("Wikidata ID", constraints=[url_column()]),
        dp.PandasColumn("Smapshot ID", constraints=[int_column()]),
        dp.PandasColumn("Media URL", constraints=[url_column()]),
    ],
)


main_dataframe_types = dp.create_dagster_pandas_dataframe_type(
    name="main_dataframe_types",
    description="Dataframe type to validate the import_wikidata.csv",
    columns=[
        # Portals Columns
        dp.PandasColumn.integer_column("portals_id", is_required=False),
        dp.PandasColumn("Document URL", constraints=[url_column()], is_required=False),
        # Wikidata Columns
        dp.PandasColumn(
            "wikidata_id_url", constraints=[url_column()], is_required=False
        ),
        dp.PandasColumn.string_column("wikidata_ims_id", is_required=False),
        dp.PandasColumn.string_column("wikidata_image", is_required=False),
        dp.PandasColumn("Depicts", constraints=[url_column()], is_required=False),
        dp.PandasColumn.string_column("Wikidata ID", is_required=False),
        # Cumulus Columns
        dp.PandasColumn.string_column(
            "Title", is_required=False, ignore_missing_vals=True
        ),
        # dp.PandasColumn.string_column(
        #    "Description (English)", is_required=False, ignore_missing_vals=True),
        dp.PandasColumn.string_column(
            "Description (Portuguese)", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "Date", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn("First Year", constraints=[int_column()], is_required=False),
        dp.PandasColumn("Last Year", constraints=[int_column()], is_required=False),
        dp.PandasColumn.string_column(
            "Type", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column("Collections", is_required=False),
        dp.PandasColumn.string_column("Provider", is_required=False),
        dp.PandasColumn.string_column("Materials", is_required=False),
        dp.PandasColumn.string_column(
            "Fabrication Method", is_required=False, ignore_missing_vals=True
        ),
        # dp.PandasColumn.string_column(
        #    "Rights", is_required=False, ignore_missing_vals=True),
        # dp.PandasColumn.string_column(
        #    "License", is_required=False, ignore_missing_vals=True),
        # dp.PandasColumn.string_column("Attribution", is_required=False),
        dp.PandasColumn("Width (mm)", constraints=[float_column()], is_required=False),
        dp.PandasColumn("Height (mm)", is_required=False, constraints=[float_column()]),
        dp.PandasColumn("Smapshot ID", constraints=[int_column()], is_required=False),
        # Images
        dp.PandasColumn("Media URL", is_required=False, constraints=[url_column()]),
        dp.PandasColumn.string_column(
            "img_sd", is_required=False, ignore_missing_vals=True
        ),
        # Import wiki Columns
        dp.PandasColumn.string_column(
            "qid", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "Qid", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "P31", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "Lpt-br", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "Dpt-br", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "Den", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "P571", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "qal1319", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "qal1326", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "P17", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "P1259", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "qal2044", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "qal7787", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "qal8208", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "P170", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "P186", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "format", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "P195", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "P217", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "P2079", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "P4036", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "P2049", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "P2048", is_required=False, ignore_missing_vals=True
        ),
        dp.PandasColumn.string_column(
            "P7835", is_required=False, ignore_missing_vals=True
        ),
    ],
)
