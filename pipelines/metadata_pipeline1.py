from datetime import datetime
import re
import dagster_pandas as dp
import validators

import dagster as dg
import pandas as pd
from dotenv import load_dotenv
from solids.utils import *

load_dotenv(override=True)


class url_column(dp.PandasColumn):
    def __init__(self):
        message = "Value must be a  valid link"
        super(url_column, self).__init__(
            error_description=message, markdown_description=message
        )
        print(message)

    def validate(self, dataframe, column_name):
        rows_with_unexpected_buckets = dataframe[dataframe[column_name].apply(
            lambda x: False if validators.url(x.split(" ")[0]) else True)]
        if not rows_with_unexpected_buckets.empty:
            raise dp.ColumnConstraintViolationException(
                constraint_name=self.name,
                constraint_description=self.error_description,
                column_name=column_name,
                offending_rows=rows_with_unexpected_buckets,
            )


metadata = dp.create_dagster_pandas_dataframe_type(
    name="metadata",
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
        dp.PandasColumn("Source URL", constraints=url_column()),
        dp.PandasColumn.string_column("Materials", non_nullable=True),  # teste
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
        dp.PandasColumn("Wikidata ID", constraints=url_column()),
        dp.PandasColumn.string_column("Smapshot ID"),
        dp.PandasColumn("Media URL", constraints=url_column()),

    ],
)


preset = {
    "resources": {
        "cumulus_root": {"config": {"env": "CUMULUS"}},
        "wikidata_root": {"config": {"env": "WIKIDATA"}},
        "portals_root": {"config": {"env": "PORTALS"}},
        "camera_root": {"config": {"env": "CAMERA"}},
        "images_root": {"config": {"env": "IMAGES"}},
        "jstor_root": {"config": {"env": "JSTOR_XLS"}},
    },
}

################   SOLIDS   ##################


@ dg.solid(
    input_defs=[
        dg.InputDefinition("cumulus", root_manager_key="cumulus_root"),
        dg.InputDefinition("wikidata", root_manager_key="wikidata_root"),
        dg.InputDefinition("portals", root_manager_key="portals_root"),
        dg.InputDefinition("camera", root_manager_key="camera_root"),
        dg.InputDefinition("images", root_manager_key="images_root"),
    ]
)
def create_metadata(context, cumulus, wikidata, portals, camera, images) -> dp.DataFrame:
    camera_new = camera[
        [
            "Source ID",
            "Longitude",
            "Latitude",
        ]
    ]

    cumulus[["First Year", "Last Year"]] = cumulus[
        ["First Year", "Last Year"]
    ].applymap(lambda x: x if pd.isnull(x) else str(int(x)))

    dataframes_outer = [cumulus, camera_new, images]
    dataframe_left = [portals, wikidata]
    metadata = pd.DataFrame(columns=["Source ID"])

    for df in dataframes_outer:
        metadata = metadata.merge(df, how="outer", on="Source ID")

    for df in dataframe_left:
        metadata = metadata.merge(df, how="left", on="Source ID")

    metadata_new = metadata[
        [
            "Source ID",
            "Title",
            "Creator",
            # vazio ou string fixa feito no cumulus ok
            "Description (English)",
            "Description (Portuguese)",
            "Date",
            "First Year",
            "Last Year",
            "Type",
            "Item Set",
            "Source",
            "Source URL",  # url do portals
            "Materials",
            "Fabrication Method",
            "Rights",  # vazio ou string fixa feito no cumulus ok
            "License",  # vazio ou string fixa feito no cumulus ok
            "Attribution",  # vazio ou string fixa feito no cumulus ok
            "Width (mm)",
            "Height (mm)",
            "Latitude",  # camera
            "Longitude",  # camera
            "Depicts",  # wikidata
            "Wikidata ID",  # id do wikiddata
            "Smapshot ID",  # vazio
            "Media URL",  # Media URL do images
        ]
    ]

    metadata.name = "metadata"
    return metadata_new


@ dg.solid(
    input_defs=[dg.InputDefinition("jstor", root_manager_key="jstor_root")],
    output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="metadata", dagster_type=metadata)])
def metadata_jstor(context, jstor, metadata) -> dp.DataFrame:
    jstor = jstor.rename(columns=lambda x: re.sub(r'\[[0-9]*\]', '', x))
    jstor["Source ID"].fillna(jstor["SSID"], inplace=True)
    metadata = metadata.append(jstor)

    metadata_new = metadata[''
                            [
                                "Source ID",
                                "SSID",
                                "Title",
                                "Creator",
                                # vazio ou string fixa feito no cumulus ok
                                "Description (English)",
                                "Description (Portuguese)",
                                "Date",
                                "First Year",
                                "Last Year",
                                "Type",
                                "Item Set",
                                "Source",
                                "Source URL",  # url do portals
                                "Materials",
                                "Fabrication Method",
                                "Rights",  # vazio ou string fixa feito no cumulus ok
                                "License",  # vazio ou string fixa feito no cumulus ok
                                "Attribution",  # vazio ou string fixa feito no cumulus ok
                                "Width (mm)",
                                "Height (mm)",
                                "Latitude",  # camera
                                "Longitude",  # camera
                                "Depicts",  # wikidata
                                "Wikidata ID",  # id do wikiddata
                                "Smapshot ID",  # vazio
                                "Media URL",  # Media URL do images
                            ]
                            ]

    metadata_new.name = "metadata"
    return metadata_new.set_index("Source ID")


################   PIPELINE   ##################


@ dg.pipeline(
    mode_defs=[
        dg.ModeDefinition(
            name="default",
            resource_defs={
                "pandas_csv": df_csv_io_manager,
                "cumulus_root": root_input_csv,
                "jstor_root": root_input_xls,
                "wikidata_root": root_input_csv,
                "portals_root": root_input_csv,
                "camera_root": root_input_geojson,
                "images_root": root_input_csv,
            },
        )
    ],
    preset_defs=[
        dg.PresetDefinition(
            "default",
            run_config=preset,
            mode="default",
        )
    ],
)
def metadata_pipeline():
    metadata = create_metadata()
    metadata_jstor(metadata=metadata)


################   SENSORS   ##################


@ dg.sensor(pipeline_name="metadata_pipeline")
def trigger_metadata(context):
    metadata = "data/output/metadata.csv"
    if not os.path.exists(metadata):
        now = datetime.now().strftime("%d/%m/%Y%H%M%S")
        run_key = f"metadata_{now}"
        yield dg.RunRequest(run_key=run_key)
