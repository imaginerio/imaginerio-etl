import pandas as pd
import dagster as dg
from utils.pandas_csv_io import function_int


@dg.root_input_manager(config_schema=dg.StringSource)
def xls_root_input(context):
    """
    Reads XLS file from project directory
    instead of upstream solid
    """
    path = context.resource_config
    return pd.read_excel(
        path,
        converters={
            "First Year[19466]": function_int,
            "Last Year[19467]": function_int,
        },
    )
