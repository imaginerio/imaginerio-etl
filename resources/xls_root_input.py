import pandas as pd
import dagster as dg
from resources.df_csv_io_manager import function_int


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
            "SSID": function_int,
            "First Display Year[19466]": function_int,
            "Last Display Year[19467]": function_int,
        },
    )
