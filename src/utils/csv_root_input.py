import dagster as dg
import pandas as pd
import numpy as np
#from pandas_csv_io import function_int


def function_int(x):
    return str(str(x).split(".")[0]) if x else np.nan


@dg.root_input_manager(config_schema=dg.StringSource)
def csv_root_input(context):
    """
    Reads CSV file from project directory
    instead of upstream solid
    """
    df = pd.read_csv(context.resource_config, error_bad_lines=False)

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
        context.resource_config, error_bad_lines=False, converters=conversion
    )
