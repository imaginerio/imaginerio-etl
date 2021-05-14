from bin.solids.catalog import *
from bin.solids.utils import df_csv_io_manager
import dagster as dg

@dg.pipeline(mode_defs=[dg.ModeDefinition(resource_defs={"pandas_csv": df_csv_io_manager})])
def catalog_pipeline():
    catalog_main()