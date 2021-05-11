import os
import dagster as dg

from src.bin.solids.utils import df_csv_io_manager
from src.bin.solids.apis_env import  query_omeka, omeka_dataframe


@dg.pipeline(mode_defs =[dg.ModeDefinition(resource_defs={"pandas_csv":df_csv_io_manager})])
def apis(): 

    omeka_result = query_omeka()
    omeka_dataframe(omeka_result) 
    




#CLI: dagit -f src\bin\pipelines\apis_pipeline_env.py
    