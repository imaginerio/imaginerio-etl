import os

import dagster as dg
from bin.solids.catalog import (creators_list, dates_accuracy,
                                extract_dimensions, organize_columns, xml_to_df)
from bin.solids.utils import (df_csv_io_manager, merge_dfs, root_input,
                              root_input_xml)


@dg.pipeline(mode_defs =[dg.ModeDefinition(resource_defs={"pandas_csv":df_csv_io_manager,  "metadata_root":root_input, "xml": root_input_xml})])
def calatog_main(): 
    catalog_df = xml_to_df()
    catalog_df = organize_columns(catalog_df)   
    catalog_df = dates_accuracy(catalog_df)
    catalog = extract_dimensions(catalog_df)
    listed_creators = creators_list(catalog_df)
    merge_dfs(df=catalog)
  


#CLI: dagit -f bin/pipelines/catalog_pipeline.py 


