import os


import dagster as dg
from bin.solids.catalog import (creators_list, dates_accuracy,
                                    extract_dimensions, organize_columns,
                                    read_xml, xml_to_df)
from bin.solids.utils import df_csv_io_manager,slack_solid


@dg.pipeline(mode_defs =[dg.ModeDefinition(resource_defs={"pandas_csv":df_csv_io_manager})])
def calatog_main():
    root = read_xml()   
    catalog_df = xml_to_df(root)
    catalog_df = organize_columns(catalog_df)   
    catalog_df = dates_accuracy(catalog_df)
    catalog = extract_dimensions(catalog_df)
    listed_creators = creators_list(catalog_df)
    
    
    return catalog 


#CLI: dagit -f sbin\pipelines\catalog_pipeline.py 


