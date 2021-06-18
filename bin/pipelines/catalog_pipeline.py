import os

import dagster as dg
from bin.solids.catalog import (creators_list, dates_accuracy,
                                extract_dimensions, organize_columns, xml_to_df)
from bin.solids.utils import (df_csv_io_manager, update_metadata, root_input_csv
                              root_input_xml)


@dg.pipeline(mode_defs =[dg.ModeDefinition(resource_defs={"pandas_csv":df_csv_io_manager,  "metadata_root":root_input_csv "xml": root_input_xml})])
def catalog_pipeline(): 
    catalog_df = xml_to_df()
    catalog_df = organize_columns(catalog_df)
    catalog_df = extract_dimensions(catalog_df)    
    listed_creators = creators_list(catalog_df)
    catalog = dates_accuracy(catalog_df)
    update_metadata(df=catalog)

#CLI: dagit -f bin/pipelines/catalog_pipeline.py 
#CLI: dagster pipeline execute -f bin/pipelines/catalog_pipeline.py -c bin/pipelines/catalog_pipeline.yaml