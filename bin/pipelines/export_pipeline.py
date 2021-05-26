
import dagster as dg
from bin.solids.export import *
from bin.solids.utils import df_csv_io_manager, geojson_io_manager, root_input
# from dagster_slack import slack_resource


@dg.pipeline(mode_defs =[dg.ModeDefinition(resource_defs={"pandas_csv":df_csv_io_manager, "jstor_root":root_input,"geojson":geojson_io_manager,"metadata_root":root_input})])
def export_pipeline():
    
    export_df = load_metadata()

    #Import OMEKA
    accuraced_dates = dates_accuracy_to_omeka(export_df)
    omeka_organized = organize_columns_to_omeka(accuraced_dates)
    omeka_df = import_omeka_dataframe(df=omeka_organized)

    #import GIS
    gis_df = organize_df_to_gis(export_df)
    feature_collection = create_featureCollection(gis_df)

    #import WIKIDATA
    wikidata_df = make_df_to_wikidata(export_df)
    organised_creator = organise_creator(wikidata_df) 

#CLI: dagit -f bin/pipelines/export_pipeline.py
    
   
