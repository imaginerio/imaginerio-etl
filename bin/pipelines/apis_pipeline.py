import os
import dagster as dg
# from dagster_slack import slack_resource

from bin.solids.utils import df_csv_io_manager, merge_dfs, root_input # slack_solid
from bin.solids.apis import portals_dataframe, query_portals, query_wikidata, query_omeka, omeka_dataframe, wikidata_dataframe

#@dg.pipeline(mode_defs =[dg.ModeDefinition(resource_defs={"pandas_csv":df_csv_io_manager, "metadata_root":root_input, "slack": slack_resource})])
@dg.pipeline(mode_defs =[dg.ModeDefinition(resource_defs={"pandas_csv":df_csv_io_manager, "metadata_root":root_input})])
def apis_pipeline(): 
    #slack_solid()

    omeka_results = query_omeka()
    omeka_df = omeka_dataframe(omeka_results)
    #merge_dfs(df=omeka_df)
   

    wikidata_results = query_wikidata()
    wikidata_df = wikidata_dataframe(wikidata_results)
    #merge_dfs(df=wikidata_df)
    

    portals_results = query_portals()
    portals_df = portals_dataframe(portals_results)
    #merge_dfs(df=portals_df)  

#CLI: dagit -f bin/pipelines/apis_pipeline.py
    