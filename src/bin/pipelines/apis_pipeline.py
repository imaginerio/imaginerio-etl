import os
import dagster as dg

from src.bin.solids.utils import df_csv_io_manager
from src.bin.solids.apis import portals_dataframe, query_portals, query_wikidata, query_omeka, omeka_dataframe, wikidata_dataframe


@dg.pipeline(mode_defs =[dg.ModeDefinition(resource_defs={"pandas_csv":df_csv_io_manager})])
def apis(): 

    omeka_result = query_omeka()
    omeka_dataframe(omeka_result)

    wiki_data = query_wikidata()
    wikidata_dataframe(wiki_data)

    portals_result = query_portals
    portals_dataframe(portals_result) 

    




#CLI: dagit -f src\bin\pipelines\apis_pipeline.py
    