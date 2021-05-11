import os
import dagster as dg

from bin.solids.utils import df_csv_io_manager,query_api
from bin.solids.apis import query_omeka, create_dataframes


#query_omeka = configured(query_api, name="query_omeka")({"env": OMEKA_API"})
#query_wikidata = configured(query_api, name="aquery_wikidata ")({"env": "WIKIDATA_API"})
#query_portals= configured(query_api, name="aquery_wikidata ")({"env": os.environ["WIKIDATA_API"]})

#query_omeka = load_omeka.alias('omeka')
#query_wikidata = query_api.alias('wikidata')
#query_portals = query_api.alias('portals')


@dg.pipeline(mode_defs =[dg.ModeDefinition(resource_defs={"pandas_csv":df_csv_io_manager})])
def apis(): 

    results = query_omeka()
    create_dataframes(results)

    




#CLI: dagit -f bin\pipelines\apis_pipeline.py
    