import dagster as dg
import pandas as pd
from bin.solids.utils import df_csv_io_manager, root_input


@dg.solid(input_defs=[
    dg.InputDefinition( "omeka", root_manager_key= "omeka_root"),
    dg.InputDefinition("catalog", root_manager_key= "catalog_root"),
    dg.InputDefinition("wikidata", root_manager_key= "wikidata_root"),
    dg.InputDefinition("portals", root_manager_key= "portals_root"),
    dg.InputDefinition("camera", root_manager_key= "camera_root"),
    dg.InputDefinition("images", root_manager_key= "images_root")],
    output_defs=[
        dg.OutputDefinition(io_manager_key="pandas_csv", name="metadata")])

def create_metadata(context, omeka, catalog, wikidata, portals, camera,images):
    dataframes = [omeka, catalog, wikidata, portals, camera, images,]
    metadata = pd.DataFrame(columns=['id'])
    for df in dataframes:       
        metadata = metadata.merge(df, how="outer", on="id")
        id = metadata.pop('id')
        metadata.insert(0, 'id', id) 
       
    return metadata

@dg.pipeline(mode_defs =[dg.ModeDefinition(resource_defs={
    "pandas_csv":df_csv_io_manager,
    "catalog_root":root_input,
    "omeka_root":root_input,
    "catalog_root":root_input,
    "wikidata_root":root_input,
    "portals_root":root_input,
    "camera_root":root_input,
    "images_root":root_input})])

def metadata_pipeline():
    create_metadata()


#CLI dagit -f  bin/pipelines/metadata_pipeline.py
