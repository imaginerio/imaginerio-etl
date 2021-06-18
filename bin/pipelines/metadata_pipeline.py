import dagster as dg
import pandas as pd
from bin.solids.utils import *


@dg.solid(input_defs=[
    dg.InputDefinition( "omeka", root_manager_key= "omeka_root"),
    dg.InputDefinition( "catalog", root_manager_key= "catalog_root"),
    dg.InputDefinition( "wikidata", root_manager_key= "wikidata_root"),
    dg.InputDefinition( "portals", root_manager_key= "portals_root"),
    dg.InputDefinition("camera", root_manager_key= "camera_root"),
    dg.InputDefinition("images", root_manager_key= "images_root")],
    output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="metadata")])

def create_metadata(context, omeka, catalog, wikidata, portals, camera,images):
    camera_new = camera[['id','fov','longitude', 'altitude', 'latitude','heading', 'tilt', 'source', 'geometry', 'last_year', 'first_year']]
    dataframes = [omeka, catalog, wikidata, portals, camera_new, images]
    metadata = pd.DataFrame(columns=['id'])

    for df in dataframes:       
        metadata = metadata.merge(df, how="outer", on="id") 
    
    metadata_new = metadata[
        [   'id',
            'title', 
            'creator',
            'date_circa', 
            'date_created', 
            'date_accuracy', 
            'first_year', 
            'last_year', 
            'description', 
            'type', 
            'fabrication_method', 
            'image_width', 
            'image_height', 
            'source',
            'portals_id',
            'portals_url',
            'wikidata_depict', 
            'wikidata_id', 
            'wikidata_image',
            'wikidata_ims_id',
            'img_hd', 
            'img_sd',
            'omeka_url',        
            'latitude',
            'longitude',
            'altitude',
            'heading',           
            'tilt',
            'fov',            
            'geometry',

        ]
    ]
    
    return metadata_new.set_index('id')



@dg.pipeline(mode_defs =[dg.ModeDefinition(resource_defs={"pandas_csv":df_csv_io_manager,
    "catalog_root":root_input_csv,
    "omeka_root":root_input_csv,
    "catalog_root":root_input_csv,
    "wikidata_root":root_input_csv,
    "portals_root":root_input_csv,
    "camera_root":root_input_geojson,
    "images_root":root_input_csv})])

def metadata_pipeline():
    create_metadata()