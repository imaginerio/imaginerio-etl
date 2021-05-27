import os
import dagster as dg

from bin.solids.utils import geojson_io_manager
from bin.solids.camera import get_list, split_photooverlays, change_img_href, correct_altitude_mode, create_geojson, geojson_geodataframe

@dg.pipeline
def camera_pipeline():

    kmls = get_list()
    kmls_splitteds=split_photooverlays(kmls)
    kmls_img_href = change_img_href(kmls_splitteds)
    kmls_altitude = correct_altitude_mode(kmls_img_href)
    geojson = create_geojson(kmls_altitude)
    camera_df = geojson_geodataframe(geojson)