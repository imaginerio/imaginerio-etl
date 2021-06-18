import dagster as dg

from bin.solids.utils import *
from bin.solids.camera import *


@dg.pipeline(mode_defs =[dg.ModeDefinition(resource_defs={"geojson":geojson_io_manager, "pandas_csv":df_csv_io_manager, "metadata_root":root_input_csv)])
def camera_pipeline():

    kmls = get_list()
    kmls_splitteds=split_photooverlays(kmls)
    kmls_img_href = change_img_href(kmls_splitteds)
    kmls_altitude = correct_altitude_mode(kmls_img_href)
    geojson = create_geojson(kmls=kmls_altitude)
    merge_dfs(df=geojson)

#CLI dagster pipeline execute -f bin/pipelines/camera_pipeline.py -c bin/pipelines/camera_pipeline.yaml