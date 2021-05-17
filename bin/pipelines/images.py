import dagster as dg

from bin.solids.images import *
from bin.solids.utils import df_csv_io_manager, root_input, merge_dfs


@dg.pipeline(mode_defs=[dg.ModeDefinition(resource_defs={"pandas_csv": df_csv_io_manager, "metadata_root":root_input, "camera_root":root_input})])
def images_pipeline():
    geolocated, backlog = file_picker()
    to_tag = file_dispatcher(geolocated, backlog)
    images_df = create_images_df(geolocated, backlog)
    merge_dfs(df=images_df)
    # write_metadata(files=to_tag)
    # upload_to_cloud()


