import dagster as dg

from bin.solids.images import *
from bin.solids.utils import df_csv_io_manager, root_input, merge_dfs


@dg.pipeline(mode_defs=[dg.ModeDefinition(resource_defs={"pandas_csv": df_csv_io_manager, "metatada_root":root_input, "camera_root":root_input})])
def images_pipeline():
    image_list = file_picker()
    file_dispatcher(image_list)
    images_df = create_images_df(image_list)
    merge_dfs(images_df)


