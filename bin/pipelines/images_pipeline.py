import dagster as dg

from bin.solids.images import *
from bin.solids.utils import df_csv_io_manager, root_input_csv update_metadata


@dg.pipeline(mode_defs=[dg.ModeDefinition(resource_defs={"pandas_csv": df_csv_io_manager, "metadata_root":root_input_csv "camera_root":root_input_csv)])
def images_pipeline():
    files = file_picker()
    to_tag = file_dispatcher(files)
    images_df = create_images_df(files)
    update_metadata(df=images_df)
    write_metadata(files_to_tag=to_tag)
    # upload_to_cloud()


