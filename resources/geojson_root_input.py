import dagster as dg
import geopandas as gpd


@dg.root_input_manager(config_schema=dg.StringSource)
def geojson_root_input(context):
    """
    Reads GEOJSON file from project directory
    instead of upstream solid
    """
    return gpd.read_file(context.resource_config)  # return geopandas df
