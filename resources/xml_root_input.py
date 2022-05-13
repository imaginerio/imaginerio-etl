import dagster as dg
from xml.etree import ElementTree


@dg.root_input_manager(config_schema=dg.StringSource)
def xml_root_input(context):
    """
    Reads XML file from project directory
    instead of upstream solid
    """
    path = context.resource_config
    with open(path, encoding="utf8") as f:
        tree = ElementTree.parse(f)
    root = tree.getroot()
    return root
