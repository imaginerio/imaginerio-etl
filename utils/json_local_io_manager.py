import os

import dagster as dg
from IIIFpres.utilities import *
from IIIFpres.iiifpapi3 import *

import time


class JsonIOManager(dg.IOManager):
    def __init__(self, base_dir=None):
        return None

    def load_input(self, context):
        return "ok"

    def handle_output(self, context, obj):
        if obj:
            for item in obj:
                if item["type"] == "json":
                    path = item["key"]
                    data = item["data"]
                    os.makedirs(os.path.split(path)[0], exist_ok=True)
                    with open(path, "w") as f:
                        f.write(data)
                elif item["type"] == "path":
                    pass


@dg.io_manager
def json_local_io_manager(init_context):
    return JsonIOManager()
