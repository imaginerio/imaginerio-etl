import os

import dagster as dg
from IIIFpres.utilities import *
from IIIFpres.iiifpapi3 import *

import time


class LocalIOManager(dg.IOManager):
    def __init__(self, base_dir=None):
        return None

    def load_input(self, context):
        return "ok"

    def handle_output(self, context, path):
        for root, dirs, files in os.walk(path):
            for file in files:
                key = os.path.join(root, file)
                content = "image/jpeg" if file.endswith("jpg") else "application/json"
                # print("Would be uploading {0} with type {1}".format(key, content))
        # if obj:
        #     for item in obj:
        #         if item["type"] == "json":
        #             path = item["key"]
        #             data = item["data"]
        #             os.makedirs(os.path.split(path)[0], exist_ok=True)
        #             with open(path, "w") as f:
        #                 f.write(data)
        #         elif item["type"] == "path":
        #             pass


@dg.io_manager
def local_io_manager(init_context):
    return LocalIOManager()
