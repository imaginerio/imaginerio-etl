import os
import shutil

import pandas as pd
import dagster as dg
from PIL import Image as PILImage

# from bin.utils import root_input


class Image:

    """Original image path. Attributes return id, jpg or tif extension."""

    def __init__(self, path):
        self.path = path
        self.jpg = str(os.path.split(self.path)[1].split(".")[0] + ".jpg")
        self.tif = str(os.path.split(self.path)[1])
        self.id = str(os.path.split(self.path)[1].split(".")[0])


@dg.solid
def file_picker(_):
    source = context.solid_config
    image_list = [
        Image(os.path.join(root, name))
        for root, dirs, files in os.walk(source)
        for name in files
        if "FINALIZADAS" in root
        and name.endswith((".tif"))
        and not name.endswith(("v.tif"))
        and not name.endswith("a.tif")
    ]
    return image_list


@dg.solid(input_defs=[dg.InputDefinition("camera", root_manager_key="camera_root")])
def file_dispatcher(context, image_list, camera):

    geolocated = list(camera["id"])
    TIFF = context.solid_config["TIFF"]
    JPEG_HD = context.solid_config["JPEG_HD"]
    JPEG_SD = context.solid_config["JPEG_SD"]

    for infile in image_list:

        if not os.path.exists(os.path.join(TIFF, infile.tif)):
            shutil.copy2(infile.path, TIFF)
        else:
            print(f"{infile.tif} already copied")

        if infile.id in geolocated:
            hdout = os.path.join("src", JPEG_HD, infile.jpg)
            sdout = os.path.join("src", JPEG_SD, infile.jpg)
            size = (1000, 1000)
            if not os.path.exists(hdout):
                try:
                    with PILImage.open(os.path.join(TIFF, infile.tif)) as im:
                        im.save(hdout)
                except OSError:
                    print("cannot convert", infile.tif)
            else:
                print("HD version already exists")
            if not os.path.exists(sdout):
                try:
                    with PILImage.open(os.path.join(TIFF, infile.tif)) as im:
                        im.thumbnail(size)
                        im.save(sdout)
                except OSError:
                    print("cannot create thumbnail for", infile.tif)
            else:
                print("SD version already exists")
        else:
            backlog = os.path.join("src", IMG_BACKLOG, infile.jpg)
            size = (1000, 1000)
            if not os.path.exists(backlog):
                try:
                    with PILImage.open(os.path.join(TIFF, infile.tif)) as im:
                        im.thumbnail(size)
                        im.save(backlog)
                except OSError:
                    print("cannot create backlog thumbnail for", infile.tif)
            else:
                print("Backlog version already exists")


@dg.solid
def create_images_df(_, image_list):
    return


@dg.solid
def write_metadata(_, jpeg_hd):
    return jpeg_hd


@dg.solid
def upload_to_cloud(_, jpeg_hd):
    return
