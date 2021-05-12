import os
import shutil

import pandas as pd
import dagster as dg
from PIL import Image as PILImage
from dotenv import load_dotenv

load_dotenv(override=True)

# environment variables
SOURCE = os.environ["SOURCE"]
IMG_BACKLOG = os.environ["IMG_BACKLOG"]
TIFF = os.environ["TIFF"]
JPEG_HD = os.environ["JPEG_HD"]
JPEG_SD = os.environ["JPEG_SD"]

# list geolocated items
camera = pd.read_csv("src/" + os.environ["CAMERA_CSV"])
geolocated = list(camera["name"])


class Image:

    """Original image path. Attributes return id, jpg or tif extension."""

    def __init__(self, path):
        self.path = path
        self.jpg = str(os.path.split(self.path)[1].split(".")[0] + ".jpg")
        self.tif = str(os.path.split(self.path)[1])
        self.id = str(os.path.split(self.path)[1].split(".")[0])


@dg.solid
def file_picker(_, source):
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


@dg.solid
def file_dispatcher(_, image_list, geolocated):
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
    return files


@dg.solid
def create_images_df(_, image_list):
    return


@dg.solid
def write_metadata(_, jpeg_hd):
    return jpeg_hd


@dg.solid
def upload_to_cloud(_, jpeg_hd):
    return


def file_handler(source_folder):
    """
    Returns list of original files in source folder according to internal requirements, 
    copies them to master and saves jpegs.
    """

    files = [
        Image(os.path.join(root, name))
        for root, dirs, files in os.walk(SOURCE)
        for name in files
        if "FINALIZADAS" in root
        and name.endswith((".tif"))
        and not name.endswith(("v.tif"))
    ]

    for infile in files:

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
    return files


def backlog_handler(path):
    backlog = os.listdir(path)
    for image in backlog:
        if image.split(".")[0] in geolocated:
            if not os.path.exists(os.path.join(JPEG_SD, image)):
                os.rename(
                    os.path.join(IMG_BACKLOG, image), os.path.join(JPEG_SD, image)
                )
            else:
                os.remove(os.path.join(IMG_BACKLOG, image))
        else:
            continue


def create_images_df(folder):
    """Creates a dataframe with every image available and links to full size and thumbnail"""

    imgs = os.listdir(folder)
    imgs.remove("backlog")

    df = {
        "id": [img.split(".")[0] for img in imgs],
        "img_hd": [os.path.join(os.environ["CLOUD"] + image) for image in imgs],
        "img_sd": [os.path.join(os.environ["THUMB"] + image) for image in imgs],
    }

    images_df = pd.DataFrame(data=df)
    images_df.drop_duplicates(inplace=True)
    return images_df


def main():
    """Execute all functions."""

    files = file_handler(SOURCE)
    backlog_handler(IMG_BACKLOG)

    print("Creating image dataframe...")

    images_df = create_images_df(JPEG_SD)

    print(images_df.head())

    images_df.to_csv(os.path.join("src", os.environ["IMAGES"]), index=False)


if __name__ == "__main__":
    main()
