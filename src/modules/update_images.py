import os
import shutil

import ffmpeg
import pandas as pd
from PIL import Image as PILImage
from dotenv import load_dotenv

load_dotenv(override=True)

# path to images source on remote drive
SOURCE = os.environ["SOURCE"]

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


def file_handler(source_folder):
    """
    Returns list of original files in source folder according to internal requirements, 
    copies them to master and saves jpegs.
    """

    files = [
        Image(os.path.join(root, name))
        for root, dirs, files in os.walk(os.environ["SOURCE"])
        for name in files
        if "FINALIZADAS" in root
        and name.endswith((".tif"))
        and not name.endswith(("v.tif"))
    ]

    for infile in files:

        if not os.path.exists(os.path.join(os.environ["TIFF"], infile.tif)):
            shutil.copy2(infile.path, os.environ["TIFF"])
        else:
            print(f"{infile.tif} already copied")

        if infile.id in geolocated:
            hdout = os.path.join("src", os.environ["JPEG_HD"], infile.jpg)
            sdout = os.path.join("src", os.environ["JPEG_SD"], infile.jpg)
            size = (1000, 1000)
            if not os.path.exists(hdout):
                try:
                    with PILImage.open(
                        os.path.join(os.environ["TIFF"], infile.tif)
                    ) as im:
                        im.save(hdout)
                except OSError:
                    print("cannot convert", infile.tif)
            else:
                print("HD version already exists")
            if not os.path.exists(sdout):
                try:
                    with PILImage.open(
                        os.path.join(os.environ["TIFF"], infile.tif)
                    ) as im:
                        im.thumbnail(size)
                        im.save(sdout)
                except OSError:
                    print("cannot create thumbnail for", infile.tif)
            else:
                print("SD version already exists")
        else:
            backlog = os.path.join("src", os.environ["IMG_BACKLOG"], infile.jpg)
            size = (1000, 1000)
            if not os.path.exists(backlog):
                try:
                    with PILImage.open(
                        os.path.join(os.environ["TIFF"], infile.tif)
                    ) as im:
                        im.thumbnail(size)
                        im.save(backlog)
                except OSError:
                    print("cannot create backlog thumbnail for", infile.tif)
            else:
                print("Backlog version already exists")
    return files


def create_images_df(files):
    """Creates a dataframe with every image available and its alternate versions.
    "files" is a list of Image objects."""

    groups = []
    to_remove = []
    items = []

    # Find ids that contain other ids (secondary versions) and group them together
    for item in files:
        i = 0
        matched = []
        while i < len(files):
            if item.id in files[i].id:
                matched.append(files[i])
            i += 1
        if len(matched) > 1:
            groups.append(matched)
        else:
            groups.append(item)

    # Remove secondary versions from main list
    for group in groups:
        if type(group) == list:
            to_remove += group[1:]

    groups = [item for item in groups if item not in to_remove]

    # Create list of dicts with all files available for each item
    for image in groups:
        if type(image) == list:
            # include just the 'master' version when others are available
            if image[0].id in geolocated:
                item = {
                    "id": image[0].id,
                    "img_hd": os.path.join(os.environ["CLOUD"], image[0].jpg),
                    "img_sd": os.path.join(os.environ["GITHUB"], image[0].jpg),
                }
                for i in image[1:]:
                    item[f" {i.id[-1]}"] = i.jpg
            else:
                # if item isn't geolocated, send to github backlog
                item = {
                    "id": image[0].id,
                    "img_sd": os.path.join(
                        os.environ["GITHUB"], "backlog", image[0].jpg
                    ),
                }
        else:
            if image.id in geolocated:
                item = {
                    "id": image.id,
                    "img_hd": os.path.join(os.environ["CLOUD"], image.jpg),
                    "img_sd": os.path.join(os.environ["GITHUB"], image.jpg),
                }
            else:
                # if item isn't geolocated, send to github backlog
                item = {
                    "id": image.id,
                    "img_sd": os.path.join(os.environ["GITHUB"], "backlog", image.jpg),
                }
        items.append(item)

    images_df = pd.DataFrame(items)
    images_df.sort_values(by=["id"])

    return images_df


def main():
    """Execute all functions."""
    files = file_handler(SOURCE)

    print("Creating image dataframe...")

    images_df = create_images_df(files)

    print(images_df.head())

    images_df.to_csv(os.path.join("src", os.environ["IMAGES"]), index=False)


if __name__ == "__main__":
    main()
