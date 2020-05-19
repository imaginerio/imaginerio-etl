import os, shutil, ffmpeg
import pandas as pd


SOURCE_PATH = input("Source folder:")
GITHUB_PATH = (
    "https://raw.githubusercontent.com/imaginerio/situated-views/master/images/jpeg-sd/"
)
CLOUD_PATH = "/cloud/"
MASTER = "./images/master/"
JPEG_HD = "./images/jpeg-hd/"
JPEG_SD = "./images/jpeg-sd/"


class Image:

    """Original image path. Methods return pure filename, with jpg or tif extension."""

    def __init__(self, path):
        self.path = path
        self.jpg = str(os.path.split(self.path)[1].split(".")[0] + ".jpg")
        self.tif = str(os.path.split(self.path)[1])
        self.record_name = str(os.path.split(self.path)[1].split(".")[0])

    """
    def jpg(self):
        str(os.path.split(self.path)[1].split(".")[0] + ".jpg")

    def tif(self):
        str(os.path.split(self.path)[1])

    def record_name(self):
        str(os.path.split(self.path)[1].split(".")[0])"""


def save_jpeg(image, output_folder, size=None, overwrite=False):
    """
    Saves jpg file using ffmpeg or returns None if file already exists and overwrite=False.
    'size' is the largest dimension in resulting image.
    """

    stream = ffmpeg.input(image)
    filename = (os.path.split(image)[1]).split(".")[0]
    if not overwrite and os.path.exists(os.path.join(output_folder, f"{filename}.jpg")):
        print(f"{filename}.jpg already exists")
        return None

    if size:
        stream = ffmpeg.filter(
            stream, "scale", size, size, force_original_aspect_ratio="decrease"
        )
    stream = ffmpeg.output(
        stream, os.path.join(output_folder, filename) + ".jpg", **{"q": 0}
    )
    ffmpeg.run(stream, overwrite_output=True)


# user insert all .tif files in images/master
# ffmpeg converts files to .jpg in images/jpeg


def file_handler(source_folder, master=MASTER, hd_folder=JPEG_HD, sd_folder=JPEG_SD):
    """
    Returns list of original files in source folder according to internal requirements, 
    copies them to master and saves jpegs.
    """
    files = [
        Image(os.path.join(root, name))
        for root, dirs, files in os.walk(source_folder)
        for name in files
        if "FINALIZADAS" in root
        and name.endswith((".tif"))
        and not name.endswith(("v.tif"))
    ]

    for image in files:
        if not os.path.exists(os.path.join(master, str(image.tif))):
            shutil.copy2(image.path, master)
        else:
            print(f"{image.tif} already in folder")

        save_jpeg(os.path.join(master, str(image.tif)), hd_folder)
        save_jpeg(os.path.join(master, str(image.tif)), sd_folder, size=1000)

    return files


# pandas creates a dataframe with all images available for a record_name
# pandas saves all data regarding imagens in images/images.csv


def create_images_df(files, github_path=GITHUB_PATH, cloud_path=CLOUD_PATH):
    """Creates a dataframe with every image available and its alternate versions."""

    record_names = sorted([image.record_name for image in files])

    items = []

    for image in record_names:
        item = {
            "record_name": Image(image).record_name,
            "img_hd": os.path.join(cloud_path, Image(image).jpg),
            "img_sd": os.path.join(github_path, Image(image).jpg),
        }
        if os.path.isfile(os.path.join(cloud_path, f"{Image(image).record_name}c.jpg")):
            item["img_hd_crop"] = os.path.join(
                cloud_path, f"{Image(image).record_name}c.jpg"
            )
        else:
            item["img_hd_crop"] = None
        items.append(item)

    images_df = pd.DataFrame(items)
    return images_df


def main():
    """Execute all functions."""
    files = file_handler(SOURCE_PATH)

    print("Creating image dataframe...")

    images_df = create_images_df(files)

    print(images_df.head())
    images_df.to_csv("./images/images.csv", index=False)


if __name__ == "__main__":
    main()


"""
ls = [i.split(".")[0] for i in os.listdir("./images/jpeg-sd")]
groups = []


for id in sorted(ls):
    i = 0
    matched = []
    while i < len(ls):
        if id in ls[i]:
            matched.append(ls[i])
        i += 1
    if len(matched) > 1:    
        groups.append(sorted(matched))
    else:
        groups.append(id)


to_remove = []
for group in groups:
    if type(group) == list:
        to_remove += group[1:]

groups = [item for item in groups if item not in to_remove]
print(groups)
"""
