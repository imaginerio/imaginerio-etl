import os, shutil, ffmpeg
import pandas as pd


SOURCE_PATH = input("Source folder:")

camera = pd.read_csv(os.path.join(os.environ['CAMERA_PATH'], "camera.csv")
geolocated = list(camera["name"])


class Image:

    """Original image path. Attributes return id, jpg or tif extension."""

    def __init__(self, path):
        self.path = path
        self.jpg = str(os.path.split(self.path)[1].split(".")[0] + ".jpg")
        self.tif = str(os.path.split(self.path)[1])
        self.id = str(os.path.split(self.path)[1].split(".")[0])


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


def file_handler(source_folder):
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
        if image.id in geolocated:
            if not os.path.exists(os.path.join(os.environ['MASTER'], image.tif)):
                shutil.copy2(image.path, os.environ['MASTER'])
            else:
                print(f"{image.tif} already in folder")

            save_jpeg(os.path.join(os.environ['MASTER'], image.tif), os.environ['JPEG_HD'])
            save_jpeg(os.path.join(os.environ['MASTER'], image.tif), os.environ['JPEG_SD'], size=1000)

    return files


# pandas creates a dataframe with all images available for a id
# pandas saves all data regarding imagens in images/images.csv


def create_images_df(files):
    """Creates a dataframe with every image available and its alternate versions.
    'files' is a list of Image objects."""

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
            item = {
                "id": image[0].id,
                "img_hd": os.path.join(os.environ['CLOUD_PATH'], image[0].jpg),
                "img_sd": os.path.join(os.environ['GITHUB_PATH'], image[0].jpg),
            }
            for i in image[1:]:
                item[f"{i.id[-1]}"] = i.jpg
        else:
            item = {
                "id": image.id,
                "img_hd": os.path.join(os.environ['CLOUD_PATH'], image.jpg),
                "img_sd": os.path.join(os.environ['GITHUB_PATH'], image.jpg),
            }
        items.append(item)

    images_df = pd.DataFrame(items)
    images_df.sort_values(by=["id"])

    return images_df


def main():
    """Execute all functions."""
    files = file_handler(SOURCE_PATH)

    print("Creating image dataframe...")

    images_df = create_images_df(files)

    print(images_df.head())
    images_df.to_csv(os.environ['IMAGES_PATH'], index=False)


if __name__ == "__main__":
    main()
