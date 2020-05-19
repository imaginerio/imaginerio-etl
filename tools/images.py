import os, shutil, ffmpeg
import pandas as pd


source_folder = input("Source folder:")


def save_jpeg(image, output_folder, size=None, overwrite=False):
    stream = ffmpeg.input(image)
    filename, ext = (os.path.split(image)[1]).split(".")
    if not overwrite and os.path.exists(os.path.join(output_folder, f"{filename}.jpg")):
        print("File already exists")
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

<<<<<<< HEAD
finalizadas = [
    [root, name]
    # os.path.join(root, name)
    for root, dirs, files in os.walk(source_folder)
    for name in files
    if "FINALIZADAS" in root
    and name.endswith((".tif"))
    and not name.endswith(("v.tif"))
]


for root, name in finalizadas:
    image_path = os.path.join(root, name)
    if not os.path.exists(f"./images/master/{name}"):
        shutil.copy2(image_path, "./images/master")
    else:
        print("File already in folder")
    save_jpeg(f"./images/master/{name}", "./images/jpeg-sd", size=1000)
    save_jpeg(f"./images/master/{name}", "./images/jpeg-hd")

=======
def file_handler(source_folder):

    files = [
        (root, name)
        for root, dirs, files in os.walk(source_folder)
        for name in files
        if "FINALIZADAS" in root
        and name.endswith((".tif"))
        and not name.endswith(("v.tif"))
    ]
>>>>>>> ce5e373554dd6e8bba78408782850ef583b977fa

    for root, name in files:
        file_path = os.path.join(root, name)

<<<<<<< HEAD
# pandas creates a dataframe with all images available for a record_name
images_df = pd.DataFrame(name.split(".")[0] for root, name in finalizadas)
print(images_df.head())
# pandas saves all data regarding images in images/images.csv
=======
        if not os.path.exists(f"./images/master/{name}"):
            shutil.copy2(file_path, "./images/master")
        else:
            print("File already in folder")
        
        save_jpeg(file_path, "./images/jpeg-hd")
        save_jpeg(file_path, "./images/jpeg-sd", size=1000)

    return files


# pandas creates a dataframe with all images available for a record_name
# pandas saves all data regarding imagens in images/images.csv

def create_images_df(files):

    record_names = sorted([name.split(".")[0] for root, name in files])

    items = []

    for record_name in record_names:    
        item = {
            "Record Name":record_name, 
            "Full jpg":f"./images/jpeg-sd/{record_name}.jpg"
            }
        if os.path.isfile(f"./images/jpeg-sd/{record_name}c.jpg"):
            item["Cropped jpg"] = f"./images/jpeg-sd/{record_name}c.jpg"
        else:
            item["Cropped jpg"] = None
        items.append(item)

    images_df = pd.DataFrame(items)
    return images_df


def main():
    files = file_handler(source_folder)
    images_df = create_images_df(files)
    images_df.to_csv('./images/images.csv', index=False)

>>>>>>> ce5e373554dd6e8bba78408782850ef583b977fa
