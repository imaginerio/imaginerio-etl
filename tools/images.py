import os, shutil, ffmpeg

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

finalizadas = [
    os.path.join(root, name)
    for root, dirs, files in os.walk(source_folder)
    for name in files
    if "FINALIZADAS" in root
    and name.endswith((".tif"))
    and not name.endswith(("v.tif"))
]

for image in finalizadas:
    if not os.path.exists(f"./images/master/{os.path.split(image)[1]}"):
        shutil.copy2(image, "./images/master")
    else:
        print("File already in folder")
    save_jpeg(image, "./images/jpeg-sd", size=1000)
    save_jpeg(image, "./images/jpeg-hd")

# ffmpeg converts files to .jpg in images/jpeg

# scale=1000:1000:force_original_aspect_ratio=decrease

# pandas creates a dataframe with all images available for a record_name

# pandas saves all data regarding imagens in images/images.csv
