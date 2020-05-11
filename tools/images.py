import os, shutil, ffmpeg

source_folder = input("Source folder:")

# user insert all .tif files in images/master
finalizadas = [
    os.path.join(root, name)
    for root, dirs, files in os.walk(source_folder)
    for name in files
    if "FINALIZADAS" in root
    if name.endswith((".tif"))
    if not name.endswith(("v.tif"))
]

for image in finalizadas:
    filename = os.path.split(image)[1]
    shutil.copy2(image, ".\images\master")

# ffmpeg converts files to .jpg in images/jpeg

# pandas creates a dataframe with all images available for a record_name

# pandas saves all data regarding imagens in images/images.csv
