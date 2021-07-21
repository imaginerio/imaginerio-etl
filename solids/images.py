import os
import shutil

import dagster as dg
import pandas as pd
from numpy import nan
from PIL import Image as PILImage

from solids.exiftool import ExifTool


class Image:

    """Original image path. Attributes return id, jpg or tif extension."""

    def __init__(self, path):
        self.path = path
        self.jpg = str(os.path.split(self.path)[1].split(".")[0] + ".jpg")
        self.tif = str(os.path.split(self.path)[1])
        self.id = str(os.path.split(self.path)[1].split(".")[0])


@dg.solid(
    config_schema=dg.StringSource,
    input_defs=[dg.InputDefinition("camera", root_manager_key="camera_root")],
)
def file_picker(context, camera):
    source = context.solid_config
    has_kml = list(camera["Source ID"])
    image_list = [
        Image(os.path.join(root, name))
        for root, dirs, files in os.walk(source)
        for name in files
        if "FINALIZADAS" in root
        and name.endswith((".tif"))
        and not name.endswith(("v.tif"))
        and not name.endswith("a.tif")
    ]

    geolocated = [img for img in image_list if img.id in has_kml]
    backlog = [img for img in image_list if img.id not in has_kml]

    context.log.info(
        f"Geolocated: {len(geolocated)} images; Backlog: {len(backlog)} images"
    )

    return {"geolocated": geolocated, "backlog": backlog}


@dg.solid(config_schema=dg.StringSource)
def file_dispatcher(context, files):

    geolocated = files["geolocated"]
    TIFF = context.solid_config["env"]["tiff"]
    JPEG_HD = context.solid_config["env"]["jpeg_hd"]
    JPEG_SD = context.solid_config["env"]["jpeg_sd"]
    IMG_BACKLOG = context.solid_config["env"]["backlog"]

    def handle_geolocated(infiles):
        for infile in infiles:
            if not os.path.exists(os.path.join(TIFF, infile.tif)):
                shutil.copy2(infile.path, TIFF)

            hdout = os.path.join(JPEG_HD, infile.jpg)
            sdout = os.path.join(JPEG_SD, infile.jpg)
            size = (1000, 1000)

            if not os.path.exists(hdout):
                try:
                    with PILImage.open(os.path.join(TIFF, infile.tif)) as im:
                        im.save(hdout)
                except OSError:
                    context.log.info(f"Cannot convert {infile.tif}")

            if not os.path.exists(sdout):
                try:
                    with PILImage.open(os.path.join(TIFF, infile.tif)) as im:
                        im.thumbnail(size)
                        im.save(sdout)
                except OSError:
                    context.log.info(f"Cannot create thumbnail for {infile.tif}")

    def handle_backlog(infiles):
        for infile in infiles:
            if not os.path.exists(os.path.join(TIFF, infile.tif)):
                shutil.copy2(infile.path, TIFF)
            backlog_path = os.path.join(IMG_BACKLOG, infile.jpg)
            size = (1000, 1000)
            if not os.path.exists(backlog_path):
                try:
                    with PILImage.open(os.path.join(TIFF, infile.tif)) as im:
                        im.thumbnail(size)
                        im.save(backlog_path)
                except OSError:
                    context.log.info(
                        f"Cannot create backlog thumbnail for {infile.tif}"
                    )
        current = os.listdir(IMG_BACKLOG)
        for image in current:
            if image.split(".")[0] in geolocated:
                if not os.path.exists(os.path.join(JPEG_SD, image)):
                    os.rename(
                        os.path.join(IMG_BACKLOG, image), os.path.join(JPEG_SD, image)
                    )
                else:
                    os.remove(os.path.join(IMG_BACKLOG, image))
            else:
                continue

    handle_geolocated(files["geolocated"])
    handle_backlog(files["backlog"])

    to_tag = [
        os.path.join(JPEG_HD, file)
        for file in os.listdir(JPEG_HD)
        if not os.path.exists(os.path.join(JPEG_HD, f"{file}_original"))
        and not file.endswith("_original")
    ]

    context.log.info(f"Passed {len(to_tag)} images to be tagged")
    return to_tag


@dg.solid(
    config_schema=dg.StringSource,
    output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="images")],
)
def create_images_df(context, files):
    """Creates a dataframe with every image available and links to full size and thumbnail"""

    hd = context.solid_config
    sd = hd + "sd/"
    dicts = []

    for img in files["geolocated"]:
        img_dict = {
            "Source ID": img.id,
            "Media URL": os.path.join(hd, img.jpg),
            "img_sd": os.path.join(sd, img.jpg),
        }
        dicts.append(img_dict)

    for img in files["backlog"]:
        img_dict = {
            "Source ID": img.id,
            "Media URL": nan,
            "img_sd": nan,
        }
        dicts.append(img_dict)

    images_df = pd.DataFrame(data=dicts)
    images_df.sort_values(by="Source ID")
    context.log.info(f"{len(images_df)} images available in hi-res")

    return images_df.set_index("Source ID", inplace=True)


@dg.solid(
    config_schema=dg.StringSource,
    input_defs=[dg.InputDefinition("metadata", root_manager_key="metadata_root")],
)
def write_metadata(context, metadata, files_to_tag):

    metadata.fillna(value="", inplace=True)
    metadata.set_index("Source ID", inplace=True)

    for i, item in enumerate(files_to_tag):
        if item.endswith(".jpg"):
            basename = os.path.split(item)[1]
            name = basename.split(".")[0]
            # date = metadata.loc[name, "Date"]
            byline = metadata.loc[name, "Creator"]
            headline = metadata.loc[name, "Title"]
            caption = metadata.loc[name, "Description (Portuguese)"]
            objecttype = metadata.loc[name, "Type"]
            # dimensions = f'{metadata.loc[name, "image_width"]}cm x {metadata.loc[name, "image_height"]}cm'
            keywords = metadata.loc[name, "wikidata_depict"].split("||")
            latitude = metadata.loc[name, "latitude"]
            longitude = metadata.loc[name, "longitude"]
            altitude = metadata.loc[name, "height"]
            imgdirection = metadata.loc[name, "heading"]

            params = [
                "-IPTC:Source=Instituto Moreira Salles/IMS",
                "-IPTC:CopyrightNotice=This image is in the Public Domain.",
                "-IPTC:City=Rio de Janeiro",
                "-IPTC:Province-State=RJ",
                "-IPTC:Country-PrimaryLocationName=Brasil",
                "-GPSLatitudeRef=S",
                "-GPSLongitudeRef=W",
                "-GPSAltitudeRef=0",
                "-GPSImgDirectionRef=T",
                # f"-IPTC:DateCreated={date}",
                f"-IPTC:By-line={byline}",
                f"-IPTC:ObjectName={name}",
                f"-IPTC:Headline={headline}",
                f"-IPTC:Caption-Abstract={caption}",
                f"-IPTC:ObjectTypeReference={objecttype}",
                # f"-IPTC:Dimensions={dimensions}",
                f"-IPTC:Keywords={keywords}",
                f"-GPSLatitude={latitude}",
                f"-GPSLongitude={longitude}",
                f"-GPSAltitude={altitude}",
                f"-GPSImgDirection={imgdirection}",
            ]
            with ExifTool(executable_=context.solid_config) as et:
                for param in params:
                    param = param.encode(encoding="utf-8")
                    dest = item.encode(encoding="utf-8")
                    et.execute(param, dest)
            context.log.info(
                f"{basename}\n{metadata.loc[name, 'date']}\nTagged {i+1} of {len(files_to_tag)} images"
            )


@dg.solid
def upload_to_cloud(_, jpeg_hd):
    return
