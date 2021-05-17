import os
import shutil

import pandas as pd
import dagster as dg
from PIL import Image as PILImage


class Image:

    """Original image path. Attributes return id, jpg or tif extension."""

    def __init__(self, path):
        self.path = path
        self.jpg = str(os.path.split(self.path)[1].split(".")[0] + ".jpg")
        self.tif = str(os.path.split(self.path)[1])
        self.id = str(os.path.split(self.path)[1].split(".")[0])


@dg.solid(input_defs=[dg.InputDefinition("camera", root_manager_key="camera_root")])
def file_picker(context):
    source = context.solid_config
    has_kml = list(camera["id"])
    image_list = [
        Image(os.path.join(root, name))
        for root, dirs, files in os.walk(source)
        for name in files
        if "FINALIZADAS" in root
        and name.endswith((".tif"))
        and not name.endswith(("v.tif"))
        and not name.endswith("a.tif")
    ]
    context.log.info(f"Globbed {len(image_list)} images")

    geolocated = [img for img in image_list if img.id in has_kml]
    backlog = [img for img in image_list if img.id not in has_kml]

    yield dg.Output(geolocated, "geolocated")
    yield dg.Output(backlog, "backlog")


@dg.solid
def file_dispatcher(context, geolocated, backlog):

    TIFF = context.solid_config["tiff"]
    JPEG_HD = context.solid_config["jpeg_hd"]
    JPEG_SD = context.solid_config["jpeg_sd"]
    IMG_BACKLOG = context.solid_config["backlog"]

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
            backlog = os.path.join(IMG_BACKLOG, infile.jpg)
            size = (1000, 1000)
            if not os.path.exists(backlog):
                try:
                    with PILImage.open(os.path.join(TIFF, infile.tif)) as im:
                        im.thumbnail(size)
                        im.save(backlog)
                except OSError:
                    print(f"Cannot create backlog thumbnail for {infile.tif}")

    handle_geolocated(geolocated)
    handle_backlog(backlog)

    to_tag = [
        os.path.join(JPEG_HD, file)
        for file in os.listdir(JPEG_HD)
        if not os.path.exists(os.path.join(JPEG_HD, f"{file}_original"))
    ]

    return to_tag

@dg.solid(output_defs=[dg.OutputDefinition(io_manager_key="pandas_csv", name="images")])
def create_images_df(context, geolocated, backlog)
    """Creates a dataframe with every image available and links to full size and thumbnail"""
    
    hd = context.solid_config
    sd = hd + "sd/"
    dicts = []

    for img in geolocated:
        img_dict = {
            id: img.id
            img_hd: os.path.join(hd, img.jpg)
            img_sd: os.path.join(sd, img.jpg)
        }
        dicts.append(img_dict)
    
    for img in backlog:
        img_dict = {
            id: img.id
            img_hd: np.Nan
            img_sd: os.path.join(sd, img.jpg)
        }
        dicts.append(img_dict)

    images_df = pd.DataFrame(data=dicts)
    images_df.sort_values(by='id')
    return images_df


@dg.solid(input_defs=[dg.InputDefinition("metadata", root_manager_key:"metadata_root"), dg.InputDefinition("ok", dg.Nothing)])
def write_metadata(_, metadata, files):

    metadata.fillna(value="", inplace=True)
    metadata.set_index("id", inplace=True)

    for i, item in enumerate(files):
        if item.endswith(".jpg"):
            basename = os.path.split(item)[1]
            name = basename.split(".")[0]
            try:
                if metadata.loc[name, "date_accuracy"] == "circa":
                    datecreated = (
                        metadata.loc[name, "start_date"].strftime("%Y")
                        + "/"
                        + metadata.loc[name, "end_date"].strftime("%Y")
                    )
                elif metadata.loc[name, "date_accuracy"] == "year":
                    datecreated = metadata.loc[name, "date"].strftime("%Y")
                elif metadata.loc[name, "date_accuracy"] == "month":
                    datecreated = metadata.loc[name, "date"].strftime("%Y-%m")
                elif metadata.loc[name, "date_accuracy"] == "day":
                    datecreated = metadata.loc[name, "date"].strftime("%Y-%m-%d")
            except AttributeError:
                print(f"Review {basename} date")
            byline = metadata.loc[name, "creator"]
            headline = metadata.loc[name, "title"]
            caption = metadata.loc[name, "description"]
            objecttype = metadata.loc[name, "type"]
            dimensions = (
                f'{metadata.loc[name, "image_width"]}cm x {metadata.loc[name, "image_height"]}cm'
            )
            keywords = metadata.loc[name, "wikidata_depict"].split("||")
            latitude = metadata.loc[name, "lat"]
            longitude = metadata.loc[name, "lng"]
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
                f"-IPTC:By-line={byline}",
                f"-IPTC:ObjectName={name}",
                f"-IPTC:Headline={headline}",
                f"-IPTC:Caption-Abstract={caption}",
                f"-IPTC:ObjectTypeReference={objecttype}",
                f"-IPTC:Keywords={keywords}",
                f"-GPSLatitude={latitude}",
                f"-GPSLongitude={longitude}",
                f"-GPSAltitude={altitude}",
                f"-GPSImgDirection={imgdirection}",
            ]
            with exiftool.ExifTool(executable_=context.solid_config) as et:
                for param in params:
                    param = param.encode(encoding="utf-8")
                    dest = item.encode(encoding="utf-8")
                    et.execute(param, dest)
            print(
                f"{basename}\n{metadata.loc[name, 'date']}\nTagged {i+1} of {len(files)} images"
            )


@dg.solid
def upload_to_cloud(_, jpeg_hd):
    return
