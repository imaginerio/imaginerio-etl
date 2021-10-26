import os
import re
import shutil
from dagster.builtins import Nothing
import dagster_pandas as dp

import boto3
import dagster as dg
import pandas as pd
import numpy as np
from tqdm import tqdm
from numpy import nan
from dotenv import load_dotenv
from PIL import Image as PILImage
from tests.dataframe_types import *
from tests.objects_types import *

from solids.exiftool import ExifTool

load_dotenv(override=True)



class Image:

    """
    Original image path. Attributes return id, jpg or tif extension.
    """

    def __init__(self, path):
        self.path = path
        self.jpg = str(os.path.split(self.path)[1].split(".")[0] + ".jpg")
        self.tif = str(os.path.split(self.path)[1])
        self.id = str(os.path.split(self.path)[1].split(".")[0])


@dg.solid(
    config_schema=dg.StringSource,
    input_defs=[dg.InputDefinition("metadata", root_manager_key="metadata_root")],
    output_defs=[dg.OutputDefinition(dagster_type=dict)],
)
def file_picker(context, metadata: dp.DataFrame):
    """
    Walks directory tree and glob relevant files.
    Returns dictionary with geolocated, backlog
    and files for review
    """
    source = context.solid_config
    metadata["Source ID"] = metadata["Source ID"].str.upper()
    has_kml = list(metadata.loc[metadata["Latitude"].notna(), "Source ID"])
    catalog = list(metadata["Source ID"])
    image_list = [
        Image(os.path.join(root, name))
        for root, dirs, files in os.walk(source)
        for name in files
        if "FINALIZADAS" in root
        and name.endswith((".tif"))
        and not re.search("[a-z]\.tif$", name)
    ]

    geolocated = [img for img in image_list if img.id.upper() in has_kml]
    backlog = [
        img
        for img in image_list
        if img.id.upper() in catalog and img.id.upper() not in has_kml
    ]
    review = [img for img in image_list if img.id.upper() not in catalog]

    context.log.info(
        f"Geolocated: {len(set([img.id for img in geolocated]))} images; Backlog: {len(set([img.id for img in backlog]))} images"
    )

    return {"geolocated": geolocated, "backlog": backlog, "review": review}


@dg.solid(
    config_schema={
        "tiff": dg.StringSource,
        "jpeg_hd": dg.StringSource,
        "jpeg_sd": dg.StringSource,
        "backlog": dg.StringSource,
        "review": dg.StringSource,
    }
)
def file_dispatcher(context, files: dict):
    """
    Copy failsafe TIFFs, generate high and low
    res JPEGs, and separate files for review
    """

    TIFF = context.solid_config["tiff"]
    JPEG_HD = context.solid_config["jpeg_hd"]
    JPEG_SD = context.solid_config["jpeg_sd"]
    IMG_BACKLOG = context.solid_config["backlog"]
    REVIEW = context.solid_config["review"]

    def handle_geolocated(infiles):
        for infile in tqdm(infiles, "Handling geolocated..."):
            if not os.path.exists(os.path.join(TIFF, infile.tif)):
                shutil.copy2(infile.path, TIFF)

            hd_path = os.path.join(JPEG_HD, infile.jpg)
            sd_path = os.path.join(JPEG_SD, infile.jpg)
            review_path = os.path.join(REVIEW, infile.jpg)
            size = (1000, 1000)

            if not os.path.exists(hd_path):
                try:
                    with PILImage.open(os.path.join(TIFF, infile.tif)) as im:
                        im.save(hd_path)
                except OSError:
                    context.log.info(f"Cannot convert {infile.tif}")

            if not os.path.exists(sd_path):
                try:
                    with PILImage.open(os.path.join(TIFF, infile.tif)) as im:
                        im.thumbnail(size)
                        im.save(sd_path)
                except OSError:
                    context.log.info(f"Cannot create thumbnail for {infile.tif}")

            if os.path.exists(review_path):
                os.remove(review_path)

    def handle_backlog(infiles):
        for infile in tqdm(infiles, "Handling backlog..."):
            if not os.path.exists(os.path.join(TIFF, infile.tif)):
                shutil.copy2(infile.path, TIFF)
            backlog_path = os.path.join(IMG_BACKLOG, infile.jpg)
            review_path = os.path.join(REVIEW, infile.jpg)
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
            if os.path.exists(review_path):
                os.remove(review_path)
        current_backlog = os.listdir(IMG_BACKLOG)
        for image in current_backlog:
            if image in [img.jpg for img in files["geolocated"]]:
                if not os.path.exists(os.path.join(JPEG_SD, image)):
                    os.rename(
                        os.path.join(IMG_BACKLOG, image), os.path.join(JPEG_SD, image)
                    )
                else:
                    os.remove(os.path.join(IMG_BACKLOG, image))
            else:
                continue

    def handle_review(infiles):
        # current_hd = [os.path.join(JPEG_HD, img) for img in os.listdir(JPEG_HD) if img.split(".")[0] in files["review"]]
        for infile in tqdm(infiles, "Handling review..."):
            review_path = os.path.join(REVIEW, infile.jpg)
            tiff_path = os.path.join(TIFF, infile.tif)
            if not os.path.exists(review_path):
                try:
                    with PILImage.open(infile.path) as im:
                        im.save(review_path)
                except OSError:
                    context.log.info(f"Cannot convert {infile.tif}")
            else:
                continue
            if os.path.exists(tiff_path):
                os.remove(tiff_path)

    handle_geolocated(files["geolocated"])
    handle_backlog(files["backlog"])
    handle_review(files["review"])

    to_tag = [
        os.path.join(JPEG_HD, file)
        for file in os.listdir(JPEG_HD)
        if not os.path.exists(os.path.join(JPEG_HD, f"{file}_original"))
        and not file.endswith("_original")
    ]
    # print("to_tag", type(to_tag))
    if to_tag:
        context.log.info(
            f"Passed {len(to_tag)} images to be tagged. Path example: {to_tag[0]}"
        )
    return to_tag


@dg.solid(
    config_schema=dg.StringSource,
    output_defs=[
        dg.OutputDefinition(
            io_manager_key="pandas_csv", name="images", dagster_type=pd.DataFrame
        )
    ],
)
def create_images_df(context, files: dict):

    """
    Creates a dataframe with every image available and links to full size and thumbnail
    """

    prefix = context.solid_config
    dicts = []

    for img in files["geolocated"]:
        img_dict = {
            "Source ID": img.id,
            "Media URL": os.path.join(prefix, img.id, "full","max","0","default.jpg"),
        }
        dicts.append(img_dict)

    for img in files["backlog"]:
        img_dict = {
            "Source ID": img.id,
            "Media URL": np.nan,
        }
        dicts.append(img_dict)

    images_df = pd.DataFrame(data=dicts)
    images_df.drop_duplicates(inplace=True)
    images_df.sort_values(by="Source ID")
    context.log.info(f"{len(images_df)} images available in hi-res")

    return images_df.set_index("Source ID")


@dg.solid(
    config_schema=dg.StringSource,
    input_defs=[dg.InputDefinition("metadata", root_manager_key="metadata_root")],
)
def write_metadata(context, metadata: dp.DataFrame, to_tag):
    """
    Write available metadata, including GPS tags,
    to high-res JPEGs
    """
    metadata.fillna(value="", inplace=True)
    metadata["Source ID"] = metadata["Source ID"].str.upper()
    metadata.set_index("Source ID", inplace=True)

    for item in tqdm(to_tag, "Embedding metadata in files..."):
        if item.endswith(".jpg"):
            basename = os.path.split(item)[1]
            name = basename.split(".")[0]
            date = metadata.loc[name.upper(), "Date"]
            byline = metadata.loc[name.upper(), "Creator"]
            headline = metadata.loc[name.upper(), "Title"]
            caption = metadata.loc[name.upper(), "Description (Portuguese)"]
            objecttype = metadata.loc[name.upper(), "Type"]
            # dimensions = f'{metadata.loc[name.upper(), "image_width"]}cm x {metadata.loc[name.upper(), "image_height"]}cm'
            keywords = metadata.loc[name.upper(), "Depicts"].split("||")
            latitude = metadata.loc[name.upper(), "Latitude"]
            longitude = metadata.loc[name.upper(), "Longitude"]
            # altitude = metadata.loc[name.upper(), "Altitude"]
            # imgdirection = metadata.loc[name.upper(), "heading"]

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
                f"-IPTC:DateCreated={date}",
                f"-IPTC:By-line={byline}",
                f"-IPTC:ObjectName={name}",
                f"-IPTC:Headline={headline}",
                f"-IPTC:Caption-Abstract={caption}",
                f"-IPTC:ObjectTypeReference={objecttype}",
                # f"-IPTC:Dimensions={dimensions}",
                f"-IPTC:Keywords={keywords}",
                f"-GPSLatitude={latitude}",
                f"-GPSLongitude={longitude}",
                # f"-GPSAltitude={altitude}",
                # f"-GPSImgDirection={imgdirection}",
            ]
            with ExifTool(executable_=context.solid_config) as et:
                for param in params:
                    param = param.encode(encoding="utf-8")
                    dest = item.encode(encoding="utf-8")
                    et.execute(param, dest)
    to_upload = to_tag
    return to_upload


@dg.solid
def upload_to_cloud(context, to_upload):

    S3 = boto3.client("s3")
    BUCKET = "imaginerio-images"
    ims = "/mnt/d/imagineRio-images/jpeg-hd"
    jstor = "/mnt/d/imagineRio-images/JSTOR-images"
    to_upload = [os.path.join(ims, file) for file in os.listdir(ims) if file.endswith(".jpg")]
    to_upload.append([os.path.join(jstor, file) for file in os.listdir(jstor) if file.endswith(".jpg")])
    #print(to_upload)
    for image_path in tqdm(to_upload, "Uploading files..."):
        id = os.path.basename(image_path).split(".")[0]
        key_name = "iiif/{0}/full/max/0/default.jpg".format(id)
        try:
            S3.upload_file(
                image_path,
                BUCKET,
                key_name,
                ExtraArgs={"ACL": "public-read", "ContentType": "image/jpeg"},
            )
        except:
            print("Couldn't upload image {0}, skipping".format(id))
