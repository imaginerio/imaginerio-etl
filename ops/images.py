import os
import re
import shutil
from dagster.builtins import Nothing
import dagster_pandas as dp

import boto3
from dagster import op, In, Out
import pandas as pd
import numpy as np
from tqdm import tqdm
from numpy import nan
from dotenv import load_dotenv
from PIL import Image as PILImage
from tests.dataframe_types import *
from tests.objects_types import *

from ops.exiftool import ExifTool

load_dotenv(override=True)


class Image:
    def __init__(self, original_path, has_kml, catalog):
        self.__original_path = original_path
        self.__id = os.path.split(self.__original_path)[1].split(".")[0]
        self.__jpg = self.__id + ".jpg"
        self.__tif = self.__id + ".tif"
        self.__is_geolocated = self.__id in has_kml
        self.__in_catalog = self.__id in catalog
        self.__metadata = None
        self.__on_cloud = None

    @property
    def id(self):
        return self.__id

    @property
    def jpg(self):
        return self.__jpg

    @property
    def tif(self):
        return self.__tif

    @property
    def is_geolocated(self):
        return self.__is_geolocated

    # @is_geolocated.setter
    # def is_geolocated(self, has_kml):
    #    self.__is_geolocated = self.__id in has_kml

    @property
    def in_catalog(self):
        return self.__in_catalog

    # @in_catalog.setter
    # def in_catalog(self, catalog):
    #    self.__in_catalog = self.__id in catalog

    @property
    def to_tif(self):
        return not os.path.exists(os.path.join(os.environ["TIF"], self.__tif))

    @property
    def to_jpg(self):
        return (
            not os.path.exists(os.path.join(os.environ["JPG"], self.__jpg))
            and self.__is_geolocated
        )

    @property
    def to_backlog(self):
        return (
            not os.path.exists(os.path.join(os.environ["BACKLOG"], self.__jpg))
            and not self.__is_geolocated
            and self.__in_catalog
        )

    @property
    def to_review(self):
        return (
            not os.path.exists(os.path.join(os.environ["REVIEW"], self.__jpg))
            and not self.__in_catalog
        )

    @property
    def on_cloud(self):
        pass

    def copy_to(self, destination):
        if destination.endswith("tif"):
            print("Copying {} to {}".format(self.__id, destination))
            shutil.copy2(self.__original_path, destination)
        elif destination.endswith("backlog") or destination.endswith("review"):
            print("Copying {} to {}".format(self.__id, destination))
            try:
                with PILImage.open(os.path.join(os.environ["TIF"], self.__tif)) as im:
                    im.thumbnail((1000, 1000))
                    im.save(os.path.join(destination, self.__jpg))
            except OSError:
                print(f"Cannot convert {self.__tif}")
        else:
            print("Copying {} to {}".format(self.__id, destination))
            try:
                with PILImage.open(os.path.join(os.environ["TIF"], self.__tif)) as im:
                    im.save(os.path.join(destination, self.__jpg))
            except OSError:
                print(f"Cannot convert {self.__tif}")

    @property
    def has_embedded_metadata(self):
        return os.path.exists(
            os.path.join(os.environ["JPG"], "{}_original".format(self.__jpg))
        )

    @property
    def metadata(self):
        return self.__metadata

    @metadata.setter
    def metadata(self, metadata):
        metadata.fillna(value="", inplace=True)
        metadata["Source ID"] = metadata["Source ID"].str.upper()
        metadata.set_index("Source ID", inplace=True)
        item = metadata.loc[self.__id]
        self.__metadata = [
            "-IPTC:Source=Instituto Moreira Salles/IMS",
            "-IPTC:CopyrightNotice=This image is in the Public Domain.",
            "-IPTC:City=Rio de Janeiro",
            "-IPTC:Province-State=RJ",
            "-IPTC:Country-PrimaryLocationName=Brasil",
            "-GPSLatitudeRef=S",
            "-GPSLongitudeRef=W",
            "-GPSAltitudeRef=0",
            "-GPSImgDirectionRef=T",
            "-IPTC:DateCreated={}".format(item["Date"]),
            "-IPTC:By-line={}".format(item["Creator"]),
            "-IPTC:ObjectName={}".format(self.__id),
            "-IPTC:Headline={}".format(item["Title"]),
            "-IPTC:Caption-Abstract={}".format(item["Description (Portuguese)"]),
            "-IPTC:ObjectTypeReference={}".format(item["Type"]),
            # "-IPTC:Dimensions={}x{}mm".format(item["Width"], item["Height"]),
            "-IPTC:Keywords={}".format(",".join(item["Depicts"].split("||"))),
            "-GPSLatitude={}".format(item["Latitude"]),
            "-GPSLongitude={}".format(item["Longitude"]),
            # "-GPSAltitude={}".format(item["Altitude"]),
            # "-GPSImgDirection={}".format(item["Bearing"]),
        ]

    def embed_metadata(self):
        with ExifTool(executable_=os.environ["EXIFTOOL"]) as et:
            for param in self.__metadata:
                param = param.encode(encoding="utf-8")
                dest = os.path.join(os.environ["JPG"], self.__jpg).encode(
                    encoding="utf-8"
                )
                et.execute(param, dest)

    def upload_to_cloud(self):
        pass


@op(
    config_schema=dg.StringSource,
    ins={"metadata": In(root_manager_key="metadata_root")},
    out=Out(dagster_type=list),
)
def file_picker(context, metadata: dp.DataFrame):
    """
    Walks directory tree and glob relevant files,
    instantiating Image objects for each
    """

    source = context.solid_config

    metadata["Source ID"] = metadata["Source ID"].str.upper()
    has_kml = list(metadata.loc[metadata["Latitude"].notna(), "Source ID"])
    catalog = list(metadata["Source ID"])
    images = [
        Image(os.path.join(root, name), has_kml, catalog)
        for root, _, files in os.walk(source)
        for name in files
        if "FINALIZADAS" in root
        and name.endswith((".tif"))
        and not re.search("[av]\.tif$", name)
    ]

    return images


@op(
    config_schema={
        "tif": dg.StringSource,
        "jpg": dg.StringSource,
        "backlog": dg.StringSource,
        "review": dg.StringSource,
    }
)
def file_dispatcher(context, images: list):
    """
    Copy failsafe TIFs, convert geolocated images
    to JPG, and separate files for review and backlog
    """

    TIF = context.solid_config["tif"]
    JPG = context.solid_config["jpg"]
    BACKLOG = context.solid_config["backlog"]
    REVIEW = context.solid_config["review"]

    for image in images:

        if image.to_tif:
            image.copy_to(TIF)

        if image.to_jpg:
            image.copy_to(JPG)

        if image.to_backlog:
            image.copy_to(BACKLOG)

        if image.to_review:
            image.copy_to(REVIEW)

    return images


@op(
    config_schema=dg.StringSource,
    out={"images": Out(io_manager_key="pandas_csv", dagster_type=pd.DataFrame)},
)
def create_images_df(context, images: list):
    """
    Creates a dataframe with every image available and links to full size and thumbnail
    """

    prefix = context.solid_config
    dicts = []

    for img in images:
        img_dict = {
            "Source ID": img.id,
        }
        img_dict["Media URL"] = (
            os.path.join(prefix, img.id, "full", "max", "0", "default.jpg")
            if img.is_geolocated
            else np.nan
        )
        dicts.append(img_dict)

    images_df = pd.DataFrame(data=dicts)
    images_df.drop_duplicates(inplace=True)
    images_df.sort_values(by="Source ID")
    context.log.info(f"{len(images_df)} images available in hi-res")

    return images_df.set_index("Source ID")


@op(
    config_schema=dg.StringSource,
    ins={"metadata": In(root_manager_key="metadata_root")},
)
def embed_metadata(context, metadata: dp.DataFrame, images):
    """
    Write available metadata, including GPS tags,
    to high-res JPGs
    """

    for image in images:
        if image.is_geolocated and not image.has_embedded_metadata:
            image.get_metadata = metadata
            image.embed_metadata

    return images


@op
def upload_to_cloud(context, images: list):
    """
    Uploads finished JPGs to AWS S3
    """

    for image in images:
        if not image.on_cloud:
            image.upload_to_cloud

    # S3 = boto3.client("s3")
    # BUCKET = "imaginerio-images"
    # for image_path in tqdm(to_upload, "Uploading files..."):
    #     id = os.path.basename(image_path).split(".")[0]
    #     key_name = "iiif/{0}/full/max/0/default.jpg".format(id)
    #     try:
    #         S3.upload_file(
    #             image_path,
    #             BUCKET,
    #             key_name,
    #             ExtraArgs={"ACL": "public-read", "ContentType": "image/jpeg"},
    #         )
    #     except:
    #         print("Couldn't upload image {0}, skipping".format(id))
