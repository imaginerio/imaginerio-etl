import os
import re
import shutil
from dagster.builtins import Nothing
import dagster_pandas as dp

import boto3
from dagster import op, In, Out, DynamicOut, DynamicOutput
import pandas as pd
import numpy as np
from tqdm import tqdm
from numpy import nan
from dotenv import load_dotenv
from PIL import Image as PILImage
from tests.dataframe_types import *
from tests.objects_types import *
from ops.helpers import file_exists

from ops.exiftool import ExifTool

load_dotenv(override=True)

PILImage.MAX_IMAGE_PIXELS = None


class Tif(object):
    def copy(self, image):
        print("Copying {} to {}".format(image.id, os.environ["TIF"]))
        shutil.copy2(image.original_path, os.environ["TIF"])


class Highres(object):
    def copy(self, image):
        origin = os.path.join(os.environ["TIF"], image.tif)
        destination = os.path.join(os.environ["JPG"], image.jpg)
        print("Copying {} to {}".format(origin, destination))
        try:
            with PILImage.open(origin) as im:
                im.save(
                    destination,
                    "jpeg",
                    quality=95,
                    icc_profile=im.info.get("icc_profile"),
                    # exif=im.info["exif"],
                )
        except OSError as e:
            print(f"Cannot convert {image.tif}")


class Lowres(object):
    def copy(self, image):
        origin = os.path.join(os.environ["TIF"], image.tif)
        if image.to_review:
            destination = os.path.join(os.environ["REVIEW"], image.jpg)
        else:
            destination = os.path.join(os.environ["BACKLOG"], image.jpg)
        print("Copying {} to {}".format(origin, destination))
        try:
            with PILImage.open(origin) as im:
                im.thumbnail((1000, 1000))
                im.save(destination)
        except OSError:
            print(f"Cannot convert {image.tif}")


class Image:
    def __init__(self, original_path, has_kml, catalog):
        self.__original_path = original_path
        self.__id = os.path.split(self.__original_path)[1].split(".")[0]
        self.__jpg = self.__id + ".jpg"
        self.__tif = self.__id + ".tif"
        self.__is_geolocated = self.__id in has_kml
        self.__in_catalog = self.__id in catalog
        self.__metadata = None
        self.__on_cloud = file_exists(self.__id, "image")

    @property
    def original_path(self):
        return self.__original_path

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

    @property
    def in_catalog(self):
        return self.__in_catalog

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
        return self.__on_cloud

    def copy_strategy(self, strategy):

        strategy.copy(self)

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
        item = metadata.loc[self.__id].copy()
        item.fillna(value="", inplace=True)
        self.__metadata = [
            param.encode(encoding="utf-8")
            for param in [
                "-xmp:artworkorobject={{aosource=Instituto Moreira Salles,aocopyrightnotice=Public Domain,aocreator={0},aosourceinvno={1},aosourceinvurl={2},aotitle={3},aocontentdescription={4},aophysicaldescription={5}}}".format(
                    item["Creator"],
                    self.__id,
                    item["Document URL"],
                    item["Title"],
                    item["Description (Portuguese)"],
                    item["Materials"],
                ),
                "-iptc:city=Rio de Janeiro",
                "-iptc:province-State=RJ",
                "-iptc:country-primarylocationname=Brasil",
                "-iptc:keywords={}".format(",".join(item["Depicts"].split("||"))),
                "-exif:gpslatitude={}".format(item["Latitude"]),
                "-exif:gpslongitude={}".format(item["Longitude"]),
                "-exif:gpslatituderef=S",
                "-exif:gpslongituderef=W",
                "-exif:gpsaltituderef=0",
                "-exif:gpsimgdirectionref=T",
                # "-IPTC:Dimensions={}x{}mm".format(item["Width"], item["Height"]),
                # "-GPSAltitude={}".format(item["Altitude"]),
                # "-GPSImgDirection={}".format(item["Bearing"]),
            ]
        ]
        if "circa" in item["Date"]:
            self.__metadata.append(
                "-xmp:AOCircaDateCreated={}".format(item["Date"]).encode(
                    encoding="utf-8"
                )
            ),
        else:
            self.__metadata.append(
                "-xmp:AODateCreated={}".format(item["Date"]).encode(encoding="utf-8"),
            )

    def embed_metadata(self):
        with ExifTool() as et:
            et.execute(
                *self.__metadata,
                os.path.join(os.environ["JPG"], self.__jpg).encode(encoding="utf-8"),
            )

    def upload_to_cloud(self):
        pass


@op(
    config_schema=dg.StringSource,
    ins={"metadata": In(root_manager_key="metadata_root")},
    # out=Out(dagster_type=list),
)
def file_picker(context, metadata: dp.DataFrame):
    """
    Walks directory tree and glob relevant files,
    instantiating Image objects for each
    """

    source = context.solid_config

    # metadata["Document ID"] = metadata["Document ID"].str.upper()
    has_kml = list(metadata.loc[metadata["Latitude"].notna(), "Document ID"])
    catalog = list(metadata["Document ID"])
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

    for image in tqdm(images, desc="Copying images..."):

        if image.to_tif:
            image.copy_strategy(Tif())

        if image.to_jpg:
            image.copy_strategy(Highres())

        if image.to_backlog or image.to_review:
            image.copy_strategy(Lowres())

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

    id = [img.id for img in images]
    url = [
        os.path.join(prefix, img.id, "full", "max", "0", "default.jpg")
        if img.is_geolocated
        else np.nan
        for img in images
    ]
    images_df = pd.DataFrame(url, index=id)
    images_df.drop_duplicates(inplace=True)
    images_df.sort_index(inplace=True)
    context.log.info(f"{len(images_df)} images available in hi-res")
    print(images_df)

    return images_df


@op(
    config_schema=dg.StringSource,
    ins={"metadata": In(root_manager_key="metadata_root")},
)
def embed_metadata(context, metadata: dp.DataFrame, images):
    """
    Write available metadata, including GPS tags,
    to high-res JPGs
    """

    metadata.set_index("Document ID", inplace=True)

    for image in tqdm(images, desc="Embedding metadata..."):
        if image.is_geolocated and not image.has_embedded_metadata:
            image.metadata = metadata
            try:
                image.embed_metadata()
            except Exception as e:
                print(e)
    return images


@op(
    out=DynamicOut(io_manager_key="s3_manager"),
)
def upload_to_cloud(images: list):
    """
    Uploads finished JPGs to AWS S3
    """

    for image in images:
        if not image.on_cloud:
            path = os.path.join(os.environ["JPG"], image.jpg)
            yield DynamicOutput(value=path, mapping_key=image.id.replace("-", "_"))

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
