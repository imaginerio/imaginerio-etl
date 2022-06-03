import argparse
import logging
import logging.config
import os
import re
import shutil

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from numpy import nan
from PIL import Image as PILImage
from tqdm import tqdm

from exiftool import ExifTool
from helpers import file_exists, upload_file_to_s3

load_dotenv(override=True)

PILImage.MAX_IMAGE_PIXELS = None

logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": True,
    }
)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class Tif(object):
    def copy(self, image):
        logger.debug("Copying {} to {}".format(image.id, os.environ["TIF"]))
        shutil.copy2(image.original_path, os.environ["TIF"])


class Highres(object):
    def copy(self, image):
        origin = os.path.join(os.environ["TIF"], image.tif)
        destination = os.path.join(os.environ["JPG"], image.jpg)
        logger.debug("Copying {} to {}".format(origin, destination))
        try:
            with PILImage.open(origin) as im:
                im.save(
                    destination,
                    "jpeg",
                    quality=95,
                    icc_profile=im.info.get("icc_profile"),
                )
        except OSError as e:
            logger.debug(f"Cannot convert {image.tif}")


class Lowres(object):
    def copy(self, image):
        origin = os.path.join(os.environ["TIF"], image.tif)
        if image.to_review:
            destination = os.path.join(os.environ["REVIEW"], image.jpg)
        else:
            destination = os.path.join(os.environ["BACKLOG"], image.jpg)
        logger.debug("Copying {} to {}".format(origin, destination))
        try:
            with PILImage.open(origin) as im:
                im.thumbnail((1000, 1000))
                im.save(destination)
        except OSError:
            logger.debug(f"Cannot convert {image.tif}")


class Image:
    def __init__(self, original_path, metadata):
        self.__original_path = original_path
        self.__id = os.path.split(self.__original_path)[1].split(".")[0]
        self.__jpg = self.__id + ".jpg"
        self.__tif = self.__id + ".tif" 
        self.__in_catalog = self.__id in metadata.index
        if self.__in_catalog:
            self.__is_geolocated = pd.notna(metadata.loc[self.__id, "Latitude"])
        else:
            self.__is_geolocated = False
        self.__to_tif = not os.path.exists(os.path.join(os.environ["TIF"], self.__tif))
        self.__to_jpg = not os.path.exists(os.path.join(os.environ["JPG"], self.__jpg)) and self.__is_geolocated
        self.__to_backlog = (
            not os.path.exists(os.path.join(os.environ["BACKLOG"], self.__jpg))
            and not self.__is_geolocated
        )
        self.__to_review = (
            not os.path.exists(os.path.join(os.environ["REVIEW"], self.__jpg))
            and not self.__in_catalog
        )
        self.__metadata = None
        #self.get_metadata(metadata)

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
        return self.__to_tif

    @property
    def to_jpg(self):
        return self.__to_jpg
        
    @property
    def to_backlog(self):
        return self.__to_backlog

    @property
    def to_review(self):
        return self.__to_review

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

    # @metadata.setter
    def get_metadata(self, metadata):
        try:
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
                    "-xmp:AODateCreated={}".format(item["Date"]).encode(
                        encoding="utf-8"
                    ),
                )
        except KeyError:
            self.__metadata = None

    def embed_metadata(self):
        with ExifTool() as et:
            et.execute(
                *self.__metadata,
                os.path.join(os.environ["JPG"], self.__jpg).encode(encoding="utf-8"),
            )
        logger.debug(f"Embedded metadata in {image.id}")

    def upload_to_cloud(self):
        pass

    def is_on_cloud(self):
        return file_exists(self.__id, "image")

    def __str__(self):
        of_interest = [self.__in_catalog, self.__is_geolocated, self.__to_tif, self.__to_jpg, self.__to_review, self.__to_backlog]
        return "\n".join([f"{k}: {v}" for k, v in self.__dict__.items() if k in of_interest])
        #k, getattr(self, k) for k in self.attrs
        #return f"Item {self.id}:\nIs in catalog: {self.__in_catalog}\nIs geolocated: {self.__is_geolocated}\n"

def get_images(metadata):
    """
    Walks directory tree and glob relevant files,
    instantiating Image objects for each
    """

    source = os.environ["SOURCE"]

    images = [
        Image(
            os.path.join(root, name),
            metadata,
        )
        for root, _, files in os.walk(source)
        for name in files
        if "FINALIZADAS" in root
        and name.endswith((".tif"))
        and not re.search("[av]\.tif$", name)
    ]

    logger.debug(f"Listed {len(images)} images to process")
    return images


def dispatch(image):
    """
    Copy failsafe TIFs, convert geolocated images
    to JPG, and separate files for review and backlog
    """

    TIF = os.environ["TIF"]
    JPG = os.environ["JPG"]
    BACKLOG = os.environ["BACKLOG"]
    REVIEW = os.environ["REVIEW"]

    if image.to_tif:
        image.copy_strategy(Tif())

    if image.to_jpg:
        image.copy_strategy(Highres())

    if image.to_backlog or image.to_review:
        image.copy_strategy(Lowres())

    logger.debug(f"Dispatched image {image.id}")
    return image


def create_images_df(images):
    """
    Creates a dataframe with every image available and links to full size and thumbnail
    """

    prefix = os.environ["BUCKET"]

    id = [img.id for img in images]
    url = [
        os.path.join(prefix, img.id, "full", "max", "0", "default.jpg")
        if img.is_geolocated
        else np.nan
        for img in images
    ]
    images_df = pd.DataFrame(url, index=id)
    # images_df.loc[images_df.duplicated()].to_csv("data/output/duplicated_images.csv")
    images_df.drop_duplicates(inplace=True)
    images_df.sort_index(inplace=True)

    logger.debug(f"{len(images_df)} images available in hi-res")

    return images_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", "-m", help="run mode", choices=["test", "prod"], default="test"
    )
    args = parser.parse_args()

    metadata = pd.read_csv("data/output/metadata.csv", index_col="Document ID")

    images = get_images(metadata)
    create_images_df(images)

    for image in tqdm(images, desc="Handling images"):
        tqdm.write(str(image))
        dispatch(image)
        if image.is_geolocated and not image.has_embedded_metadata:
            image.get_metadata(metadata)
            image.embed_metadata()
    for image in tqdm(images, desc="Uploading images"):
        if not file_exists(image.id, "image"):
            upload_file_to_s3(
                os.path.join(os.environ["JPG"], image.jpg),
                target="iiif/{0}/full/max/0/default.jpg".format(image.id),
                mode=args.mode,
            )
