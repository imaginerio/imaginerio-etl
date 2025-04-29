import os
import shutil
import sys

from exiftool import ExifTool
from PIL import Image as PILImage

from ..config import *
from ..utils.helpers import logger

PILImage.MAX_IMAGE_PIXELS = None


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
            destination = os.path.join(REVIEW, image.jpg)
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
        self.__to_tif = not os.path.exists(os.path.join(os.environ["TIF"], self.__tif))
        self.__to_jpg = (
            not os.path.exists(os.path.join(os.environ["JPG"], self.__jpg))
            and self.__in_catalog
        )
        self.__metadata = None
        if not self.has_embedded_metadata:
            self.get_metadata(metadata)

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
    def in_catalog(self):
        return self.__in_catalog

    @property
    def to_tif(self):
        return self.__to_tif

    @property
    def to_jpg(self):
        return self.__to_jpg

    @property
    def to_review(self):
        return self.__to_review

    @property
    def on_cloud(self):
        return self.__on_cloud

    @property
    def has_embedded_metadata(self):
        return os.path.exists(
            os.path.join(os.environ["JPG"], "{}_original".format(self.__jpg))
        )

    @property
    def metadata(self):
        return self.__metadata

    def copy_strategy(self, strategy):
        strategy.copy(self)

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
                        item["Material"],
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
            logger.debug(f"{self.id} metadata unavailable")

    def embed_metadata(self):
        if self.__metadata:
            with ExifTool() as et:
                et.execute(
                    *self.__metadata,
                    os.path.join(os.environ["JPG"], self.__jpg).encode(
                        encoding="utf-8"
                    ),
                )
            logger.debug(f"Embedded metadata in {self.id}")
        else:
            logger.debug(f"Skipping {self.id} metadata embedding")

    def __str__(self):
        of_interest = [
            self.__in_catalog,
            self.__to_tif,
            self.__to_jpg,
        ]
        return "\n".join(
            [f"{k}: {v}" for k, v in self.__dict__.items() if k in of_interest]
        )
