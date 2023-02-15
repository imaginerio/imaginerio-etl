import sys

sys.path.insert(0, "/scripts")

import math
import os
import re
import shutil
import sys
from operator import index

import geojson
import mercantile
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from helpers import geo_to_world_coors, query_wikidata
from lxml import etree
from PIL import Image
from pykml import parser
from pyproj import Proj
from shapely.geometry import Point
from SPARQLWrapper import JSON, SPARQLWrapper
from tqdm import tqdm
from turfpy.misc import sector


class KML:

    header = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2" xmlns:gx="http://www.google.com/kml/ext/2.2" xmlns:kml="http://www.opengis.net/kml/2.2" xmlns:atom="http://www.w3.org/2005/Atom"></kml>
"""

    def __init__(self, path):
        try:
            self._tree = etree.parse(path)
        except OSError:
            self._tree = etree.fromstring(requests.get(path).content)
        self._folder = self._tree.find(
            "kml:Folder", namespaces={"kml": "http://www.opengis.net/kml/2.2"}
        )
        self._photooverlay = self._tree.find(
            "kml:PhotoOverlay", namespaces={"kml": "http://www.opengis.net/kml/2.2"}
        )

    @staticmethod
    def to_element():
        return etree.XML(KML.header)


class Folder:

    tag = "Folder"

    def __init__(self, element):
        self._id = element.findtext(
            "kml:name",
            namespaces={"kml": "http://www.opengis.net/kml/2.2"},
        )
        self._children = element.findall(
            "kml:PhotoOverlay", namespaces={"kml": "http://www.opengis.net/kml/2.2"}
        )


class PhotoOverlay:
    def __init__(self, element, catalog):
        self._element = element
        self._id = element.findtext(
            "kml:name",
            namespaces={"kml": "http://www.opengis.net/kml/2.2"},
        )
        if self._id.startswith("0"):
            try:
                self._ssid = catalog.index[catalog["Document ID"] == self._id].item()
            except ValueError:
                self._ssid = None
        else:
            self._ssid = self._id
        self._latitude = float(
            element.findtext(
                "kml:Camera/kml:latitude",
                namespaces={"kml": "http://www.opengis.net/kml/2.2"},
            )
        )
        self._longitude = float(
            element.findtext(
                "kml:Camera/kml:longitude",
                namespaces={"kml": "http://www.opengis.net/kml/2.2"},
            )
        )
        self._altitude = float(
            element.findtext(
                "kml:Camera/kml:altitude",
                namespaces={"kml": "http://www.opengis.net/kml/2.2"},
            )
        )
        # try:
        self._altitude_mode = element.findtext(
            "kml:Camera/kml:altitudeMode",
            default=element.findtext(
                "kml:Camera/gx:altitudeMode",
                namespaces={
                    "kml": "http://www.opengis.net/kml/2.2",
                    "gx": "http://www.google.com/kml/ext/2.2",
                },
            ),
            namespaces={"kml": "http://www.opengis.net/kml/2.2"},
        )
        # except:
        #     self._altitude_mode = element.findtext(
        #         "kml:Camera/gx:altitudeMode",
        #         namespaces={
        #             "kml": "http://www.opengis.net/kml/2.2",
        #             "gx": "http://www.google.com/kml/ext/2.2",
        #         },
        #     )
        self._heading = float(
            element.findtext(
                "kml:Camera/kml:heading",
                namespaces={"kml": "http://www.opengis.net/kml/2.2"},
            )
        )
        self._tilt = float(
            element.findtext(
                "kml:Camera/kml:tilt",
                namespaces={"kml": "http://www.opengis.net/kml/2.2"},
            )
        )
        self._left_fov = float(
            element.findtext(
                "kml:ViewVolume/kml:leftFov",
                namespaces={"kml": "http://www.opengis.net/kml/2.2"},
            )
        )
        self._right_fov = float(
            element.findtext(
                "kml:ViewVolume/kml:rightFov",
                namespaces={"kml": "http://www.opengis.net/kml/2.2"},
            )
        )
        self._image = "https://imaginerio-images.s3.us-east-1.amazonaws.com/iiif/{0}/full/max/0/default.jpg".format(
            self._id
        )
        self._radius = None
        self._viewcone = None

        def try_using_column(column):
            value = str(catalog.loc[self._ssid, column])
            if value:
                return value
            else:
                return ""

        # try:
        # property_columns = ["SSID", "Title", "Date", "Description (Portuguese)",
        # "Creator", "First Year", "Last Year", "Provider"]
        # self._properties = {column: try_using_column(column) for column in property_columns}
        try:
            row = catalog.loc[self._ssid]
        except KeyError:
            row = pd.Series(dtype="object")

        self._properties = {
            "document_id": str(self._id),
            "longitude": str(round(self._longitude, 5)),
            "latitude": str(round(self._latitude, 5)),
            "altitude": str(round(self._altitude, 5)),
            "heading": str(round(self._heading, 5)),
            "tilt": str(round(self._tilt, 5)),
            "fov": str(abs(self._left_fov) + abs(self._right_fov)),
        }

        self._depicts = None if row.empty else row["Depicts"]

        if row.any():
            self._properties["ss_id"] = (str(self._ssid),)
            self._properties["title"]: row["Title"]
            self._properties["date"]: row["Date"]
            self._properties["creator"]: row["Creator"]
            self._properties["firstyear"]: str(int(row["First Year"]))
            self._properties["lastyear"]: str(int(row["Last Year"]))

    @property
    def altitude(self):
        return self._altitude

    @altitude.setter
    def altitude(self, value):
        self._altitude = value

    @property
    def gx_altitude_mode(self):
        return self._gx_altitude_mode

    @property
    def altitude_mode(self):
        return self._altitude_mode

    @altitude_mode.setter
    def altitude_mode(self, value):
        self._altitude_mode = value

    def update_id(self, catalog):
        """
        Looks for the current ID in the past IDs field and
        updates it if necessary
        """
        if self._id not in catalog.index:
            loc = catalog.loc[
                catalog["preliminary id"].str.contains(self._id, na=False),
                "Document ID",
            ]
            if not loc.empty:
                new_id = loc.item()
                self._id = new_id

    def correct_altitude_mode(self):
        """
        Checks for relative altitudes, queries mapbox altitude API and
        corrects altitude value and mode to absolute
        """

        z = 15
        tile = mercantile.tile(self._longitude, self._latitude, z)
        westmost, southmost, eastmost, northmost = mercantile.bounds(tile)
        pixel_column = int(np.interp(self._longitude, [westmost, eastmost], [0, 256]))
        pixel_row = int(np.interp(self._latitude, [southmost, northmost], [256, 0]))
        tile_img = Image.open(
            requests.get(
                "https://api.mapbox.com/v4/mapbox.terrain-rgb/10/800/200.pngraw?access_token=pk.eyJ1IjoibWFydGltcGFzc29zIiwiYSI6ImNra3pmN2QxajBiYWUycW55N3E1dG1tcTEifQ.JFKSI85oP7M2gbeUTaUfQQ",
                stream=True,
            ).raw
        ).load()
        R, G, B, _ = tile_img[pixel_row, pixel_column]
        height = -10000 + ((R * 256 * 256 + G * 256 + B) * 0.1)
        self._altitude = height + self._altitude
        self._altitude_mode = "absolute"
        # return absolute_altitude

    def get_radius_via_trigonometry(self):
        """
        Calculate viewcone radius using trigonometry
        """

        if self._tilt <= 89:
            tan = math.tan((self._tilt * math.pi) / 180)
            radius = self._altitude * tan
            if radius < 400:
                self._radius = None
            else:
                self._radius = radius / 1000
        else:
            self._radius = None

    def get_radius_via_depicted_entities(self, vocabulary):
        """
        Calculate viewcone radius using Wikidata depicts
        """

        if isinstance(self._depicts, str):
            depicts = self._depicts.split("|")
            distances = []
            points = []
            for depict in depicts:
                try:
                    q = vocabulary.loc[depict, "Wikidata ID"]
                    point = query_wikidata(q)
                    if point:
                        points.append(point[0])
                    else:
                        continue
                except (KeyError, AttributeError):
                    continue
                for point in points:
                    lnglat = re.search("\((-\d+\.\d+) (-\d+\.\d+)\)", point)
                    lng = float(lnglat.group(1))
                    lat = float(lnglat.group(2))
                    depicted = geo_to_world_coors(coors=(lng, lat))
                    origin = geo_to_world_coors(coors=(self._longitude, self._latitude))
                    distance = origin.distance(depicted)
                    distances.append(distance)
            if distances:
                self._radius = max(distances) / 1000
            else:
                self._radius = None

    def to_feature(self):
        """
        Draws a viewcone and returns a geojson polygon with properties
        """

        point = Point(self._longitude, self._latitude)
        center = geojson.Feature(geometry=point)
        start_angle = self._heading + self._left_fov
        end_angle = self._heading + self._right_fov

        if start_angle > end_angle:
            start_angle = start_angle - 360

        if not self._radius:
            self._radius = 0.4

        feature = sector(
            center,
            self._radius,
            start_angle,
            end_angle,
            options={"properties": self._properties, "steps": 200},
        )
        return feature

    def to_element(self):
        # href = self._element.xpath('//PhotoOverlay/icon/href')
        # href[0].text = self._image
        href = self._element.xpath(
            "kml:Icon/kml:href", namespaces={"kml": "http://www.opengis.net/kml/2.2"}
        )
        altitude = self._element.xpath(
            "kml:Camera/kml:altitude",
            namespaces={"kml": "http://www.opengis.net/kml/2.2"},
        )
        altitude_mode = self._element.xpath(
            "kml:Camera/kml:altitudeMode",
            namespaces={"kml": "http://www.opengis.net/kml/2.2"},
        )
        if not altitude_mode:
            altitude_mode = self._element.xpath(
                "kml:Camera/gx:altitudeMode",
                namespaces={
                    "kml": "http://www.opengis.net/kml/2.2",
                    "gx": "http://www.google.com/kml/ext/2.2",
                },
            )
        href[0].text = self._image
        altitude[0].text = str(self._altitude)
        altitude_mode[0].text = self._altitude_mode
        return self._element
