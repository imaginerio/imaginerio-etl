import math
from operator import index
import os
import re
import shutil
import sys

import geojson
import mercantile
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from lxml import etree
from PIL import Image
from pykml import parser
from pyproj import Proj
from shapely.geometry import Point
from SPARQLWrapper import JSON, SPARQLWrapper
from turfpy.misc import sector
from tqdm import tqdm

from helpers import query_wikidata, geo_to_world_coors

load_dotenv(override=True)

CAMERA = os.environ["CAMERA"]


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
        try:
            self._altitude_mode = element.findtext(
                "kml:Camera/kml:altitudeMode",
                namespaces={"kml": "http://www.opengis.net/kml/2.2"},
            )
        except:
            self._altitude_mode = element.findtext(
                "kml:Camera/gx:altitudeMode",
                namespaces={
                    "kml": "http://www.opengis.net/kml/2.2",
                    "gx": "http://www.google.com/kml/ext/2.2",
                },
            )
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
        try:
            self._depicts = catalog.loc[self._id, "Depicts"]
            self._properties = {
                "Document ID": str(self._id),
                "Title": ""
                if pd.isna(catalog.loc[self._id, "Title"])
                else str(catalog.loc[self._id, "Title"]),
                "Date": ""
                if pd.isna(catalog.loc[self._id, "Date"])
                else str(catalog.loc[self._id, "Date"]),
                "Description (Portuguese)": ""
                if pd.isna(catalog.loc[self._id, "Description (Portuguese)"])
                else str(catalog.loc[self._id, "Description (Portuguese)"]),
                "Creator": ""
                if pd.isna(catalog.loc[self._id, "Creator"])
                else str(catalog.loc[self._id, "Creator"]),
                "First Year": ""
                if pd.isna(catalog.loc[self._id, "First Year"])
                else str(int(catalog.loc[self._id, "First Year"])),
                "Last Year": ""
                if pd.isna(catalog.loc[self._id, "Last Year"])
                else str(int(catalog.loc[self._id, "Last Year"])),
                "Provider": str(catalog.loc[self._id, "Provider"]),
                "Longitude": str(round(self._longitude, 5)),
                "Latitude": str(round(self._latitude, 5)),
                "Altitude": str(round(self._altitude, 5)),
                "Heading": str(round(self._heading, 5)),
                "Tilt": str(round(self._tilt, 5)),
                "FOV": str(abs(self._left_fov) + abs(self._right_fov)),
            }
        except KeyError:
            self._depicts = None
            self._properties = None

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
                self._radius = radius
        else:
            self._radius = None

    def get_radius_via_depicted_entities(self):
        """
        Calculate viewcone radius using Wikidata depicts
        """

        if isinstance(self._depicts, str):
            depicts = self._depicts.split("||")
            distances = []
            points = []
            for depict in depicts:
                q = re.search("(?<=\/)Q\d+", depict).group(0)
                point = query_wikidata(q)
                if point:
                    points.append(point[0])
                else:
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
                self._radius = max(distances)
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
            self._radius = 400

        feature = sector(
            center,
            round(self._radius),
            round(start_angle),
            round(end_angle),
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
        href[0].text = self._image
        altitude[0].text = str(self._altitude)
        altitude_mode[0].text = self._altitude_mode
        return self._element


if __name__ == "__main__":
    metadata = pd.read_csv(os.environ["METADATA"], index_col="Document ID")
    master_kml = "/content/master.kml"
    photo_overlays = []
    samples = [
        "data/input/kmls/2019-04-12-Davi.kml",
        "data/input/kmls/2019-05-21-Martim.kml",
    ]

    if os.path.exists(CAMERA):
        features = (geojson.load(open(CAMERA))).features
    else:
        features = []

    if os.path.exists(master_kml):
        master_folder = KML(master_kml)._folder
    else:
        master = KML.to_element()
        master_folder = etree.SubElement(master, "Folder")

    # Parse PhotoOverlays
    for sample in samples:
        if sample._folder is not None:
            folder = Folder(sample._folder)
            for child in folder._children:
                photo_overlays.append(PhotoOverlay(child, metadata))
        else:
            photo_overlays.append(PhotoOverlay(sample._photooverlay, metadata))

    # Manipulate PhotoOverlays
    for photo_overlay in tqdm(photo_overlays, desc="Manipulating PhotoOverlays"):
        print(photo_overlay._id)
        # photo_overlay.update_id(metadata)
        if "relative" in photo_overlay._altitude_mode:
            photo_overlay.correct_altitude_mode()
        if photo_overlay._depicts:
            photo_overlay.get_radius_via_depicted_entities()
        else:
            photo_overlay.get_radius_via_trigonometry()

        # Dispatch data
        features.append(photo_overlay.to_feature())
        # print(photo_overlay.to_element())
        master_folder.append(photo_overlay.to_element())

    feature_collection = geojson.FeatureCollection(features=features)
    # print(etree.tostring(master))
    etree.ElementTree(master).write("/content/master.kml", pretty_print=True)
    # print(feature_collection)
