import math
import os
import re
import shutil
import sys

import dagster as dg
import geojson
import mercantile
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from PIL import Image
from pykml import parser
from pyproj import Proj
from shapely.geometry import Point
from SPARQLWrapper import JSON, SPARQLWrapper
from tests.dataframe_types import *
from tests.objects_types import *
from turfpy.misc import sector
from tqdm import tqdm
load_dotenv(override=True)


def find_with_re(property, kml):
    """
    Utility function to find KML properties using regex
    """
    return re.search(f"(?<=<{property}>).+(?=<\/{property}>)", kml).group(0)


def reproject(coordinates, inverse=False):
    """
    Transform Rio's geographic to world
    coordinates or vice-versa with inverse=True
    """
    rj = Proj("EPSG:32722")
    origin = Point(coordinates)
    origin_proj = rj(origin.x, origin.y, inverse=inverse)

    return Point(origin_proj)


def query_wikidata(Q):
    """
    Query Wikidata's SPARQL endpoint for entities' coordinates
    """
    endpoint_url = "https://query.wikidata.org/sparql"

    query = """SELECT ?coordinate
        WHERE
        {
        wd:%s wdt:P625 ?coordinate .
        }""" % (
        Q
    )

    def get_results(endpoint_url, query):
        user_agent = "WDQS-example Python/%s.%s" % (
            sys.version_info[0],
            sys.version_info[1],
        )
        # TODO adjust user agent; see https://w.wiki/CX6
        sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        return sparql.query().convert()

    results = get_results(endpoint_url, query)
    result_list = []
    for result in results["results"]["bindings"]:
        if result:
            result_list.append(result["coordinate"]["value"])
    return result_list


def get_radius(kml):
    """
    Calculate viewcone radius using Wikidata depicts
    or trigonometry if not available
    """
    with open(kml, "r") as f:
        KML = parser.parse(f).getroot()

    id = str(KML.PhotoOverlay.name)
    tilt = KML.PhotoOverlay.Camera.tilt
    df = pd.read_csv(
        os.environ["METADATA"],
        index_col="Source ID",
    )
    depicts = df.loc[id, "Depicts"]
    if isinstance(depicts, str):
        depicts = depicts.split("||")
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
                lng = lnglat.group(1)
                lat = lnglat.group(2)
                depicted = reproject((float(lng), float(lat)))
                origin = reproject(
                    (
                        KML.PhotoOverlay.Camera.longitude,
                        KML.PhotoOverlay.Camera.latitude,
                    )
                )

                distance = origin.distance(depicted)
                distances.append(distance)

        if distances:
            radius = max(distances)

        else:

            return None
    else:
        if tilt <= 89:
            tan = math.tan((tilt * math.pi) / 180)
            radius = KML.PhotoOverlay.Camera.altitude * tan
            if radius < 400:

                return None
        else:

            return None

    return radius


def draw_feature(kml, properties, radius=0.2):
    """
    Use pre-obtained radius to draw circular sector
    (viewcone) for each image
    """
    with open(kml, "r") as f:
        KML = parser.parse(f).getroot()

    camera = KML.PhotoOverlay.Camera
    viewvolume = KML.PhotoOverlay.ViewVolume
    point = Point(camera.longitude, camera.latitude)
    center = geojson.Feature(geometry=point)
    start_angle = camera.heading - viewvolume.rightFov
    end_angle = camera.heading - viewvolume.leftFov

    if start_angle > end_angle:
        start_angle = start_angle - 360
    else:
        pass

    cone = sector(
        center,
        radius,
        start_angle,
        end_angle,
        options={"properties": properties, "steps": 200},
    )
    return cone


@dg.solid(
    config_schema=dg.StringSource,
    output_defs=[dg.OutputDefinition(dagster_type=type_list_of_kmls)],
)
def get_list(context):
    """
    List KML files to be processed
    """
    path = context.solid_config
    path_gitkeep = os.path.join(path, ".gitkeep")
    list_kmls = os.listdir(path)
    kmls = []
    for kml in list_kmls:
        full_path = os.path.join(path, kml)
        kmls.append(full_path)
    list_kmls = [x for x in kmls if x != path_gitkeep]

    return list_kmls


@dg.solid(
    config_schema={"new_single": dg.StringSource,
                   "processed_raw": dg.StringSource}
)
def split_photooverlays(context, kmls: type_list_of_kmls, delete_original=False):
    """
    Split <folder> KMLs into individual ones
    """
    path_new_single = context.solid_config["new_single"]
    path_processed_raw = context.solid_config["processed_raw"]
    splited_kmls = []
    photooverlays = ""

    for kml in tqdm(kmls, desc="KMLS"):
        splited_kmls.append(kml)
        with open(kml, "r") as f:
            txt = f.read()
            if re.search("<Folder>", txt):
                header = "\n".join(txt.split("\n")[:2])
                photooverlays = re.split(".(?=<PhotoOverlay>)", txt)[1:]
                photooverlays[-1] = re.sub("</Folder>\n</kml>",
                                           "", photooverlays[-1])

        for po in photooverlays:
            filename = find_with_re("name", po)
            with open(os.path.join(path_new_single, filename + ".kml"), "w") as k:
                k.write(f"{header}\n{po}</kml>")
        if delete_original:
            os.remove(os.path.abspath(kml))
        shutil.move(kml, path_processed_raw)


@dg.solid(
    config_schema=dg.StringSource,
    input_defs=[dg.InputDefinition(
        "cumulus", root_manager_key="cumulus_root")],
    output_defs=[dg.OutputDefinition(dagster_type=type_list_of_kmls)],
)
def rename_single(context, cumulus: main_dataframe_types):
    """
    Check for changes in identifiers and correct filename
    """
    path = context.solid_config
    path_gitkeep = os.path.join(path, ".gitkeep")
    kmls = [
        os.path.join(path, file)
        for file in os.listdir(path)
        if os.path.isfile(os.path.join(path, file))
    ]
    list_kmls = [x for x in kmls if x != path_gitkeep]

    for kml in list_kmls:
        with open(kml, "r") as f:
            txt = f.read()
            filename = find_with_re("name", txt)
            if filename not in cumulus["Source ID"]:
                loc = cumulus.loc[
                    cumulus["preliminary id"].str.contains(filename, na=False),
                    "Source ID",
                ]
                if not loc.empty:
                    new_filename = loc.item()
                    txt = re.sub("(?<=<name>).+(?=<\/name>)",
                                 new_filename, txt)
                    with open(os.path.join(path, new_filename + ".kml"), "w") as k:
                        k.write(txt)
                    context.log.info(f"Renamed: {filename} > {new_filename}")
                    if os.path.exists(os.path.join(path, filename + ".kml")):
                        os.remove(os.path.join(path, filename + ".kml"))
                else:
                    context.log.info(f"Not renamed: {filename}")

    new_kmls = [
        os.path.join(path, file)
        for file in tqdm(os.listdir(path), desc="KMLS")
        if os.path.isfile(os.path.join(path, file))
    ]
    list_kmls = [x for x in new_kmls if x != path_gitkeep]

    return list_kmls


@dg.solid(output_defs=[dg.OutputDefinition(dagster_type=type_list_of_kmls)])
def change_img_href(context, list_kmls: type_list_of_kmls):
    """
    Substitute local image file link used for
    geolocating by public IIIF Image API
    """
    for kml in tqdm(list_kmls, desc="KMLS"):
        with open(kml, "r+") as f:
            txt = f.read()
            filename = find_with_re("name", txt)
            txt = re.sub(
                "(?<=<href>).+(?=<\/href>\n\t+<\/Icon>\n\t+<ViewVolume>)",
                f"https://images.imaginerio.org/iiif-img/{filename}/full/^1200,/0/default.jpg",
                txt,
            )
            f.seek(0)
            f.write(txt)
            f.truncate()

    return list_kmls


@dg.solid(output_defs=[dg.OutputDefinition(dagster_type=type_list_of_kmls)])
def correct_altitude_mode(context, kmls: type_list_of_kmls):
    """
    Check for KMLs with altitude relative to ground,
    query Mapbox-terrain-rgb and change for relative
    to sea level (absolute)
    """
    for kml in tqdm(kmls, desc="KMLS"):
        with open(kml, "r+") as f:
            txt = f.read()
            if re.search("(?<=altitudeMode>)relative(.+)?(?=\/altitudeMode>)", txt):
                lat = round(float(find_with_re("latitude", txt)), 5)
                lng = round(float(find_with_re("longitude", txt)), 5)
                alt = round(float(find_with_re("altitude", txt)), 5)
                z = 15
                tile = mercantile.tile(lng, lat, z)
                westmost, southmost, eastmost, northmost = mercantile.bounds(
                    tile)
                pixel_column = np.interp(lng, [westmost, eastmost], [0, 256])
                pixel_row = np.interp(lat, [southmost, northmost], [256, 0])
                tile_img = Image.open(
                    requests.get(
                        "https://api.mapbox.com/v4/mapbox.terrain-rgb/10/800/200.pngraw?access_token=pk.eyJ1IjoibWFydGltcGFzc29zIiwiYSI6ImNra3pmN2QxajBiYWUycW55N3E1dG1tcTEifQ.JFKSI85oP7M2gbeUTaUfQQ",
                        stream=True,
                    ).raw
                ).load()

                R, G, B, _ = tile_img[int(pixel_row), int(pixel_column)]
                height = -10000 + ((R * 256 * 256 + G * 256 + B) * 0.1)
                new_height = height + alt
                txt = re.sub(
                    "(?<=<altitudeMode>).+(?=<\/altitudeMode>)", "absolute", txt
                )
                txt = re.sub("(?<=<altitude>).+(?=<\/altitude>)",
                             f"{new_height}", txt)
                txt = re.sub(
                    "(?<=<coordinates>).+(?=<\/coordinates>)",
                    f"{lng},{lat},{new_height}",
                    txt,
                )

                f.seek(0)
                f.write(txt)
                f.truncate()
            else:
                continue
    return kmls


@dg.solid(
    input_defs=[dg.InputDefinition(
        "metadata", root_manager_key="metadata_root")],
    output_defs=[dg.OutputDefinition(dagster_type=type_list_of_features)],
)
def create_feature(
    context, kmls: type_list_of_kmls, metadata: metadata_dataframe_types
):
    """
    Build GEOJSON Feature with metadata and viewcone polygon
    """
    new_features = []
    processed_ids = []
    metadata["upper_ids"] = metadata["Source ID"].str.upper()
    metadata = metadata.set_index("upper_ids")
    # Id = ""

    for kml in tqdm(kmls, desc="KMLS"):
        try:
            with open(kml, "r") as f:
                KML = parser.parse(f).getroot()
                Id = (str(KML.PhotoOverlay.name)).upper()
                properties = {
                    "Source ID": metadata.loc[Id, "Source ID"],
                    "Title": ""
                    if pd.isna(metadata.loc[Id, "Title"])
                    else str(metadata.loc[Id, "Title"]),
                    "Date": ""
                    if pd.isna(metadata.loc[Id, "Date"])
                    else str(metadata.loc[Id, "Date"]),
                    "Description (Portuguese)": ""
                    if pd.isna(metadata.loc[Id, "Description (Portuguese)"])
                    else str(metadata.loc[Id, "Description (Portuguese)"]),
                    "Creator": ""
                    if pd.isna(metadata.loc[Id, "Creator"])
                    else str(metadata.loc[Id, "Creator"]),
                    "First Year": ""
                    if pd.isna(metadata.loc[Id, "First Year"])
                    else str(int(metadata.loc[Id, "First Year"])),
                    "Last Year": ""
                    if pd.isna(metadata.loc[Id, "Last Year"])
                    else str(int(metadata.loc[Id, "Last Year"])),
                    "Source": "Instituto Moreira Salles",
                    "Longitude": str(
                        round(float(KML.PhotoOverlay.Camera.longitude), 5)
                    ),
                    "Latitude": str(round(float(KML.PhotoOverlay.Camera.latitude), 5)),
                    "altitude": str(round(float(KML.PhotoOverlay.Camera.altitude), 5)),
                    "heading": str(round(float(KML.PhotoOverlay.Camera.heading), 5)),
                    "tilt": str(round(float(KML.PhotoOverlay.Camera.tilt), 5)),
                    "fov": str(
                        abs(float(KML.PhotoOverlay.ViewVolume.leftFov))
                        + abs(float(KML.PhotoOverlay.ViewVolume.rightFov))
                    ),
                }

                radius = get_radius(kml)
                context.log.info(f"OK: {Id}")
                if radius:
                    feature = draw_feature(
                        kml, radius=radius / 1000, properties=properties
                    )
                else:
                    feature = draw_feature(kml, properties=properties)
                new_features.append(feature)
                processed_ids.append(Id)

        except Exception as E:
            context.log.info(f"ERROR: {E} no ID: {Id}")

    return new_features


@dg.solid(
    config_schema={"new_single": dg.StringSource,
                   "processed_single": dg.StringSource}
)
def move_files(context, new_features: type_list_of_features):
    """
    Manage processed files
    """

    path_new_single = context.solid_config["new_single"]
    path_processed_single = context.solid_config["processed_single"]
    list_kmls = [feature["properties"]["Source ID"]
                 for feature in new_features]

    for kml in tqdm(list_kmls, desc="KMLS"):
        try:
            kml_from = os.path.join(path_new_single, kml + ".kml")
            kml_to = os.path.join(path_processed_single, kml + ".kml")
            if os.path.exists(kml_to):
                os.remove(os.path.abspath(kml_to))
                shutil.move(kml_from, path_processed_single)
            else:
                shutil.move(kml_from, path_processed_single)

        except Exception as e:
            context.log.info(e)

    return list


@dg.solid(
    config_schema=dg.StringSource,
    output_defs=[
        dg.OutputDefinition(
            io_manager_key="geojson", name="import_viewcones", dagster_type=type_geojson
        )
    ],
)
def create_geojson(context, new_features: type_list_of_features):
    """
    Build GEOJSON from FeatureCollection
    """
    camera = context.solid_config

    if new_features:
        if os.path.isfile(camera):
            current_features = (geojson.load(open(camera))).features
            current_ids = [
                feature["properties"]["Source ID"] for feature in current_features
            ]

            for new_feature in tqdm(new_features, desc="FEATURES"):
                id_new = new_feature["properties"]["Source ID"]

                if id_new in current_ids:
                    # context.log.info(id_new)
                    print("New: " + id_new)
                    index = current_ids.index(id_new)
                    current_features[index] = new_feature

                else:
                    print("Appended: " + id_new)
                    # context.log.info("Appended: ", id_new)
                    current_features.append(new_feature)

            feature_collection = geojson.FeatureCollection(
                features=current_features)
            return feature_collection

        else:
            feature_collection = geojson.FeatureCollection(
                features=new_features)
            return feature_collection
    else:
        context.log.info("Nothing's to updated on import_viewcones")
        pass
