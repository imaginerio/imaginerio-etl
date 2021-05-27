import math
import os
import re
import sys
from io import BytesIO

import dagster as dg
import geojson
import geopandas as gpd
import matplotlib._png as png
import mercantile
import numpy as np
import pandas as pd
import requests
from PIL import Image
from pykml import parser
from pyproj import Proj
from shapely.geometry import Point, Polygon, shape
from SPARQLWrapper import JSON, SPARQLWrapper


def find_with_re(property, kml):
    return re.search(f"(?<=<{property}>).+(?=<\/{property}>)", kml).group(0)

def reproject(coordinates):
    rj = Proj(init='EPSG:32722')
    origin = Point(coordinates)
    origin_proj = rj(origin.y, origin.x)
    return origin_proj

def query_wikidata(Q):
  endpoint_url = "https://query.wikidata.org/sparql"

  query = """SELECT ?coordinate
        WHERE
        {
        wd:%s wdt:P625 ?coordinate .
        }""" % (Q)


  def get_results(endpoint_url, query):
      user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
      # TODO adjust user agent; see https://w.wiki/CX6
      sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
      sparql.setQuery(query)
      sparql.setReturnFormat(JSON)
      return sparql.query().convert()


  results = get_results(endpoint_url, query)
  result_list = []
  for result in results["results"]["bindings"]:
      if result:
        result_list.append(result['coordinate']['value'])
  return result_list

def get_radius(kml):
    with open(kml, "r") as f:
      KML = parser.parse(f).getroot()
    id = str(KML.PhotoOverlay.name)
    tilt = KML.PhotoOverlay.Camera.tilt
    df = pd.read_csv("https://raw.githubusercontent.com/imaginerio/situated-views/dev/src/data-out/metadata.csv", index_col="id")
    depicts = df.loc["id", "wikidata_depict"]
    if depicts != np.nan:
      depicts = depicts.split("||")
      distances = []
      for depict in depicts:
        q = re.search("(?<=\/)Q\d+", depicts).group(0)
        point = query_wikidata(q)
        if point:
          lng = re.search("\((-\d+\.\d+) (-\d+\.\d+)\)", point).group(0)
          lat = re.search("\((-\d+\.\d+) (-\d+\.\d+)\)", point).group(1)
          depicted = reproject((lng, lat))
          origin = reproject((KML.PhotoOverlay.Camera.longitude, KML.PhotoOverlay.Camera.latitude))
          distance = origin.distance(depicted)
          distances.append(distance)
        else:
          continue
      if distances:
        radius = distances.max()
      else:
        return None
    else:
      if tilt <= 89:
        tan = math.tan((tilt * math.pi) / 180)
        radius = KML.PhotoOverlay.altitude * tan
        if radius < 400:
          return None
      else:
        return None
    return radius

def draw_cone(kml, radius=400, steps=200):

    with open(kml, "r") as f:
      KML = parser.parse(f).getroot()
      
    camera = KML.PhotoOverlay.Camera
    viewvolume = KML.PhotoOverlay.ViewVolume
    center = Point(reproject((camera.longitude, camera.latitude)))
    start_angle = camera.heading - viewvolume.rightFov
    end_angle = camera.heading  - viewvolume.leftFov

    def polar_point(origin_point, angle,  distance):
        return [origin_point.x + math.sin(math.radians(angle)) * distance, origin_point.y + math.cos(math.radians(angle)) * distance]

    if start_angle > end_angle:
        start_angle = start_angle - 360
    else:
        pass
    step_angle_width = (end_angle-start_angle) / steps
    sector_width = (end_angle-start_angle) 
    segment_vertices = []
    segment_vertices.append(polar_point(center, 0,0))
    segment_vertices.append(polar_point(center, start_angle,radius))
    for z in range(1, steps):
        segment_vertices.append((polar_point(center, start_angle + z * step_angle_width,radius)))
    segment_vertices.append(polar_point(center, start_angle+sector_width,radius))
    segment_vertices.append(polar_point(center, 0,0))

    return Polygon(segment_vertices)

@dg.solid
def get_list(context):
  path = context.solid_config
  list_kmls = os.listdir(path)
  kmls=[]
  for kml in list_kmls:
      full_path = os.path.join(path, kml)
      kmls.append(full_path)
  return kmls

@dg.solid
def split_photooverlays(context, kmls, delete_original=False):
  path = context.solid_config
  for kml in kmls:
    with open(kml, "r") as f:
        txt = f.read()
        if re.search("<Folder>", txt):
          header = "\n".join(txt.split("\n")[:2])
          photooverlays = re.split(".(?=<PhotoOverlay>)", txt)[1:]
          photooverlays[-1] = re.sub("</Folder>\n</kml>", "",photooverlays[-1])
    for po in photooverlays:
        filename = find_with_re("name", po)
        with open(os.path.join(os.path.dirname("kml_folder"), filename + ".kml"), "w") as k:
            k.write(f"{header}\n{po}</kml>")
    if delete_original:
        os.remove(os.path.abspath(kml))

  return [os.path.join(path, file) for file in os.listdir(path)]
  
@dg.solid
def change_img_href(context,kmls):
  for kml in kmls:
    with open(kml, "r+") as f:
        txt = f.read()
        filename = find_with_re("name", txt)
        txt = re.sub(
        "(?<=<href>).+(?=<\/href>\n\t+<\/Icon>\n\t+<ViewVolume>)",
        f"https://images.imaginerio.org/iiif-img/{filename}/full/^1200,/0/default.jpg",
        txt
        )
        f.seek(0)
        f.write(txt)
        f.truncate()
  return kmls

@dg.solid
def correct_altitude_mode(context,kmls):
    for kml in kmls:
      with open(kml, "r+") as f:
          txt = f.read()
          if re.search("(?<=<altitudeMode>)relative(.+)?(?=<\/altitudeMode>)", txt):
            lat = round(float(find_with_re("latitude", txt)), 5)
            lng = round(float(find_with_re("longitude", txt)), 5)
            alt = round(float(find_with_re("altitude", txt)), 5)
            z = 15
            tile = mercantile.tile(lng, lat, z)            
            westmost, southmost, eastmost, northmost = mercantile.bounds(tile)
            pixel_column = np.interp(lng, [westmost, eastmost], [0,256])
            pixel_row = np.interp(lat, [southmost, northmost], [256, 0])
            response = requests.get(f"https://api.mapbox.com/v4/mapbox.terrain-rgb/{z}/{tile.x}/{tile.y}.pngraw?access_token=pk.eyJ1IjoibWFydGltcGFzc29zIiwiYSI6ImNra3pmN2QxajBiYWUycW55N3E1dG1tcTEifQ.JFKSI85oP7M2gbeUTaUfQQ")
            buffer = BytesIO(response.content)  # opening directly from raw response doesn't work for JPEGs
            tile_img = png.read_png_int(buffer)
            R,G,B,_ = (tile_img[int(pixel_row), int(pixel_column)])
            height = -10000 + ((R * 256 * 256 + G * 256 + B) * 0.1)
            new_height = height + alt
            txt = re.sub("(?<=<altitudeMode>).+(?=<\/altitudeMode>)", "absolute", txt)
            txt = re.sub("(?<=<altitude>).+(?=<\/altitude>)", f"{new_height}", txt)
            txt = re.sub("(?<=<coordinates>).+(?=<\/coordinates>)", f"{lng},{lat},{new_height}", txt)
            #print(txt)
            f.seek(0)
            f.write(txt)
            f.truncate()
          else:
            continue
    return kmls

@dg.solid(output_defs=[dg.OutputDefinition(io_manager_key="geojson", name="camera")])
def create_geojson(context,kmls):
  features = []
  for kml in kmls:
    with open(kml, "r") as f:
      KML = parser.parse(f).getroot()
    properties = {
        "id": KML.PhotoOverlay.name,
        "lng": KML.PhotoOverlay.Camera.longitude,
        "lat": KML.PhotoOverlay.Camera.latitude,
        "height": KML.PhotoOverlay.Camera.altitude,
        "heading": KML.PhotoOverlay.Camera.heading,
        "tilt": KML.PhotoOverlay.Camera.tilt,
        "fov": abs(float(KML.PhotoOverlay.ViewVolume.leftFov)) + abs(float(KML.PhotoOverlay.ViewVolume.rightFov)),
    }
    radius = get_radius(kml)
    if radius:
      viewcone = draw_cone(kml, radius=radius)
    else:
      viewcone = draw_cone(kml)
  
    features.append(geojson.Feature(geometry=viewcone, properties=properties))

  collection = geojson.FeatureCollection(features=features)

  return collection

