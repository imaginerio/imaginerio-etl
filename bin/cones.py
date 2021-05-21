import shapely.geometry import Point, Polygon
from pyproj import Proj
import math


#Draw cone
def sector(center, start_angle, end_angle, radius, steps=200):
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


#Get depicted items coordinates
def get_distance(id):
    depicts = df.loc[id, "depicts"]
    http://www.wikidata.org/entity/Q10353359 Praça XV de Novembro
    q = depicts.extract(r"(?<=\/)Q\d+")
    origin = df.loc[id, "lat"], df.loc[id, "lon"]
    depicted = 
    """SELECT ?coordinate
    WHERE
    {
    wd:{q} wdt:P625 ?coordinate .
    }"""
    origin = reproject(depicted)
    depicted = reproject(depicted)
    distance = origin.distance(depicted)
    return distance

#Reproject
def reproject(coordinates):
    rj = Proj(init='EPSG:32722')
    origin = Point(coordinates)
    origin_proj = rj(origin.y, origin.x)
    return origin_proj

